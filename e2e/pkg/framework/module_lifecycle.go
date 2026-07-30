/*
Copyright 2026 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package framework

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The two Deckhouse objects that pin a module to a dev build. Neither kind has
// Go types in this module's dependency set — and adding them would drag the
// whole Deckhouse API in — so both are built and read as unstructured.
var (
	gvkModuleConfig       = schema.GroupVersionKind{Group: "deckhouse.io", Version: "v1alpha1", Kind: "ModuleConfig"}
	gvkModulePullOverride = schema.GroupVersionKind{Group: "deckhouse.io", Version: "v1alpha2", Kind: "ModulePullOverride"}
)

const (
	// ModuleName is the Deckhouse module this framework drives. It is also the
	// name of its ModuleConfig and of its ModulePullOverride: Deckhouse keys both
	// by module name.
	ModuleName = "sds-replicated-volume"

	// moduleNamespace is where the module's workloads live. Same namespace as
	// controllerMetricsNamespace (controller_metrics.go) — spelled out separately
	// because that one is part of the metrics endpoint's address, while this one
	// is the namespace whose workloads prove the module is running.
	moduleNamespace = "d8-sds-replicated-volume"

	// modulePullOverrideScanInterval is how often Deckhouse re-resolves the tag
	// to a digest. 15s matches the documented dev-install recipe: the retag of a
	// live MPO must be noticed in seconds, not on a multi-minute default.
	modulePullOverrideScanInterval = "15s"

	// DefaultModuleReadyTimeout is the readiness budget EnsureModuleVersion uses
	// when the caller passes none. A retag pulls a fresh bundle and restarts every
	// workload of the module, so the wait is dominated by image pulls.
	DefaultModuleReadyTimeout = 15 * time.Minute

	// moduleReadyPollInterval is how often the readiness criterion is re-read.
	// The wait is dominated by pulls and pod restarts, so polling faster would
	// only add API traffic.
	moduleReadyPollInterval = 10 * time.Second
)

// moduleWorkloadKind names the controller that owns a workload, because the
// rollout counters live under different names for each.
type moduleWorkloadKind string

const (
	workloadDeployment moduleWorkloadKind = "Deployment"
	workloadDaemonSet  moduleWorkloadKind = "DaemonSet"
)

// moduleWorkload identifies one workload of the module.
type moduleWorkload struct {
	Kind moduleWorkloadKind
	Name string
}

// String renders the workload the way kubectl addresses it (deployment/agent).
func (w moduleWorkload) String() string {
	return strings.ToLower(string(w.Kind)) + "/" + w.Name
}

// moduleWorkloads lists every workload the module ships with the new control
// plane, in the order the dev-install runbook checks them. The list is the
// readiness criterion's subject: waiting for "the workloads in the namespace" to
// be rollout-complete is vacuously true on a stand where the module is not
// installed at all, so the expected set has to be named.
func moduleWorkloads() []moduleWorkload {
	return []moduleWorkload{
		{Kind: workloadDeployment, Name: "controller"},
		{Kind: workloadDeployment, Name: "csi-controller"},
		{Kind: workloadDeployment, Name: "spaas"},
		{Kind: workloadDeployment, Name: "webhooks"},
		{Kind: workloadDaemonSet, Name: "agent"},
		{Kind: workloadDaemonSet, Name: "csi-node"},
	}
}

// ---------------------------------------------------------------------------
// Exported helpers
// ---------------------------------------------------------------------------

// EnsureModuleVersion installs the Deckhouse module moduleName at imageTag and
// blocks until the module actually runs that build.
//
// It writes exactly two objects, create-or-update:
//
//   - ModuleConfig (deckhouse.io/v1alpha1) with spec.enabled=true. ONLY
//     spec.enabled is touched, so a stand's own settings and version survive,
//     and an already-enabled ModuleConfig is not written at all. Enabling is not
//     optional: Deckhouse ignores the ModulePullOverride of a disabled module,
//     which is why both objects are submitted before anything is awaited.
//   - ModulePullOverride (deckhouse.io/v1alpha2) with spec.imageTag=imageTag,
//     rollback=false and scanInterval=15s. Calling the helper again with a
//     DIFFERENT tag retags the live object — that retag IS the module upgrade.
//
// Readiness is judged by the workloads, never by the tag inside a pod's image:
// werf renders images content-addressed (<registry>/<module>@sha256:<digest>),
// so no pod ever mentions the tag. All of these must hold at once:
//
//   - every workload of moduleWorkloads() exists in d8-sds-replicated-volume —
//     "the rollout is complete" over an empty namespace is vacuously true and is
//     therefore rejected, which is what makes the criterion usable for a first
//     installation;
//   - each workload is rollout-complete (its controller observed the current
//     generation, every pod runs the current template, no pod of an older
//     template is left) and reports its pods available and ready;
//   - and, when this call retagged a live MPO, status.imageDigest of that MPO
//     differs from the digest observed BEFORE the write. That bundle digest is
//     the primary proof that Deckhouse pulled the new build; an absent or empty
//     digest means "no signal yet" and the wait continues. After a retag the
//     whole criterion additionally has to hold on two consecutive polls, so that
//     a sample taken between the new digest and the re-apply of the manifests is
//     not mistaken for a finished rollout (see moduleReadyConfirmations).
//
// Pod-template digests are reported as progress but deliberately NOT required to
// change: werf builds are content-addressed, so a component untouched between
// the two tags keeps its digest, and demanding a change would hang forever on
// close dev builds.
//
// Idempotent. A repeat call with the tag already in the live MPO writes nothing
// and only re-checks the rollout, so the helper is safe as a pre-discovery hook
// that may run on more than one Ginkgo worker (see WithPreDiscovery).
//
// timeout budgets the whole readiness wait; 0 means DefaultModuleReadyTimeout.
// NOTHING is cleaned up and no DeferCleanup is registered: the module is left
// installed at imageTag on purpose — the state of the stand after a run is the
// diagnosis material, and retagging it back would destroy it.
func (f *Framework) EnsureModuleVersion(ctx context.Context, moduleName, imageTag string, timeout time.Duration) {
	GinkgoHelper()
	if timeout <= 0 {
		timeout = DefaultModuleReadyTimeout
	}
	if err := ensureModuleVersion(ctx, f.Client, f, moduleName, imageTag, timeout, moduleReadyPollInterval); err != nil {
		Fail(err.Error())
	}
}

// ---------------------------------------------------------------------------
// Seams
// ---------------------------------------------------------------------------

// moduleObjectClient is the seam the write path goes through. client.Client
// implements it against the cluster; helper unit tests substitute the
// controller-runtime fake client, so create-or-update is exercised — with real
// NotFound and resourceVersion semantics — without a cluster.
type moduleObjectClient interface {
	Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error
	Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error
	Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error
}

// moduleObserver is the seam the readiness poll reads through. *Framework reads
// one snapshot per poll with the framework client; unit tests script the
// snapshots, so every branch of the criterion is walked without a cluster.
type moduleObserver interface {
	observeModule(ctx context.Context, moduleName string) (moduleObservation, error)
}

// ---------------------------------------------------------------------------
// Observation
// ---------------------------------------------------------------------------

// moduleObservation is one snapshot of everything the readiness criterion reads:
// the pull override's tag and published bundle digest, plus the state of every
// expected workload.
type moduleObservation struct {
	// MPOFound reports whether a ModulePullOverride exists at all.
	MPOFound bool
	// MPOTag is spec.imageTag of that object ("" when there is none).
	MPOTag string
	// MPODigest is status.imageDigest — the digest of the bundle Deckhouse
	// resolved the tag to. "" means Deckhouse has not published one yet.
	MPODigest string
	// Workloads holds one entry per moduleWorkloads() entry, in that order,
	// present or not.
	Workloads []moduleWorkloadState
}

// moduleWorkloadState is one workload's rollout state, with the Deployment and
// DaemonSet counters projected onto one set of names.
type moduleWorkloadState struct {
	Workload moduleWorkload
	// Found reports whether the object exists.
	Found bool

	Generation         int64
	ObservedGeneration int64

	// Desired is how many pods the workload wants (spec.replicas /
	// status.desiredNumberScheduled).
	Desired int32
	// Updated is how many of them run the current pod template.
	Updated int32
	// Current is how many pods exist for the workload, on any template.
	Current int32
	// Ready and Available are the readiness the workload's controller publishes
	// for its pods.
	Ready     int32
	Available int32

	// Images is the sorted pod-template image set (init containers included).
	// werf renders every image as <registry>/<module>@sha256:<digest>, so this
	// slice IS the pod-template digest set a retag may change.
	Images []string
}

// rolloutError reports nil when the workload finished rolling out and all of its
// pods are up, and otherwise the reason it did not — phrased for a timeout
// message.
func (s moduleWorkloadState) rolloutError() error {
	switch {
	case !s.Found:
		return fmt.Errorf("%s does not exist yet", s.Workload)
	case s.ObservedGeneration < s.Generation:
		return fmt.Errorf("%s: its controller is still at observedGeneration %d, the object is at generation %d",
			s.Workload, s.ObservedGeneration, s.Generation)
	case s.Desired == 0:
		return fmt.Errorf("%s wants 0 pods, so its readiness would prove nothing", s.Workload)
	case s.Updated != s.Desired:
		return fmt.Errorf("%s: %d of %d pods run the current template", s.Workload, s.Updated, s.Desired)
	case s.Current != s.Updated:
		return fmt.Errorf("%s: %d pods exist against %d on the current template, an older template is still around",
			s.Workload, s.Current, s.Updated)
	case s.Available < s.Desired:
		return fmt.Errorf("%s: %d of %d pods available", s.Workload, s.Available, s.Desired)
	case s.Ready < s.Desired:
		return fmt.Errorf("%s: %d of %d pods ready", s.Workload, s.Ready, s.Desired)
	}
	return nil
}

// observeModule reads one moduleObservation through the framework client.
//
// The ModulePullOverride is read as unstructured, which the framework client
// serves straight from the API server (its cache covers typed objects only) — so
// the poll sees status.imageDigest as soon as Deckhouse writes it.
func (f *Framework) observeModule(ctx context.Context, moduleName string) (moduleObservation, error) {
	var out moduleObservation

	mpo := &unstructured.Unstructured{}
	mpo.SetGroupVersionKind(gvkModulePullOverride)
	err := f.Client.Get(ctx, client.ObjectKey{Name: moduleName}, mpo)
	switch {
	case err == nil:
		out.MPOFound = true
		out.MPOTag = nestedScalarString(mpo.Object, "spec", "imageTag")
		out.MPODigest = nestedScalarString(mpo.Object, "status", "imageDigest")
	case apierrors.IsNotFound(err):
		// No override yet — the normal state of a first installation.
	default:
		return moduleObservation{}, fmt.Errorf("reading %s %q: %w", gvkModulePullOverride.Kind, moduleName, err)
	}

	var deployments appsv1.DeploymentList
	if err := f.Client.List(ctx, &deployments, client.InNamespace(moduleNamespace)); err != nil {
		return moduleObservation{}, fmt.Errorf("listing Deployments in %s: %w", moduleNamespace, err)
	}
	var daemonSets appsv1.DaemonSetList
	if err := f.Client.List(ctx, &daemonSets, client.InNamespace(moduleNamespace)); err != nil {
		return moduleObservation{}, fmt.Errorf("listing DaemonSets in %s: %w", moduleNamespace, err)
	}
	out.Workloads = projectModuleWorkloads(moduleWorkloads(), &deployments, &daemonSets)

	return out, nil
}

// projectModuleWorkloads projects the expected workloads onto what the cluster
// currently holds. A workload nobody found stays in the result as Found=false:
// the criterion has to be able to say WHICH workload is missing.
func projectModuleWorkloads(
	want []moduleWorkload,
	deployments *appsv1.DeploymentList,
	daemonSets *appsv1.DaemonSetList,
) []moduleWorkloadState {
	out := make([]moduleWorkloadState, 0, len(want))
	for _, w := range want {
		state := moduleWorkloadState{Workload: w}
		switch w.Kind {
		case workloadDeployment:
			for i := range deployments.Items {
				if deployments.Items[i].Name == w.Name {
					state = deploymentState(w, &deployments.Items[i])
					break
				}
			}
		case workloadDaemonSet:
			for i := range daemonSets.Items {
				if daemonSets.Items[i].Name == w.Name {
					state = daemonSetState(w, &daemonSets.Items[i])
					break
				}
			}
		}
		out = append(out, state)
	}
	return out
}

// deploymentState projects a Deployment's rollout counters. An unset
// spec.replicas defaults to 1, exactly as the API server does.
func deploymentState(w moduleWorkload, d *appsv1.Deployment) moduleWorkloadState {
	desired := int32(1)
	if d.Spec.Replicas != nil {
		desired = *d.Spec.Replicas
	}
	return moduleWorkloadState{
		Workload:           w,
		Found:              true,
		Generation:         d.Generation,
		ObservedGeneration: d.Status.ObservedGeneration,
		Desired:            desired,
		Updated:            d.Status.UpdatedReplicas,
		Current:            d.Status.Replicas,
		Ready:              d.Status.ReadyReplicas,
		Available:          d.Status.AvailableReplicas,
		Images:             podTemplateImages(&d.Spec.Template),
	}
}

// daemonSetState projects a DaemonSet's rollout counters. Desired comes from the
// status, because the DaemonSet controller — not the author — decides how many
// nodes the set covers.
func daemonSetState(w moduleWorkload, ds *appsv1.DaemonSet) moduleWorkloadState {
	return moduleWorkloadState{
		Workload:           w,
		Found:              true,
		Generation:         ds.Generation,
		ObservedGeneration: ds.Status.ObservedGeneration,
		Desired:            ds.Status.DesiredNumberScheduled,
		Updated:            ds.Status.UpdatedNumberScheduled,
		Current:            ds.Status.CurrentNumberScheduled,
		Ready:              ds.Status.NumberReady,
		Available:          ds.Status.NumberAvailable,
		Images:             podTemplateImages(&ds.Spec.Template),
	}
}

// podTemplateImages returns the sorted image set of a pod template, init
// containers included.
func podTemplateImages(template *corev1.PodTemplateSpec) []string {
	out := make([]string, 0, len(template.Spec.InitContainers)+len(template.Spec.Containers))
	for i := range template.Spec.InitContainers {
		out = append(out, template.Spec.InitContainers[i].Image)
	}
	for i := range template.Spec.Containers {
		out = append(out, template.Spec.Containers[i].Image)
	}
	slices.Sort(out)
	return out
}

// ---------------------------------------------------------------------------
// Readiness criterion
// ---------------------------------------------------------------------------

// moduleReadiness is what one readiness poll is waiting for: the rollout of
// every expected workload, plus — after a retag — a bundle digest different from
// the one the override carried before the write.
type moduleReadiness struct {
	// DigestBefore is MPO status.imageDigest as observed BEFORE the write ("" if
	// there was no override or no digest yet).
	DigestBefore string
	// RequireDigestChange is set only when this call retagged a LIVE override.
	// A first installation and a no-op call have no digest to compare against.
	RequireDigestChange bool
	// ImagesBefore is the pod-template image set per workload before the write,
	// keyed by workload. Progress reporting only — see moduleImagesChanged.
	ImagesBefore map[string][]string
}

// checkModuleReady reports nil when the observation satisfies the criterion, and
// otherwise the reason it does not — the message a timeout would carry.
func checkModuleReady(want moduleReadiness, obs moduleObservation) error {
	if want.RequireDigestChange {
		switch obs.MPODigest {
		case "":
			return fmt.Errorf("%s carries no status.imageDigest yet", gvkModulePullOverride.Kind)
		case want.DigestBefore:
			return fmt.Errorf("%s still reports the bundle digest it had before the retag (%s)",
				gvkModulePullOverride.Kind, want.DigestBefore)
		}
	}
	// An observation with no workloads at all would make every rollout claim
	// below vacuously true.
	if len(obs.Workloads) == 0 {
		return errors.New("no workload of the module was observed at all")
	}
	for _, state := range obs.Workloads {
		if err := state.rolloutError(); err != nil {
			return err
		}
	}
	return nil
}

// moduleImagesChanged returns the workloads whose pod-template image set differs
// from the pre-write snapshot.
//
// Reported, never required: werf builds are content-addressed, so a component
// that did not change between the two tags keeps its digest and its pod template
// — requiring a change for every workload (or even for one) would hang on close
// dev builds. It is printed so that a run against two tags that DO differ shows
// which components were actually replaced.
func moduleImagesChanged(before map[string][]string, obs moduleObservation) []string {
	var out []string
	for _, state := range obs.Workloads {
		if !state.Found {
			continue
		}
		if !slices.Equal(before[state.Workload.String()], state.Images) {
			out = append(out, state.Workload.String())
		}
	}
	return out
}

// observationImages projects an observation into the per-workload image sets
// moduleReadiness compares against.
func observationImages(obs moduleObservation) map[string][]string {
	out := make(map[string][]string, len(obs.Workloads))
	for _, state := range obs.Workloads {
		if state.Found {
			out[state.Workload.String()] = state.Images
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Core
// ---------------------------------------------------------------------------

// moduleObjectAction is what the write path did to one object.
type moduleObjectAction string

const (
	moduleObjectCreated   moduleObjectAction = "created"
	moduleObjectUpdated   moduleObjectAction = "updated"
	moduleObjectUnchanged moduleObjectAction = "unchanged"
)

// ensureModuleVersion is the failing logic of EnsureModuleVersion: snapshot,
// write both objects, then wait for the rollout the write implies.
//
// The snapshot is taken BEFORE the write on purpose — it is the only moment at
// which the digest of the OLD bundle can still be read, and that value is what
// turns "a digest is published" into "a new digest is published".
func ensureModuleVersion(
	ctx context.Context,
	c moduleObjectClient,
	observer moduleObserver,
	moduleName, imageTag string,
	timeout, poll time.Duration,
) error {
	if moduleName == "" {
		return errors.New("the module name must not be empty")
	}
	if imageTag == "" {
		return fmt.Errorf("the image tag of module %q must not be empty", moduleName)
	}

	before, err := observer.observeModule(ctx, moduleName)
	if err != nil {
		return fmt.Errorf("reading the state of module %q before the write: %w", moduleName, err)
	}

	configAction, err := ensureModuleConfigEnabled(ctx, c, moduleName)
	if err != nil {
		return err
	}
	overrideAction, previousTag, err := ensureModulePullOverride(ctx, c, moduleName, imageTag)
	if err != nil {
		return err
	}

	want := moduleReadiness{
		DigestBefore:        before.MPODigest,
		RequireDigestChange: overrideAction == moduleObjectUpdated,
		ImagesBefore:        observationImages(before),
	}

	fmt.Fprintf(GinkgoWriter,
		"[%s] [module] %s: ModuleConfig %s, ModulePullOverride %s (tag %q -> %q), digest before %q; "+
			"waiting up to %s for the rollout (digest change required: %t)\n",
		time.Now().Format("15:04:05.000"), moduleName, configAction, overrideAction,
		previousTag, imageTag, before.MPODigest, timeout, want.RequireDigestChange)

	return awaitModuleReady(ctx, observer, moduleName, want, timeout, poll)
}

// ensureModuleConfigEnabled makes sure the module's ModuleConfig exists and is
// enabled, touching nothing else: a stand's settings and version are its own,
// and Deckhouse ignores a ModulePullOverride of a disabled module.
func ensureModuleConfigEnabled(
	ctx context.Context,
	c moduleObjectClient,
	moduleName string,
) (moduleObjectAction, error) {
	current := &unstructured.Unstructured{}
	current.SetGroupVersionKind(gvkModuleConfig)
	err := c.Get(ctx, client.ObjectKey{Name: moduleName}, current)
	switch {
	case apierrors.IsNotFound(err):
		if err := c.Create(ctx, buildModuleConfig(moduleName)); err != nil {
			return "", fmt.Errorf("creating %s %q: %w", gvkModuleConfig.Kind, moduleName, err)
		}
		return moduleObjectCreated, nil
	case err != nil:
		return "", fmt.Errorf("reading %s %q: %w", gvkModuleConfig.Kind, moduleName, err)
	}

	enabled, found, err := unstructured.NestedBool(current.Object, "spec", "enabled")
	if err != nil {
		return "", fmt.Errorf("reading spec.enabled of %s %q: %w", gvkModuleConfig.Kind, moduleName, err)
	}
	if found && enabled {
		return moduleObjectUnchanged, nil
	}
	if err := unstructured.SetNestedField(current.Object, true, "spec", "enabled"); err != nil {
		return "", fmt.Errorf("setting spec.enabled of %s %q: %w", gvkModuleConfig.Kind, moduleName, err)
	}
	if err := c.Update(ctx, current); err != nil {
		return "", fmt.Errorf("enabling %s %q: %w", gvkModuleConfig.Kind, moduleName, err)
	}
	return moduleObjectUpdated, nil
}

// ensureModulePullOverride makes sure the module's ModulePullOverride pins
// imageTag, and reports what it did together with the tag that was there before.
//
// A live override is retagged in place (that retag is the upgrade), and the tag
// is the only field rewritten: rollback and scanInterval are filled in when
// absent, so an override a stand configured on purpose is not overwritten. An
// override that already pins imageTag is not written at all — that no-op is what
// makes the helper idempotent.
func ensureModulePullOverride(
	ctx context.Context,
	c moduleObjectClient,
	moduleName, imageTag string,
) (moduleObjectAction, string, error) {
	current := &unstructured.Unstructured{}
	current.SetGroupVersionKind(gvkModulePullOverride)
	err := c.Get(ctx, client.ObjectKey{Name: moduleName}, current)
	switch {
	case apierrors.IsNotFound(err):
		if err := c.Create(ctx, buildModulePullOverride(moduleName, imageTag)); err != nil {
			return "", "", fmt.Errorf("creating %s %q: %w", gvkModulePullOverride.Kind, moduleName, err)
		}
		return moduleObjectCreated, "", nil
	case err != nil:
		return "", "", fmt.Errorf("reading %s %q: %w", gvkModulePullOverride.Kind, moduleName, err)
	}

	previousTag, _, err := unstructured.NestedString(current.Object, "spec", "imageTag")
	if err != nil {
		return "", "", fmt.Errorf("reading spec.imageTag of %s %q: %w", gvkModulePullOverride.Kind, moduleName, err)
	}
	if previousTag == imageTag {
		return moduleObjectUnchanged, previousTag, nil
	}

	if err := unstructured.SetNestedField(current.Object, imageTag, "spec", "imageTag"); err != nil {
		return "", "", fmt.Errorf("setting spec.imageTag of %s %q: %w", gvkModulePullOverride.Kind, moduleName, err)
	}
	if err := fillModulePullOverrideDefaults(current); err != nil {
		return "", "", fmt.Errorf("completing %s %q: %w", gvkModulePullOverride.Kind, moduleName, err)
	}
	if err := c.Update(ctx, current); err != nil {
		return "", "", fmt.Errorf("retagging %s %q from %q to %q: %w",
			gvkModulePullOverride.Kind, moduleName, previousTag, imageTag, err)
	}
	return moduleObjectUpdated, previousTag, nil
}

// buildModuleConfig builds the ModuleConfig that enables the module. Only
// spec.enabled is set: everything else about a module's configuration belongs to
// whoever installed the stand.
func buildModuleConfig(moduleName string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"enabled": true},
	}}
	u.SetGroupVersionKind(gvkModuleConfig)
	u.SetName(moduleName)
	return u
}

// buildModulePullOverride builds the ModulePullOverride that pins the module to
// a dev build: the tag to follow, no rollback of the module's version, and a
// scan interval short enough for a retag to be picked up in seconds.
func buildModulePullOverride(moduleName, imageTag string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{
			"imageTag":     imageTag,
			"rollback":     false,
			"scanInterval": modulePullOverrideScanInterval,
		},
	}}
	u.SetGroupVersionKind(gvkModulePullOverride)
	u.SetName(moduleName)
	return u
}

// fillModulePullOverrideDefaults adds the fields a hand-made override may lack,
// leaving present values alone.
func fillModulePullOverrideDefaults(u *unstructured.Unstructured) error {
	if _, found, err := unstructured.NestedBool(u.Object, "spec", "rollback"); err != nil {
		return fmt.Errorf("reading spec.rollback: %w", err)
	} else if !found {
		if err := unstructured.SetNestedField(u.Object, false, "spec", "rollback"); err != nil {
			return fmt.Errorf("setting spec.rollback: %w", err)
		}
	}
	interval, _, err := unstructured.NestedString(u.Object, "spec", "scanInterval")
	if err != nil {
		return fmt.Errorf("reading spec.scanInterval: %w", err)
	}
	if interval == "" {
		if err := unstructured.SetNestedField(
			u.Object, modulePullOverrideScanInterval, "spec", "scanInterval"); err != nil {
			return fmt.Errorf("setting spec.scanInterval: %w", err)
		}
	}
	return nil
}

// moduleReadyConfirmations is how many CONSECUTIVE polls must accept the state
// after a retag before the module counts as rolled out.
//
// Deckhouse publishes the new bundle digest before it re-applies the module's
// manifests, so a single sample taken inside that window would see the new
// digest next to workloads still calmly running the OLD template — and call the
// upgrade finished. Demanding the criterion twice, one poll interval apart,
// closes the window: the re-apply lands within seconds, and the second sample
// already sees the generation bump (or the pod restarts it causes) and rejects
// it.
//
// It cannot deadlock on a pair of tags whose workloads are byte-identical:
// nothing is required to CHANGE here, the same accepted state merely has to be
// observed twice.
const moduleReadyConfirmations = 2

// awaitModuleReady polls the module until the criterion accepts it (twice, after
// a retag — see moduleReadyConfirmations), the budget runs out, or ctx ends.
//
// A failed read is not a verdict: a module rollout restarts the very webhooks
// and API extensions the read goes through, so a read error counts as "not yet"
// and is only reported if the budget expires on it. The poll interval is a
// parameter so unit tests walk the loop without waiting in real time.
func awaitModuleReady(
	ctx context.Context,
	observer moduleObserver,
	moduleName string,
	want moduleReadiness,
	timeout, poll time.Duration,
) error {
	required := 1
	if want.RequireDigestChange {
		required = moduleReadyConfirmations
	}

	deadline := time.Now().Add(timeout)
	var last error
	accepted := 0

	for {
		obs, err := observer.observeModule(ctx, moduleName)
		if err != nil {
			last, accepted = fmt.Errorf("reading the state of module %q: %w", moduleName, err), 0
		} else if last = checkModuleReady(want, obs); last != nil {
			accepted = 0
		} else {
			accepted++
			if accepted >= required {
				fmt.Fprintf(GinkgoWriter,
					"[%s] [module] %s ready: tag %q, bundle digest %q, pod templates replaced: %v\n",
					time.Now().Format("15:04:05.000"), moduleName, obs.MPOTag, obs.MPODigest,
					moduleImagesChanged(want.ImagesBefore, obs))
				return nil
			}
		}

		if last == nil {
			// Accepted, but not yet confirmed by another poll.
			last = fmt.Errorf("the module looks rolled out, but the state held for %d of the %d polls"+
				" required to rule out a sample taken between the new digest and the re-apply",
				accepted, required)
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("module %q did not become ready within %s: %w", moduleName, timeout, last)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for module %q to become ready: %w; last state: %v",
				moduleName, ctx.Err(), last)
		case <-time.After(poll):
		}
	}
}
