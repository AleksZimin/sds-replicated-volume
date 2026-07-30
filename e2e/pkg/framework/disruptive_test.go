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
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/types"
	. "github.com/onsi/gomega"
)

var _ = Describe("hasDisruptiveLabel", func() {
	It("returns true when Labels contain Disruptive", func() {
		args := []any{Labels{"Smoke", LabelDisruptive}}
		Expect(hasDisruptiveLabel(args)).To(BeTrue())
	})

	It("returns false when no Disruptive label", func() {
		args := []any{Labels{"Smoke", "Full"}}
		Expect(hasDisruptiveLabel(args)).To(BeFalse())
	})

	It("returns false when no Labels at all", func() {
		args := []any{"text", func() {}}
		Expect(hasDisruptiveLabel(args)).To(BeFalse())
	})

	It("finds Disruptive among multiple Labels args", func() {
		args := []any{Labels{"Smoke"}, Labels{LabelDisruptive}}
		Expect(hasDisruptiveLabel(args)).To(BeTrue())
	})
})

// disruptiveOp stands in for the operation name a destructive helper passes to
// the guard. Every message assertion pins it, because a message that does not
// name the refused operation cannot be acted on.
const disruptiveOp = "rebooting node worker-2"

// suiteLevelNodeTypes is the set the guard answers with "move the call into a
// spec". It mirrors types.NodeTypesForSuiteLevelNodes member by member, so a
// Ginkgo release that adds a suite-level node type shows up here as a diff to
// review rather than as a node type that silently falls through to the
// missing-label branch and demands a label that cannot be written.
var suiteLevelNodeTypes = []types.NodeType{
	types.NodeTypeBeforeSuite,
	types.NodeTypeSynchronizedBeforeSuite,
	types.NodeTypeAfterSuite,
	types.NodeTypeSynchronizedAfterSuite,
	types.NodeTypeReportBeforeSuite,
	types.NodeTypeReportAfterSuite,
	types.NodeTypeCleanupAfterSuite,
}

var _ = Describe("classifyDisruptiveCallSite", func() {
	It("covers exactly the suite-level node types Ginkgo declares", func() {
		var union types.NodeType
		for _, nodeType := range suiteLevelNodeTypes {
			union |= nodeType
		}
		Expect(union).To(Equal(types.NodeTypesForSuiteLevelNodes))
	})

	It("allows a spec that carries the Disruptive label", func() {
		site, message := classifyDisruptiveCallSite(
			disruptiveOp, types.NodeTypeIt, []string{LabelSmoke, LabelDisruptive})

		Expect(site).To(Equal(disruptiveCallSiteAllowed))
		Expect(message).To(BeEmpty())
	})

	// The zero NodeType is what Ginkgo reports when nothing is executing, and the
	// label list is empty there too — so the verdict must come from the node type
	// and the message must blame the call site, not an absent label.
	It("blames the call site, not a missing label, when no node is running", func() {
		site, message := classifyDisruptiveCallSite(disruptiveOp, types.NodeTypeInvalid, nil)

		Expect(site).To(Equal(disruptiveCallSiteOutsideSpec))
		Expect(message).To(ContainSubstring(disruptiveOp))
		Expect(message).To(ContainSubstring("no Ginkgo node was running"))
		Expect(message).NotTo(ContainSubstring("Label(fw.LabelDisruptive)"),
			"outside a spec there is no spec to put a label on, so asking for one names the wrong cause")
	})

	It("tells a suite-level node to move the call into a spec", func() {
		for _, nodeType := range suiteLevelNodeTypes {
			site, message := classifyDisruptiveCallSite(disruptiveOp, nodeType, nil)

			Expect(site).To(Equal(disruptiveCallSiteSuiteNode), "node type %s", nodeType)
			Expect(message).To(ContainSubstring(disruptiveOp))
			Expect(message).To(ContainSubstring("MUST be called from a spec"))
			Expect(message).To(ContainSubstring(nodeType.String()),
				"the message has to name the node the call was made from")
		}
	})

	It("demands the label from a spec that lacks it, and names the gate variables", func() {
		site, message := classifyDisruptiveCallSite(
			disruptiveOp, types.NodeTypeIt, []string{LabelSmoke, LabelFeatureQuorum})

		Expect(site).To(Equal(disruptiveCallSiteUnlabeledSpec))
		Expect(message).To(ContainSubstring(disruptiveOp))
		Expect(message).To(ContainSubstring("Label(fw.LabelDisruptive)"))
		Expect(message).To(ContainSubstring(LabelSmoke), "the labels in scope help the author see what was read")
		Expect(message).To(ContainSubstring(envAllowDisruptive))
		Expect(message).To(ContainSubstring(envRunAll))
	})
})

// The guard reads the labels in SCOPE, so the label on this container is what
// lets the spec below through — the shape used across e2e/full, where Disruptive
// sits on the Describe and the destructive call sits in the It.
//
// The label is inert in this suite: the unit tests never call fw.Setup(), so no
// class gate and no args transformer is registered and the spec runs regardless
// of E2E_ALLOW_DISRUPTIVE.
var _ = Describe("RequireDisruptiveSpec", Label(LabelDisruptive), func() {
	It("returns quietly from a spec that inherits the label from its container", func() {
		Expect(CurrentSpecReport().Labels()).To(ContainElement(LabelDisruptive))

		RequireDisruptiveSpec(disruptiveOp)
	})
})

// TestDisruptiveGuardOutsideOfASpec covers the one branch that is unreachable
// from inside the Ginkgo suite. An ordinary go test function runs with no Ginkgo
// node executing — the state a call from tree construction or from a plain test
// lands in — and there the guard PANICS with this package's own message: Fail
// would have no spec to attribute the failure to and would unwind with Ginkgo's
// generic UncaughtGinkgoPanic text, losing the reason.
//
// The same branch pins the guard INTO each destructive framework helper: the
// panic names the operation the helper passed in, which can only happen if the
// helper really calls the guard, and does so before touching the cluster — the
// fixtures below carry nil clients, so a helper that skipped the guard would
// panic with a nil-pointer runtime error instead of a message.
//
// Two callers are not covered here. startVolumeIO lives in e2e/full, a package
// whose tests need a cluster, so a unit pin could not be run there. The guard
// inside RequireDisruptiveSpec's other two branches calls Fail, which cannot be
// observed from a spec without failing it — classifyDisruptiveCallSite is the
// unit-testable decision, and it is covered above.
func TestDisruptiveGuardOutsideOfASpec(t *testing.T) {
	if leafNodeType := CurrentSpecReport().LeafNodeType; leafNodeType != types.NodeTypeInvalid {
		t.Fatalf("precondition: expected no Ginkgo node to be running, got %s", leafNodeType)
	}

	trvr := newTestRVRForTest("e2e-rv")

	for _, tc := range []struct {
		caller    string
		operation string
		call      func()
	}{
		{
			caller:    "RequireDisruptiveSpec",
			operation: disruptiveOp,
			call:      func() { RequireDisruptiveSpec(disruptiveOp) },
		},
		{
			caller:    "Framework.RebootNode",
			operation: "rebooting node worker-2",
			call:      func() { (&Framework{}).RebootNode(context.Background(), "worker-2") },
		},
		{
			caller:    "TestRVR.RemoveFinalizers",
			operation: "removing the finalizers of " + trvr.Name(),
			call:      func() { trvr.RemoveFinalizers(context.Background()) },
		},
	} {
		t.Run(tc.caller, func(t *testing.T) {
			expectDisruptiveGuardPanic(t, tc.operation, tc.call)
		})
	}
}

// expectDisruptiveGuardPanic runs call and asserts it was stopped by the
// Disruptive guard's outside-a-spec branch: a panic carrying this package's own
// string message, which states the cause and names operation.
func expectDisruptiveGuardPanic(t *testing.T, operation string, call func()) {
	t.Helper()

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatalf("returned normally outside of a spec, want the Disruptive guard to panic")
		}
		message, ok := recovered.(string)
		if !ok {
			t.Fatalf("panicked with %T (%v), want the guard's own string message", recovered, recovered)
		}
		if !strings.Contains(message, operation) {
			t.Errorf("panic message does not name the operation %q: %s", operation, message)
		}
		if !strings.Contains(message, "no Ginkgo node was running") {
			t.Errorf("panic message does not state the cause: %s", message)
		}
	}()

	call()
}
