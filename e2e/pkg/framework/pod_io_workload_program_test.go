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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The writer program and the probe commands run inside a pod, where nothing
// checks them before they run: a quoting mistake surfaces as a workload that
// never beats, hours into a cluster run. These specs check them here — first as
// text, then by actually running the writer against a local directory, which is
// the only way to prove that the journal it produces is the journal
// parseIOJournal reads.
var _ = Describe("PodIOWorkload pod commands", func() {
	w, err := (&Framework{}).newPodIOWorkload(nil, PodIOWorkloadOptions{
		Namespace:        testPodIONS,
		StorageClassName: testPodIOSC,
		Name:             testPodIOName,
	})
	Expect(err).NotTo(HaveOccurred())

	DescribeTable("are valid shell",
		func(cmd []string) {
			Expect(cmd[:2]).To(Equal([]string{"sh", "-c"}))

			Expect(shellSyntaxError(cmd[2])).To(Succeed())
		},
		Entry("writer program", []string{"sh", "-c", w.program()}),
		Entry("probe", w.probeCommand(ioWorkloadJournalTail)),
		Entry("checksum", w.checksumCommand()),
		Entry("stop", w.stopCommand()),
	)

	It("binds every value the program reads", func() {
		header, _, found := strings.Cut(w.program(), podIOWorkloadProgram)

		Expect(found).To(BeTrue(), "the program must be the constant plus a header of assignments")
		Expect(header).To(ContainSubstring("dir=" + podIODir))
		Expect(header).To(ContainSubstring("interval=1"))
		Expect(header).To(ContainSubstring(fmt.Sprintf("records=%d", podIODefaultDataKiB*podIORecordsPerKiB)))
		Expect(header).To(ContainSubstring("record='" + podIODataRecord + "'"))
	})

	It("floors the beat interval at a whole second", func() {
		fast, err := (&Framework{}).newPodIOWorkload(nil, PodIOWorkloadOptions{
			Namespace:        testPodIONS,
			StorageClassName: testPodIOSC,
			Name:             testPodIOName,
			// BusyBox has no fractional sleep and no sub-second date format, so a
			// faster beat rate would neither sleep nor timestamp.
			Interval: 100 * time.Millisecond,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(fast.program()).To(ContainSubstring("interval=1"))
	})

	It("takes the pod's own clock and keeps it parsable without date +%N", func() {
		script := w.probeCommand(1)[2]

		Expect(script).To(ContainSubstring(`"$(date +%s)000"`),
			"%3N is a GNU extension BusyBox prints verbatim, which would make every probe unparsable")
		Expect(script).NotTo(ContainSubstring("%3N"))
	})

	It("reads the whole journal when asked to", func() {
		Expect(w.probeCommand(ioWorkloadJournalFull)[2]).
			To(ContainSubstring(fmt.Sprintf("tail -n %d %s", ioWorkloadJournalFull, w.journalPath())))
	})

	It("never exits on a failure, so the journal stays readable", func() {
		// die() journals the reason and then idles: the journal and the data file
		// are read by exec'ing into this very container, so a container that exits
		// (or crash-loops) takes the only way to read its own evidence with it.
		die := lineContaining(podIOWorkloadProgram, "die() {")
		Expect(die).NotTo(BeEmpty())
		Expect(podIOWorkloadProgram).To(ContainSubstring("printf 'pod-io-workload: %s\\n' \"$1\" >&2\n    idle"))
		Expect(podIOWorkloadProgram).NotTo(ContainSubstring("exit 1\n}"),
			"a failure must be journalled and idled, not exited")
	})

	It("fsyncs one file rather than every filesystem of the node", func() {
		// sync(2) without an argument flushes EVERY filesystem on the node: one
		// frozen volume would then stall the beats of every other workload on that
		// node, and the freeze would be reported against healthy volumes.
		Expect(podIOWorkloadProgram).To(ContainSubstring(`sync -d "$1"`))
		Expect(podIOWorkloadProgram).To(ContainSubstring(`sync_mode=file`))
		Expect(podIOWorkloadProgram).To(ContainSubstring(`sync -d "$beat" 2>/dev/null || sync_mode=all`),
			"a shell whose sync takes no file argument must still work")
	})

	It("publishes a beat only after the write was fsynced, read back and compared", func() {
		write := strings.Index(podIOWorkloadProgram, `printf '%s\n' "$payload" >"$beat"`)
		sync := strings.Index(podIOWorkloadProgram, `sync_file "$beat"`)
		readBack := strings.Index(podIOWorkloadProgram, `back=$(cat "$beat")`)
		compare := strings.Index(podIOWorkloadProgram, `if [ "$back" != "$payload" ]`)
		beat := strings.Index(podIOWorkloadProgram, `journal_line "ok $seq 0 $ts $c"`)

		Expect(write).To(BeNumerically(">", 0))
		Expect(sync).To(BeNumerically(">", write))
		Expect(readBack).To(BeNumerically(">", sync))
		Expect(compare).To(BeNumerically(">", readBack))
		Expect(beat).To(BeNumerically(">", compare))
	})

	It("writes the data file once and hashes it before recording the digest", func() {
		guard := strings.Index(podIOWorkloadProgram, `if [ ! -f "$sum" ]; then`)
		hash := strings.Index(podIOWorkloadProgram, `d=$(sha256sum <"$data.tmp"`)
		record := strings.Index(podIOWorkloadProgram, `printf '%s\n' "$d" >"$sum"`)

		Expect(guard).To(BeNumerically(">", 0),
			"a restarted container must verify the ORIGINAL bytes, not rewrite them")
		Expect(hash).To(BeNumerically(">", guard))
		Expect(record).To(BeNumerically(">", hash))
	})
})

// The specs below run the real writer in a temporary directory. That is what
// pins the program to the parser: the journal it produces has to be the journal
// parseIOJournal reads, the resumed sequence has to stay monotonic, and the
// digest it records has to be the digest of the file it wrote.
var _ = Describe("PodIOWorkload writer program, executed", Ordered, func() {
	// A four-record data file and a three-second run: enough for the data phase
	// and a couple of beats, short enough to run in every suite.
	const (
		writerRecords = 4
		writerBudget  = 3 * time.Second
	)

	var dir string
	var journal, data, sum, stop string

	BeforeAll(func() {
		for _, bin := range []string{"sh", "sha256sum", "date", "sed", "grep", "cut", "tail", "wc"} {
			if _, err := exec.LookPath(bin); err != nil {
				Skip("the writer program cannot be executed here: " + bin + " is missing")
			}
		}
		dir = filepath.Join(GinkgoT().TempDir(), "io")
		journal = filepath.Join(dir, "journal")
		data = filepath.Join(dir, "data")
		sum = filepath.Join(dir, "data.sha256")
		stop = filepath.Join(dir, "stop")
	})

	// runWriter runs the program until the budget kills it, and returns nothing
	// but the side effects it left in dir: the writer is a loop that never returns
	// on its own, exactly as it does not in the pod.
	//
	// Its output goes to a file and the kill goes to the whole process group,
	// because the writer's `sleep` runs as a child holding the same descriptors: a
	// pipe would keep the parent's Wait blocked on that surviving child (an hour,
	// for the sleep of an idling writer), and killing only the shell would leave it
	// behind.
	runWriter := func() {
		GinkgoHelper()
		script := strings.Join([]string{
			"dir=" + dir,
			"interval=1",
			fmt.Sprintf("records=%d", writerRecords),
			"record='" + podIODataRecord + "'",
			"",
			podIOWorkloadProgram,
		}, "\n")

		tmp := GinkgoT().TempDir()
		path := filepath.Join(tmp, "writer.sh")
		Expect(os.WriteFile(path, []byte(script), 0o600)).To(Succeed())
		logPath := filepath.Join(tmp, "writer.log")
		log, err := os.Create(logPath)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = log.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), writerBudget)
		defer cancel()
		cmd := exec.CommandContext(ctx, "sh", path)
		cmd.Stdout, cmd.Stderr = log, log
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
		cmd.WaitDelay = 5 * time.Second

		err = cmd.Run()

		// Killed by the budget is the expected outcome; anything else means the
		// writer returned, which it must never do.
		Expect(ctx.Err()).To(MatchError(context.DeadlineExceeded), "writer returned: %s", readFileString(logPath))
		Expect(err).To(HaveOccurred())
	}

	It("writes a data file of exactly the size its record count implies, and records its digest", func() {
		runWriter()

		content, err := os.ReadFile(data)
		Expect(err).NotTo(HaveOccurred())
		Expect(content).To(HaveLen(writerRecords*podIORecordSize),
			"the record must be exactly podIORecordSize bytes, or the data file size is a guess")
		recorded, err := os.ReadFile(sum)
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(string(recorded))).To(Equal(sha256Hex(content)))
	})

	It("produces a journal the framework's parser reads, with beats one second apart", func() {
		text, err := os.ReadFile(journal)
		Expect(err).NotTo(HaveOccurred())

		j, err := parseIOJournal(string(text))

		Expect(err).NotTo(HaveOccurred())
		Expect(j.Started).To(BeTrue())
		Expect(len(j.Beats)).To(BeNumerically(">=", 2), "journal: %s", text)
		Expect(j.Beats[0].Sequence).To(Equal(int64(0)))
		gap, _ := j.maxInterBeatGap()
		Expect(gap).To(BeNumerically("<=", 2*time.Second))
	})

	It("resumes its sequence after a restart instead of starting over", func() {
		before, err := parseIOJournal(readFileString(journal))
		Expect(err).NotTo(HaveOccurred())
		lastBefore := before.Beats[len(before.Beats)-1].Sequence

		runWriter()

		after, err := parseIOJournal(readFileString(journal))
		Expect(err).NotTo(HaveOccurred(),
			"a restart that restarted the sequence would make the journal non-monotonic and unparsable")
		Expect(after.Beats[len(after.Beats)-1].Sequence).To(BeNumerically(">", lastBefore))
	})

	It("keeps the data file it already wrote, digest included", func() {
		content, err := os.ReadFile(data)
		Expect(err).NotTo(HaveOccurred())
		recorded := strings.TrimSpace(readFileString(sum))

		Expect(recorded).To(Equal(sha256Hex(content)),
			"the second run must not have rewritten the data behind the recorded digest")
	})

	It("drops a record the previous container was cut off in the middle of", func() {
		// A crash between the write and the fsync of a journal line leaves a
		// partial record. Left in place it would sit in the MIDDLE of the journal
		// after the next append, and a malformed record anywhere but the last line
		// makes the whole journal unparsable.
		Expect(appendToFile(journal, "ok 9999 0 175000")).To(Succeed())

		runWriter()

		j, err := parseIOJournal(readFileString(journal))
		Expect(err).NotTo(HaveOccurred())
		for _, beat := range j.Beats {
			Expect(beat.Sequence).To(BeNumerically("<", 9999))
		}
	})

	It("stops on the flag the framework raises, and idles instead of exiting", func() {
		Expect(os.WriteFile(stop, nil, 0o600)).To(Succeed())

		runWriter()

		j, err := parseIOJournal(readFileString(journal))
		Expect(err).NotTo(HaveOccurred())
		Expect(j.Termination).NotTo(BeNil(), "the writer must report its last word")
		Expect(j.Termination.Failed).To(BeFalse())
		Expect(j.Termination.Message).To(ContainSubstring("writer stopped on request"))
	})

	It("reports a volume it cannot even write to through the pod status", func() {
		// No journal can exist on an unwritable volume, so this is the one failure
		// the writer exits on: the pod's container status is then the only place
		// that can carry the reason.
		readOnly := filepath.Join(GinkgoT().TempDir(), "missing", "io")
		Expect(os.WriteFile(filepath.Dir(readOnly), []byte("not a directory"), 0o600)).To(Succeed())

		script := strings.Join([]string{
			"dir=" + readOnly, "interval=1", "records=4",
			"record='" + podIODataRecord + "'", "", podIOWorkloadProgram,
		}, "\n")
		path := filepath.Join(GinkgoT().TempDir(), "writer.sh")
		Expect(os.WriteFile(path, []byte(script), 0o600)).To(Succeed())

		out, err := exec.Command("sh", path).CombinedOutput()

		Expect(err).To(HaveOccurred())
		Expect(string(out)).To(ContainSubstring("pod-io-workload: cannot create"))
	})
})

// shellSyntaxError runs `sh -n` over script and reports what it said.
func shellSyntaxError(script string) error {
	GinkgoHelper()
	path := filepath.Join(GinkgoT().TempDir(), "script.sh")
	Expect(os.WriteFile(path, []byte(script), 0o600)).To(Succeed())

	out, err := exec.Command("sh", "-n", path).CombinedOutput()
	if err != nil {
		return fmt.Errorf("sh -n: %v: %s", err, out)
	}
	return nil
}

// sha256Hex is the digest the writer's sha256sum produces for the same bytes.
func sha256Hex(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func readFileString(path string) string {
	GinkgoHelper()
	content, err := os.ReadFile(path)
	Expect(err).NotTo(HaveOccurred())
	return string(content)
}

func appendToFile(path, text string) error {
	fh, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = fh.Close() }()
	_, err = fh.WriteString(text)
	return err
}
