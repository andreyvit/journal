package journal_test

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/andreyvit/journal"
)

type testJournal struct {
	*journal.Journal

	T   testing.TB
	FS  fstest.MapFS
	Dir string

	clock *fakeClock
}

func setupWritable(t *testing.T, clock *fakeClock, o journal.Options) *testJournal {
	dir := t.TempDir()
	if clock == nil {
		clock = newClock()
	}
	j := &testJournal{
		T:   t,
		FS:  make(fstest.MapFS),
		Dir: dir,

		clock: clock,
	}
	o.FileName = "j*.wal"
	o.Now = j.clock.Now
	o.Logger = testLogger(t)
	o.Verbose = true

	j.Journal = journal.New(dir, o)
	j.StartWriting()
	t.Cleanup(func() {
		err := j.FinishWriting()
		if err != nil {
			t.Error(err)
		}
	})
	return j
}

func (j *testJournal) Eq(fileName string, expected ...string) {
	j.T.Helper()
	bytesEq(j.T, j.Data(fileName), expand(expected...))
}

func (j *testJournal) Put(fileName string, expected ...string) {
	ensure(os.WriteFile(filepath.Join(j.Dir, fileName), expand(expected...), 0o644))
}

func (j *testJournal) All(filter journal.Filter) []journal.Record {
	var result []journal.Record
	fail := func(err error) {
		j.T.Fatalf("journal error: %v", err)
	}
	for rec := range j.Journal.Records(filter, fail) {
		rec.Data = bytes.Clone(rec.Data)
		result = append(result, rec)
	}
	return result
}

func (j *testJournal) Data(fileName string) []byte {
	b, err := os.ReadFile(filepath.Join(j.Dir, fileName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		j.T.Fatalf("when reading %v: %v", fileName, err)
	}
	return b
}

func (j *testJournal) FileNames() []string {
	var names []string
	for _, env := range must(os.ReadDir(j.Dir)) {
		names = append(names, env.Name())
	}
	slices.Sort(names)
	return names
}
