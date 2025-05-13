package journal_test

import (
	"bytes"
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/andreyvit/journal"
	"github.com/andreyvit/sealer"
)

var sealKey = &sealer.Key{
	ID:  [32]byte{'X'},
	Key: [32]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
}

type testJournal struct {
	*journal.Journal

	T   testing.TB
	FS  fstest.MapFS
	Dir string

	clock *fakeClock
}

type nonVerboseOpt struct{}

var nonVerbose = nonVerboseOpt{}

func setupWritable(t testing.TB, clock *fakeClock, o journal.Options, opts ...any) *testJournal {
	dir := t.TempDir()
	return open(t, clock, dir, o, opts...)
}

func open(t testing.TB, clock *fakeClock, dir string, o journal.Options, opts ...any) *testJournal {
	if clock == nil {
		clock = newClock()
	}
	j := &testJournal{
		T:   t,
		FS:  make(fstest.MapFS),
		Dir: dir,

		clock: clock,
	}
	o.SealKeys = []*sealer.Key{sealKey}
	o.FileName = "j*.wal"
	o.Now = j.clock.Now
	o.Logger = testLogger(t)
	o.Verbose = true

	for _, opt := range opts {
		switch opt.(type) {
		case nonVerboseOpt:
			o.Verbose = false
		default:
			panic(fmt.Sprintf("unsupported option type: %T", opt))
		}
	}

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
	j.T.Helper()
	var result []journal.Record
	fail := func(err error) {
		j.T.Helper()
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
	slices.SortFunc(names, func(a, b string) int {
		return cmp.Or(
			cmp.Compare(a[2:], b[2:]),
			cmp.Compare(a, b),
		)
	})
	return names
}

func (j *testJournal) Segments() []string {
	var names []string
	for _, seg := range must(j.FindSegments(journal.Filter{})) {
		names = append(names, seg.String())
	}
	slices.Sort(names)
	return names
}
