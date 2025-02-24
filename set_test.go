package journal_test

import (
	"context"
	"testing"
	"time"

	"github.com/andreyvit/journal"
)

func TestSet(t *testing.T) {
	clock := newClock()

	set := journal.NewSet(journal.SetOptions{
		Now:    clock.Now,
		Logger: testLogger(t),
	})

	j1 := setupWritable(t, clock, journal.Options{
		MaxFileSize: 160,
		Autorotate: journal.AutorotateOptions{
			Interval: 1 * time.Hour,
		},
	})
	set.Add(j1.Journal)

	j2 := setupWritable(t, clock, journal.Options{
		MaxFileSize: 160,
		Autorotate: journal.AutorotateOptions{
			Interval: 30 * time.Minute,
		},
	})
	set.Add(j2.Journal)

	eq(t, set.Process(context.Background()), 0)

	ensure(j1.WriteRecord(0, []byte("hello")))
	ensure(j2.WriteRecord(0, []byte("world")))
	ensure(j1.WriteRecord(0, []byte("boo")))

	eq(t, set.Process(context.Background()), 0)

	clock.Advance(time.Hour)
	eq(t, set.Process(context.Background()), 2)
}
