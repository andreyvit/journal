package journal_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/andreyvit/journal"
)

func TestMergedRecords_basic(t *testing.T) {
	tempDirA := t.TempDir()
	tempDirB := t.TempDir()

	opts := journal.Options{
		FileName: "j*.wal",
		Now:      time.Now,
	}

	journalA := journal.New(tempDirA, opts)
	journalB := journal.New(tempDirB, opts)

	journalA.StartWriting()
	journalB.StartWriting()

	timestamp := journal.ToTimestamp(time.Now())

	success(t, journalA.WriteRecord(timestamp+1, []byte("alpha")))
	success(t, journalB.WriteRecord(timestamp+2, []byte("beta")))

	success(t, journalA.Commit())
	success(t, journalB.Commit())

	journals := map[uint64]*journal.Journal{
		1: journalA,
		2: journalB,
	}

	filter := journal.Filter{}
	var errors []error
	fail := func(err error) {
		errors = append(errors, err)
	}

	var results []journal.RecordWithSource
	for rec := range journal.MergedRecords(journals, filter, fail) {
		t.Logf("Got record ID=%d, Source=%d, Data=%q", rec.ID, rec.Source, rec.Data)
		results = append(results, rec)
	}

	eq(t, len(errors), 0)
	eq(t, len(results), 2)

	for _, rec := range results {
		switch rec.Source {
		case 1:
			deepEq(t, rec.Data, []byte("alpha"))
		case 2:
			deepEq(t, rec.Data, []byte("beta"))
		}
	}

	success(t, journalA.FinishWriting())
	success(t, journalB.FinishWriting())
}

func TestMergedRecords_sorts_by_timestamp_not_id(t *testing.T) {
	testJournals, _ := setupTestJournals(t, 3)
	journalA := testJournals[0]
	journalB := testJournals[1]
	journalC := testJournals[2]

	baseTime := journal.ToTimestamp(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC))

	timestampsA := []uint64{baseTime + 100, baseTime + 300, baseTime + 500, baseTime + 700, baseTime + 900, baseTime + 1100, baseTime + 1300}
	timestampsB := []uint64{baseTime + 200, baseTime + 400, baseTime + 600, baseTime + 800, baseTime + 1000, baseTime + 1200, baseTime + 1400}
	timestampsC := []uint64{baseTime + 150, baseTime + 350, baseTime + 550, baseTime + 750, baseTime + 950, baseTime + 1150, baseTime + 1350}

	for i, ts := range timestampsA {
		data := fmt.Sprintf("A-%d-ts%d", i+1, ts)
		success(t, journalA.WriteRecord(ts, []byte(data)))
	}
	for i, ts := range timestampsB {
		data := fmt.Sprintf("B-%d-ts%d", i+10, ts)
		success(t, journalB.WriteRecord(ts, []byte(data)))
	}
	for i, ts := range timestampsC {
		data := fmt.Sprintf("C-%d-ts%d", i+100, ts)
		success(t, journalC.WriteRecord(ts, []byte(data)))
	}

	success(t, journalA.Commit())
	success(t, journalB.Commit())
	success(t, journalC.Commit())

	journals := map[uint64]*journal.Journal{
		1: journalA,
		2: journalB,
		3: journalC,
	}

	results := collectMergedRecords(t, journals, journal.Filter{})
	eq(t, len(results), 21)

	expectedOrder := []struct {
		source     uint64
		timestamp  uint64
		dataPrefix string
	}{
		{1, baseTime + 100, "A-1"},
		{3, baseTime + 150, "C-100"},
		{2, baseTime + 200, "B-10"},
		{1, baseTime + 300, "A-2"},
		{3, baseTime + 350, "C-101"},
		{2, baseTime + 400, "B-11"},
		{1, baseTime + 500, "A-3"},
		{3, baseTime + 550, "C-102"},
		{2, baseTime + 600, "B-12"},
		{1, baseTime + 700, "A-4"},
		{3, baseTime + 750, "C-103"},
		{2, baseTime + 800, "B-13"},
		{1, baseTime + 900, "A-5"},
		{3, baseTime + 950, "C-104"},
		{2, baseTime + 1000, "B-14"},
		{1, baseTime + 1100, "A-6"},
		{3, baseTime + 1150, "C-105"},
		{2, baseTime + 1200, "B-15"},
		{1, baseTime + 1300, "A-7"},
		{3, baseTime + 1350, "C-106"},
		{2, baseTime + 1400, "B-16"},
	}

	for i, expected := range expectedOrder {
		actual := results[i]
		eq(t, actual.Source, expected.source)
		eq(t, actual.Timestamp, expected.timestamp)

		dataStr := string(actual.Data)
		if len(dataStr) < len(expected.dataPrefix) || dataStr[:len(expected.dataPrefix)] != expected.dataPrefix {
			t.Errorf("record %d: expected data prefix %q, got %q", i, expected.dataPrefix, dataStr)
		}
	}

	success(t, journalA.FinishWriting())
	success(t, journalB.FinishWriting())
	success(t, journalC.FinishWriting())
}

func TestMergedRecords_overlapping_ids_different_timestamps(t *testing.T) {
	testJournals, _ := setupTestJournals(t, 2)
	journalA := testJournals[0]
	journalB := testJournals[1]

	baseTime := journal.ToTimestamp(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC))

	success(t, journalA.WriteRecord(baseTime+100, []byte("A-ID1-ts100")))
	success(t, journalA.WriteRecord(baseTime+300, []byte("A-ID2-ts300")))
	success(t, journalA.WriteRecord(baseTime+500, []byte("A-ID3-ts500")))

	success(t, journalB.WriteRecord(baseTime+200, []byte("B-ID1-ts200")))
	success(t, journalB.WriteRecord(baseTime+400, []byte("B-ID2-ts400")))
	success(t, journalB.WriteRecord(baseTime+600, []byte("B-ID3-ts600")))

	success(t, journalA.Commit())
	success(t, journalB.Commit())

	journals := map[uint64]*journal.Journal{
		1: journalA,
		2: journalB,
	}

	results := collectMergedRecords(t, journals, journal.Filter{})
	eq(t, len(results), 6)

	expectedOrder := []struct {
		source    uint64
		timestamp uint64
		data      string
	}{
		{1, baseTime + 100, "A-ID1-ts100"},
		{2, baseTime + 200, "B-ID1-ts200"},
		{1, baseTime + 300, "A-ID2-ts300"},
		{2, baseTime + 400, "B-ID2-ts400"},
		{1, baseTime + 500, "A-ID3-ts500"},
		{2, baseTime + 600, "B-ID3-ts600"},
	}

	for i, expected := range expectedOrder {
		actual := results[i]
		eq(t, actual.Source, expected.source)
		eq(t, actual.Timestamp, expected.timestamp)
		deepEq(t, actual.Data, []byte(expected.data))
	}

	success(t, journalA.FinishWriting())
	success(t, journalB.FinishWriting())
}

func TestMergedRecords_identical_timestamps_stable_ordering(t *testing.T) {
	testJournals, _ := setupTestJournals(t, 3)
	journalA := testJournals[0]
	journalB := testJournals[1]
	journalC := testJournals[2]

	sameTime := journal.ToTimestamp(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC))
	laterTime := sameTime + 1000

	success(t, journalA.WriteRecord(sameTime, []byte("A-1-same")))
	success(t, journalA.WriteRecord(sameTime, []byte("A-2-same")))
	success(t, journalA.WriteRecord(laterTime, []byte("A-3-later")))

	success(t, journalB.WriteRecord(sameTime, []byte("B-1-same")))
	success(t, journalB.WriteRecord(sameTime, []byte("B-2-same")))
	success(t, journalB.WriteRecord(laterTime, []byte("B-3-later")))

	success(t, journalC.WriteRecord(sameTime, []byte("C-1-same")))
	success(t, journalC.WriteRecord(sameTime, []byte("C-2-same")))
	success(t, journalC.WriteRecord(laterTime, []byte("C-3-later")))

	success(t, journalA.Commit())
	success(t, journalB.Commit())
	success(t, journalC.Commit())

	journals := map[uint64]*journal.Journal{
		1: journalA,
		2: journalB,
		3: journalC,
	}

	results := collectMergedRecords(t, journals, journal.Filter{})
	eq(t, len(results), 9)

	sameTimeCount := 0
	laterTimeCount := 0
	seenSources := make(map[uint64][]int)

	for i, rec := range results {
		if rec.Timestamp == sameTime {
			sameTimeCount++
			seenSources[rec.Source] = append(seenSources[rec.Source], i)
		} else if rec.Timestamp == laterTime {
			laterTimeCount++
		}
	}

	eq(t, sameTimeCount, 6)
	eq(t, laterTimeCount, 3)

	for source, positions := range seenSources {
		for i := 1; i < len(positions); i++ {
			if positions[i] <= positions[i-1] {
				t.Errorf("source %d: records not in stable order, positions %v", source, positions)
			}
		}
	}

	success(t, journalA.FinishWriting())
	success(t, journalB.FinishWriting())
	success(t, journalC.FinishWriting())
}

func TestMergedRecords_many_records_complex_patterns(t *testing.T) {
	tempDirs := make([]string, 4)
	journals := make(map[uint64]*journal.Journal)
	sources := []uint64{1, 2, 3, 4}

	opts := journal.Options{
		FileName: "j*.wal",
		Now:      time.Now,
	}

	for i, source := range sources {
		tempDirs[i] = t.TempDir()
		journals[source] = journal.New(tempDirs[i], opts)
		journals[source].StartWriting()
	}

	baseTime := journal.ToTimestamp(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	type recordInfo struct {
		source    uint64
		timestamp uint64
		data      string
	}

	var expectedOrder []recordInfo

	patterns := map[uint64][]uint64{
		1: {50, 250, 450, 650, 850, 1050, 1250, 1450, 1650, 1850},
		2: {150, 350, 550, 750, 950, 1150, 1350, 1550, 1750, 1950},
		3: {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000},
		4: {75, 275, 475, 675, 875, 1075, 1275, 1475, 1675, 1875},
	}

	for source, timestamps := range patterns {
		for i, offset := range timestamps {
			ts := baseTime + offset
			data := fmt.Sprintf("%d-%d-ts%d", source, i+1, offset)
			success(t, journals[source].WriteRecord(ts, []byte(data)))
			expectedOrder = append(expectedOrder, recordInfo{source, ts, data})
		}
	}

	for _, j := range journals {
		success(t, j.Commit())
	}

	slices.SortFunc(expectedOrder, func(a, b recordInfo) int {
		if a.timestamp < b.timestamp {
			return -1
		} else if a.timestamp > b.timestamp {
			return 1
		}
		if a.source < b.source {
			return -1
		} else if a.source > b.source {
			return 1
		}
		return 0
	})

	results := collectMergedRecords(t, journals, journal.Filter{})
	eq(t, len(results), 40)

	for i, expected := range expectedOrder {
		actual := results[i]
		eq(t, actual.Source, expected.source)
		eq(t, actual.Timestamp, expected.timestamp)
		deepEq(t, actual.Data, []byte(expected.data))
	}

	for _, j := range journals {
		success(t, j.FinishWriting())
	}
}

func TestMergedRecords_reverse_chronological_ids(t *testing.T) {
	testJournals, _ := setupTestJournals(t, 2)
	journalA := testJournals[0]
	journalB := testJournals[1]

	baseTime := journal.ToTimestamp(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC))

	timestampsA := []uint64{
		baseTime + 600,
		baseTime + 700,
		baseTime + 800,
		baseTime + 900,
		baseTime + 1000,
	}

	timestampsB := []uint64{
		baseTime + 550,
		baseTime + 650,
		baseTime + 750,
		baseTime + 850,
		baseTime + 950,
	}

	for i, ts := range timestampsA {
		data := fmt.Sprintf("A-ID%d-ts%d", i+1, ts-baseTime)
		success(t, journalA.WriteRecord(ts, []byte(data)))
	}

	for i, ts := range timestampsB {
		data := fmt.Sprintf("B-ID%d-ts%d", i+1, ts-baseTime)
		success(t, journalB.WriteRecord(ts, []byte(data)))
	}

	success(t, journalA.Commit())
	success(t, journalB.Commit())

	journals := map[uint64]*journal.Journal{
		1: journalA,
		2: journalB,
	}

	results := collectMergedRecords(t, journals, journal.Filter{})
	eq(t, len(results), 10)

	expectedOrder := []struct {
		source uint64
		ts     uint64
		data   string
	}{
		{2, baseTime + 550, "B-ID1-ts550"},
		{1, baseTime + 600, "A-ID1-ts600"},
		{2, baseTime + 650, "B-ID2-ts650"},
		{1, baseTime + 700, "A-ID2-ts700"},
		{2, baseTime + 750, "B-ID3-ts750"},
		{1, baseTime + 800, "A-ID3-ts800"},
		{2, baseTime + 850, "B-ID4-ts850"},
		{1, baseTime + 900, "A-ID4-ts900"},
		{2, baseTime + 950, "B-ID5-ts950"},
		{1, baseTime + 1000, "A-ID5-ts1000"},
	}

	for i, expected := range expectedOrder {
		actual := results[i]
		eq(t, actual.Source, expected.source)
		eq(t, actual.Timestamp, expected.ts)
		deepEq(t, actual.Data, []byte(expected.data))
	}

	success(t, journalA.FinishWriting())
	success(t, journalB.FinishWriting())
}

func setupTestJournals(t *testing.T, count int) ([]*journal.Journal, []string) {
	t.Helper()
	journals := make([]*journal.Journal, count)
	dirs := make([]string, count)

	opts := journal.Options{
		FileName: "j*.wal",
		Now:      time.Now,
	}

	for i := range count {
		dirs[i] = t.TempDir()
		journals[i] = journal.New(dirs[i], opts)
		journals[i].StartWriting()
	}

	return journals, dirs
}

func collectMergedRecords(t *testing.T, journals map[uint64]*journal.Journal, filter journal.Filter) []journal.RecordWithSource {
	t.Helper()
	var errors []error
	fail := func(err error) {
		errors = append(errors, err)
	}

	var results []journal.RecordWithSource
	for rec := range journal.MergedRecords(journals, filter, fail) {
		rec.Data = slices.Clone(rec.Data)
		results = append(results, rec)
	}

	eq(t, len(errors), 0)
	return results
}
