package journal

import (
	"cmp"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"
)

type Segment struct {
	ts     uint64
	recnum uint64
	segnum uint64
	status Status
}

func (seg Segment) Timestamp() uint64 { return seg.ts }

func (seg Segment) Time() time.Time { return ToTime(seg.ts) }

func (seg Segment) RecordNumber() uint64 { return seg.recnum }

func (seg Segment) SegmentNumber() uint64 { return seg.segnum }

func (seg Segment) FirstRecord() Meta {
	return Meta{ID: seg.recnum, Timestamp: seg.ts}
}

func (seg Segment) String() string {
	if seg.IsZero() {
		return ""
	}
	return formatSegmentName("", "", seg)
}

func (seg Segment) IsZero() bool    { return seg.segnum == 0 }
func (seg Segment) IsNonZero() bool { return !seg.IsZero() }

func compareSegments(a, b Segment) int {
	if a.segnum != b.segnum {
		return cmp.Compare(a.segnum, b.segnum)
	}
	return cmp.Compare(a.status, b.status)
}

func (seg Segment) fileName(j *Journal) string {
	return formatSegmentName(j.fileNamePrefix, j.fileNameSuffix, seg)
}

func (j *Journal) FindSegments(filter Filter) ([]Segment, error) {
	return j.findKnownSegments(filter)
}

func (j *Journal) findUnknownSegments(filter Filter) ([]Segment, error) {
	var segs []Segment
	var bestPrev Segment
	err := j.enumSegments(func(seg Segment) error {
		if seg.recnum < filter.MinRecordID || seg.ts < filter.MinTimestamp {
			if bestPrev.IsZero() || compareSegments(seg, bestPrev) > 0 {
				bestPrev = seg
			}
			return nil
		}
		if filter.MaxRecordID != 0 && seg.recnum > filter.MaxRecordID {
			return nil
		}
		if filter.MaxTimestamp != 0 && seg.ts > filter.MaxTimestamp {
			return nil
		}
		segs = append(segs, seg)
		return nil
	})
	if err != nil {
		return nil, err
	}

	if !bestPrev.IsZero() {
		segs = append(segs, bestPrev)
	}

	slices.SortFunc(segs, compareSegments)
	return segs, nil
}

func (j *Journal) enumSegments(f func(Segment) error) error {
	dirf, err := os.Open(j.dir)
	if err != nil {
		return err
	}
	defer dirf.Close()

	ds, err := dirf.Stat()
	if err != nil {
		return err
	}
	if !ds.IsDir() {
		return fmt.Errorf("%v: not a directory", j.debugName)
	}

	for {
		ents, err := dirf.ReadDir(16)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		for _, ent := range ents {
			if !ent.Type().IsRegular() {
				continue
			}
			name := ent.Name()
			if !strings.HasPrefix(name, j.fileNamePrefix) || !strings.HasSuffix(name, j.fileNameSuffix) {
				continue
			}

			seg, err := parseSegmentName(j.fileNamePrefix, j.fileNameSuffix, name)
			if err != nil {
				return err
			}
			err = f(seg)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
