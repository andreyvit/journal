package journal

import (
	"cmp"
	"io"
	"io/fs"
	"os"
	"slices"
	"strings"
)

type Segment struct {
	ts     uint64
	recnum uint64
	segnum uint32
	status Status
}

func (seg Segment) String() string {
	return formatSegmentName("", "", seg)
}

func (seg Segment) IsZero() bool {
	return seg.segnum == 0
}

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
	dirf, err := os.Open(j.dir)
	if err != nil {
		return nil, err
	}
	defer dirf.Close()

	var segs []Segment
	for {
		ents, err := dirf.ReadDir(16)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
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
				return nil, err
			}

			if filter.MaxRecordID != 0 && seg.recnum > filter.MaxRecordID {
				continue
			}
			if filter.MaxTimestamp != 0 && seg.ts > filter.MaxTimestamp {
				continue
			}
			segs = append(segs, seg)
		}
	}

	slices.SortFunc(segs, compareSegments)

	if filter.MinRecordID != 0 {
		cutoff := -1
		for i, seg := range segs {
			if seg.recnum == filter.MinRecordID {
				cutoff = i
				break
			} else if seg.recnum > filter.MinRecordID {
				cutoff = i - 1
				break
			}
		}
		if cutoff > 0 {
			segs = segs[cutoff:]
		}
	}

	if filter.MinTimestamp != 0 {
		cutoff := -1
		for i, seg := range segs {
			if seg.ts == filter.MinTimestamp {
				cutoff = i
				break
			} else if seg.ts > filter.MinTimestamp {
				cutoff = i - 1
				break
			}
		}
		if cutoff > 0 {
			segs = segs[cutoff:]
		}
	}

	return segs, nil
}

func (j *Journal) findLastSegment(dirf fs.ReadDirFile) (Segment, error) {
	var last Segment
	for {
		if err := j.context.Err(); err != nil {
			break
		}

		ents, err := dirf.ReadDir(16)
		if err == io.EOF {
			break
		}
		for _, ent := range ents {
			if !ent.Type().IsRegular() {
				continue
			}
			name := ent.Name()
			if !strings.HasPrefix(name, j.fileNamePrefix) {
				continue
			}
			if !strings.HasSuffix(name, j.fileNameSuffix) {
				continue
			}

			seg, err := parseSegmentName(j.fileNamePrefix, j.fileNameSuffix, name)
			if err != nil {
				return Segment{}, err
			}
			if last.IsZero() || compareSegments(seg, last) > 0 {
				last = seg
			}
		}
	}
	return last, nil
}
