package journal

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

type journalState struct {
	lock        sync.Mutex
	initialized bool
	err         error
	unsealed    []Segment
	lastSealed  Segment
}

func (j *Journal) Initialize() error {
	j.state.lock.Lock()
	defer j.state.lock.Unlock()
	return j.state.ensureInitialized(j)
}

func (j *Journal) SegmentCount() (int, error) {
	j.state.lock.Lock()
	defer j.state.lock.Unlock()
	if err := j.state.ensureInitialized(j); err != nil {
		return -1, err
	}
	return len(j.state.unsealed), nil
}

func (j *Journal) needsRotation(now uint64) (bool, error) {
	if j.autorotate.Interval == 0 {
		return false, nil
	}

	j.state.lock.Lock()
	defer j.state.lock.Unlock()
	if err := j.state.ensureInitialized(j); err != nil {
		return false, err
	}
	return j.state.needsRotation(now, &j.autorotate), nil
}

func (j *Journal) lastSegment() (Segment, error) {
	j.state.lock.Lock()
	defer j.state.lock.Unlock()
	if err := j.state.ensureInitialized(j); err != nil {
		return Segment{}, err
	}
	return j.state.last(), nil
}

func (j *Journal) findKnownSegments(filter Filter) ([]Segment, error) {
	j.state.lock.Lock()
	defer j.state.lock.Unlock()
	if err := j.state.ensureInitialized(j); err != nil {
		return nil, err
	}
	segs, _ := j.state.findKnownSegments(filter)
	return segs, nil
}

func (j *Journal) resetState() {
	j.state.lock.Lock()
	defer j.state.lock.Unlock()
	j.state.reset()
}

func (j *Journal) updateStateWithSegmentGone(seg Segment) {
	j.state.lock.Lock()
	defer j.state.lock.Unlock()
	j.state.removeSegment(seg)
}

func (j *Journal) updateStateWithSegmentAdded(seg Segment) {
	j.state.lock.Lock()
	defer j.state.lock.Unlock()
	j.state.addSegment(j, seg)
}

func (j *Journal) updateStateWithSegmentFinalized(oldSeg, newSeg Segment) {
	j.state.lock.Lock()
	defer j.state.lock.Unlock()
	j.state.replaceSegment(oldSeg, newSeg)
}

func (js *journalState) reset() {
	js.initialized = false
	js.err = nil
	js.unsealed = nil
}

func (js *journalState) ensureInitialized(j *Journal) error {
	if js.err != nil {
		return js.err
	}
	if js.initialized {
		return nil
	}
	js.initialized = true

	err := js.initialize(j)
	js.err = err
	return err
}

func (js *journalState) initialize(j *Journal) error {
	var lastSealed Segment
	var unsealed []Segment
	err := j.enumSegments(func(seg Segment) error {
		if seg.status.IsSealed() {
			if lastSealed.IsZero() || compareSegments(seg, lastSealed) > 0 {
				lastSealed = seg
			}
		} else {
			unsealed = append(unsealed, seg)
		}
		return nil
	})
	if err != nil {
		return err
	}

	slices.SortFunc(unsealed, compareSegments)
	js.unsealed = unsealed
	js.lastSealed = lastSealed
	return nil
}

func (js *journalState) last() Segment {
	if n := len(js.unsealed); n > 0 {
		return js.unsealed[n-1]
	} else {
		return js.lastSealed
	}
}

func (js *journalState) needsRotation(now uint64, rotopt *AutorotateOptions) bool {
	last := js.last()
	if last.IsZero() || !last.status.IsDraft() {
		return false
	}
	elapsed := time.Duration(now-last.ts) * time.Millisecond
	return elapsed >= rotopt.Interval
}

func (js *journalState) addSegment(j *Journal, seg Segment) {
	if !js.initialized {
		return
	}
	if n := len(js.unsealed); n > 0 {
		prev := js.unsealed[n-1]
		if compareSegments(seg, prev) <= 0 {
			panic(fmt.Errorf("internal error: %v: adding segment %v after %v", j, seg, prev))
		}
	}
	js.unsealed = append(js.unsealed, seg)
}

func (js *journalState) removeSegment(seg Segment) {
	if !js.initialized {
		return
	}
	if js.lastSealed == seg {
		js.reset()
		return
	}
	if i := slices.Index(js.unsealed, seg); i >= 0 {
		js.unsealed = slices.Delete(js.unsealed, i, i+1)
	}
}

func (js *journalState) replaceSegment(oldSeg, newSeg Segment) {
	if !js.initialized {
		return
	}
	if i := slices.Index(js.unsealed, oldSeg); i >= 0 {
		js.unsealed[i] = newSeg
	}
}

func (js *journalState) findKnownSegments(filter Filter) ([]Segment, bool) {
	unsealed := js.unsealed

	end := len(unsealed)
	for end > 0 {
		last := unsealed[end-1]
		if (filter.MaxRecordID > 0 && last.recnum > filter.MaxRecordID) || (filter.MaxTimestamp > 0 && last.ts > filter.MaxTimestamp) {
			end--
			continue
		}
		break
	}

	start := 0
	exactMatch := false
	for start < end {
		first := unsealed[start]
		if first.recnum == filter.MinRecordID && first.ts == filter.MinTimestamp {
			exactMatch = true
			break
		} else if first.recnum >= filter.MinRecordID && first.ts >= filter.MinTimestamp {
			break
		}
		start++
	}
	if exactMatch {
		return unsealed[start:end], false
	} else if start > 0 {
		return unsealed[start-1 : end], false
	} else {
		return unsealed[0:end], true
	}
}
