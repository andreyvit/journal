package journal

import (
	"fmt"
	"os"
	"slices"
	"sync"
	"time"
)

type journalState struct {
	lock        sync.Mutex
	initialized bool
	err         error
	unsealed    []Segment // still considering if we're storing this
	sealed      []Segment
	sealingTemp Segment

	// set after startWriting, not after .initialize
	lastKnown     bool
	lastCommitted Meta
	lastRaw       Meta
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
	segs, _ := j.state.findUnsealedSegments(filter)
	more, _ := j.state.findKnownSealedSegments(filter)
	if len(more) > 0 {
		segs = append(more, segs...)
	}
	return segs, nil
}

func (j *Journal) nextToSeal() (Segment, error) {
	j.state.lock.Lock()
	defer j.state.lock.Unlock()
	if err := j.state.ensureInitialized(j); err != nil {
		return Segment{}, err
	}
	return j.state.nextToSeal(), nil
}

func (j *Journal) nextToTrim() (Segment, error) {
	j.state.lock.Lock()
	defer j.state.lock.Unlock()
	if err := j.state.ensureInitialized(j); err != nil {
		return Segment{}, err
	}
	return j.state.nextToTrim(), nil
}

func (j *Journal) setSealingTemp(seg Segment) {
	j.state.lock.Lock()
	defer j.state.lock.Unlock()
	j.state.sealingTemp = seg
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

func (j *Journal) setLastRecord(lastCommitted, lastRaw Meta) {
	j.state.lock.Lock()
	defer j.state.lock.Unlock()
	j.state.setLastRecord(lastCommitted, lastRaw)
}

func (j *Journal) setLastUncommittedRecord(lastRaw Meta) {
	j.state.lock.Lock()
	defer j.state.lock.Unlock()
	j.state.setLastUncommittedRecord(lastRaw)
}

func (j *Journal) setLastRecordUnknown() {
	j.state.lock.Lock()
	defer j.state.lock.Unlock()
	j.state.setLastRecordUnknown()
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
	var undesirables []Segment
	var all []Segment
	err := j.enumSegments(func(seg Segment) error {
		switch seg.status {
		case sealingTemp:
			undesirables = append(undesirables, seg)
		default:
			all = append(all, seg)
		}
		return nil
	})
	if err != nil {
		return err
	}
	slices.SortFunc(all, compareSegments)

	var sealed []Segment
	var unsealed []Segment
	var maxSeg uint64
	for _, seg := range all {
		if seg.segnum > maxSeg {
			if seg.status.IsSealed() {
				sealed = append(sealed, seg)
			} else {
				unsealed = append(unsealed, seg)
			}
			maxSeg = seg.segnum
		} else {
			undesirables = append(undesirables, seg)
		}
	}
	js.sealed = sealed
	js.unsealed = unsealed

	for _, seg := range undesirables {
		err := j.removeFile(seg)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	return nil
}

func (js *journalState) last() Segment {
	u := js.lastUnsealed()
	s := js.lastSealed()
	if u.IsZero() || (s.IsNonZero() && s.segnum > u.segnum) {
		return s
	} else {
		return u
	}
}

func (js *journalState) lastUnsealed() Segment {
	if n := len(js.unsealed); n > 0 {
		return js.unsealed[n-1]
	} else {
		return Segment{}
	}
}

func (js *journalState) lastSealed() Segment {
	if n := len(js.sealed); n > 0 {
		return js.sealed[n-1]
	} else {
		return Segment{}
	}
}

func (js *journalState) nextToSeal() Segment {
	last := js.lastSealed()
	for _, seg := range js.unsealed {
		if last.IsZero() || seg.segnum > last.segnum {
			return seg
		}
	}
	return Segment{}
}

func (js *journalState) nextToTrim() Segment {
	if n := len(js.unsealed); n > 0 {
		seg := js.unsealed[0]
		lastSealed := js.lastSealed()
		if lastSealed.IsNonZero() && seg.segnum <= lastSealed.segnum {
			return seg
		}
	}
	return Segment{}
}

func (js *journalState) summary() Summary {
	s := Summary{
		FirstSealedSegment: Segment{},
		LastSealedSegment:  Segment{},
		SegmentCount:       len(js.unsealed),
		LastCommitted:      js.lastCommitted,
		LastUncommitted:    js.lastRaw,
	}
	if n := len(js.unsealed); n > 0 {
		first := js.unsealed[0]
		s.FirstUnsealedSegment = first
		s.LastUnsealedSegment = js.unsealed[n-1]
		s.SegmentCount = n
	}
	if n := len(js.sealed); n > 0 {
		first := js.sealed[0]
		s.FirstSealedSegment = first
		s.LastSealedSegment = js.sealed[n-1]
		s.SegmentCount += n
	}
	return s
}

func (js *journalState) setLastRecord(lastCommitted, lastRaw Meta) {
	js.lastKnown = true
	js.lastCommitted = lastCommitted
	js.lastRaw = lastRaw
}

func (js *journalState) setLastRecordUnknown() {
	js.lastKnown = false
}

func (js *journalState) setLastUncommittedRecord(lastRaw Meta) {
	js.lastKnown = true
	js.lastRaw = lastRaw
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
	if seg.status.IsSealed() {
		if n := len(js.sealed); n > 0 {
			prev := js.sealed[n-1]
			if compareSegments(seg, prev) <= 0 {
				panic(fmt.Errorf("internal error: %v: adding sealed segment %v after %v", j, seg, prev))
			}
		}
		js.sealed = append(js.sealed, seg)
	} else {
		if n := len(js.unsealed); n > 0 {
			prev := js.unsealed[n-1]
			if compareSegments(seg, prev) <= 0 {
				panic(fmt.Errorf("internal error: %v: adding unsealed segment %v after %v", j, seg, prev))
			}
		}
		js.unsealed = append(js.unsealed, seg)
	}
}

func (js *journalState) removeSegment(seg Segment) {
	if !js.initialized {
		return
	}
	if seg.status.IsSealed() {
		if i := slices.Index(js.sealed, seg); i >= 0 {
			js.sealed = slices.Delete(js.sealed, i, i+1)
		}
	} else {
		if i := slices.Index(js.unsealed, seg); i >= 0 {
			js.unsealed = slices.Delete(js.unsealed, i, i+1)
		}
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

func (js *journalState) findKnownSealedSegments(filter Filter) ([]Segment, bool) {
	sealed := js.sealedSegmentsBeforeUnsealed()

	end := len(sealed)
	for end > 0 {
		last := sealed[end-1]
		if (filter.MaxRecordID > 0 && last.recnum > filter.MaxRecordID) || (filter.MaxTimestamp > 0 && last.ts > filter.MaxTimestamp) {
			end--
			continue
		}
		break
	}

	start := 0
	exactMatch := false
	for start < end {
		first := sealed[start]
		if first.recnum == filter.MinRecordID && first.ts == filter.MinTimestamp {
			exactMatch = true
			break
		} else if first.recnum >= filter.MinRecordID && first.ts >= filter.MinTimestamp {
			break
		}
		start++
	}
	if exactMatch {
		return sealed[start:end], false
	} else if start > 0 {
		return sealed[start-1 : end], false
	} else {
		return sealed[0:end], true
	}
}

func (js *journalState) sealedSegmentsBeforeUnsealed() []Segment {
	sealed := js.sealed
	if len(js.unsealed) == 0 {
		return sealed
	}

	first := js.unsealed[0]
	for len(sealed) > 0 && sealed[len(sealed)-1].segnum >= first.segnum {
		sealed = sealed[:len(sealed)-1]
	}
	return sealed
}

func (js *journalState) findUnsealedSegments(filter Filter) ([]Segment, bool) {
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
