package journal

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

type RetentionOptions struct {
	TTL      time.Duration
	IdleTTL  time.Duration
	MaxFiles int
}

type PruneBoundary struct {
	LastRecord Record
}

type PruneResult struct {
	Deleted int
	Blocked bool
	Through uint64
}

type retentionState struct {
	mu                 sync.Mutex
	heads              map[*readHead]struct{}
	accessed           map[uint64]time.Time
	unavailableThrough uint64
	retainedThrough    uint64
}

type readHead struct {
	hidden   bool
	journal  *Journal
	recordID uint64
	segment  uint64
}

func (j *Journal) newReadHead(recordID uint64) (*readHead, error) {
	j.retention.mu.Lock()
	defer j.retention.mu.Unlock()
	if recordID == 0 {
		recordID = j.retention.retainedThrough + 1
	}
	if recordID <= j.retention.unavailableThrough {
		return nil, ErrTailGap
	}
	if j.retention.heads == nil {
		j.retention.heads = make(map[*readHead]struct{})
	}
	head := &readHead{journal: j, recordID: recordID}
	j.retention.heads[head] = struct{}{}
	return head, nil
}

func (h *readHead) advance(recordID, segment uint64) {
	if h == nil {
		return
	}
	j := h.journal
	j.retention.mu.Lock()
	defer j.retention.mu.Unlock()
	h.recordID = max(h.recordID, recordID)
	h.segment = segment
	j.touchSegmentLocked(segment, j.now())
}

func (h *readHead) close() {
	if h == nil {
		return
	}
	j := h.journal
	j.retention.mu.Lock()
	defer j.retention.mu.Unlock()
	if h.segment != 0 {
		j.touchSegmentLocked(h.segment, j.now())
	}
	delete(j.retention.heads, h)
}

func (j *Journal) touchSegment(segment uint64) {
	j.retention.mu.Lock()
	defer j.retention.mu.Unlock()
	j.touchSegmentLocked(segment, j.now())
}

func (j *Journal) touchSegmentLocked(segment uint64, now time.Time) {
	if j.retention.accessed == nil {
		j.retention.accessed = make(map[uint64]time.Time)
	}
	if now.After(j.retention.accessed[segment]) {
		j.retention.accessed[segment] = now
	}
}

func (j *Journal) Prune(
	opt RetentionOptions,
	before func(PruneBoundary) error,
) (PruneResult, error) {
	var result PruneResult
	if opt.TTL < 0 || opt.IdleTTL < 0 || opt.MaxFiles < 0 {
		return result, errors.New("journal: negative retention limit")
	}
	if opt.TTL == 0 && opt.MaxFiles == 0 {
		return result, nil
	}
	j.trimLock.Lock()
	defer j.trimLock.Unlock()
	j.writer.writeLock.Lock()
	defer j.writer.writeLock.Unlock()
	if err := j.writer.ensurePreparedToWrite_locked(); err != nil {
		return result, err
	}
	segments, err := j.FindSegments(Filter{})
	if err != nil {
		return result, err
	}
	var (
		candidates []Segment
		headers    []segmentHeader
	)
	for i, segment := range segments {
		if i == len(segments)-1 || segment.status.IsDraft() {
			break
		}
		var header segmentHeader
		if err = loadSegmentHeader(j, &header, segment); err != nil {
			return result, err
		}
		candidates = append(candidates, segment)
		headers = append(headers, header)
	}
	var (
		now     = j.now()
		count   int
		through uint64
	)
	j.retention.mu.Lock()
	previousFloor := j.retention.unavailableThrough
	minimumHead := ^uint64(0)
	for head := range j.retention.heads {
		minimumHead = min(minimumHead, head.recordID)
	}
	for _, segment := range candidates {
		if _, ok := j.retention.accessed[segment.segnum]; !ok {
			j.touchSegmentLocked(segment.segnum, now)
		}
	}
	for i, segment := range candidates {
		header := headers[i]
		expired := opt.TTL > 0 && now.Sub(ToTime(header.LastTimestamp)) >= opt.TTL
		excess := opt.MaxFiles > 0 && len(segments)-count > opt.MaxFiles
		if !expired && !excess {
			break
		}
		pinned := len(j.retention.heads) > 0 && minimumHead <= header.LastRecordNumber
		if pinned || now.Sub(j.retention.accessed[segment.segnum]) < opt.IdleTTL {
			result.Blocked = true
			break
		}
		count++
		through = header.LastRecordNumber
	}
	if count == 0 {
		j.retention.mu.Unlock()
		return result, nil
	}
	j.retention.unavailableThrough = max(previousFloor, through)
	j.retention.mu.Unlock()
	authorized := false
	defer func() {
		j.retention.mu.Lock()
		if !authorized {
			j.retention.unavailableThrough = previousFloor
		}
		j.retention.mu.Unlock()
	}()
	last, err := j.lastRecordInSegment(candidates[count-1], through)
	if err != nil {
		return result, err
	}
	if before != nil {
		if err = before(PruneBoundary{LastRecord: last}); err != nil {
			return result, err
		}
	}
	authorized = true
	j.retention.mu.Lock()
	j.retention.retainedThrough = max(j.retention.retainedThrough, through)
	j.retention.mu.Unlock()
	result.Through = through
	for _, segment := range candidates[:count] {
		if err = os.Remove(j.filePath(segment.fileName(j))); err != nil && !os.IsNotExist(err) {
			return result, errors.Join(err, j.syncDirectory())
		}
		j.updateStateWithSegmentGone(segment)
		result.Deleted++
	}
	return result, j.syncDirectory()
}

func (j *Journal) lastRecordInSegment(segment Segment, recordID uint64) (Record, error) {
	file, reader, err := openSegment(j, segment)
	if err != nil {
		return Record{}, err
	}
	defer file.Close()
	var last Record
	for {
		err = reader.next()
		if err == io.EOF {
			if last.ID != recordID || reader.rec != recordID || reader.committedRec != recordID || reader.ts != reader.h.LastTimestamp {
				return Record{}, ErrInternal
			}
			return last, nil
		}
		if err != nil {
			return Record{}, err
		}
		if reader.rec == recordID {
			last = Record{ID: reader.rec, Timestamp: reader.ts, Data: bytes.Clone(reader.data)}
		}
	}
}
