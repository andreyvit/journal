package journal

import (
	"errors"
	"os"
	"slices"
	"time"
)

var ErrInspectionBusy = errors.New("journal: inspection busy")

type SegmentInfo struct {
	Number      uint64
	Name        string
	Status      Status
	Size        int64
	FirstRecord Meta
	LastRecord  Meta
	Metadata    SegmentMetadata
	Records     uint64
	Readers     int
	Writers     int
	Pinned      bool
	LastAccess  time.Time
	Err         error
}

func (j *Journal) InspectSegments() ([]SegmentInfo, error) {
	if !j.writer.writeLock.TryLock() {
		return nil, ErrInspectionBusy
	}
	if !j.state.lock.TryLock() {
		j.writer.writeLock.Unlock()
		return nil, ErrInspectionBusy
	}
	if !j.retention.mu.TryLock() {
		j.state.lock.Unlock()
		j.writer.writeLock.Unlock()
		return nil, ErrInspectionBusy
	}
	if !j.state.initialized || j.state.err != nil {
		err := j.state.err
		if err == nil {
			err = errors.New("journal: inspection unavailable before initialization")
		}
		j.retention.mu.Unlock()
		j.state.lock.Unlock()
		j.writer.writeLock.Unlock()
		return nil, err
	}
	segments := append(slices.Clone(j.state.sealed), j.state.unsealed...)
	slices.SortFunc(segments, compareSegments)
	var (
		infos       = make([]SegmentInfo, len(segments))
		minimumHead = ^uint64(0)
		hasHeads    = len(j.retention.heads) != 0
	)
	for head := range j.retention.heads {
		minimumHead = min(minimumHead, head.recordID)
	}
	for i, seg := range segments {
		info := &infos[i]
		info.Number = seg.segnum
		info.Name = seg.fileName(j)
		info.Status = seg.status
		info.FirstRecord = seg.FirstRecord()
		info.LastAccess = j.retention.accessed[seg.segnum]
		if sw := j.writer.segWriter; sw != nil && sw.seg.segnum == seg.segnum {
			info.Writers = 1
			info.Size = sw.size
			info.Metadata = sw.committedMetadata
			if j.state.lastKnown && j.state.lastCommitted.ID >= seg.recnum {
				info.LastRecord = j.state.lastCommitted
				info.Records = info.LastRecord.ID - seg.recnum + 1
			}
		}
		for head := range j.retention.heads {
			if !head.hidden && head.segment == seg.segnum {
				info.Readers++
			}

		}
	}
	j.retention.mu.Unlock()
	j.state.lock.Unlock()
	j.writer.writeLock.Unlock()
	for i, seg := range segments {
		info := &infos[i]
		stat, err := os.Stat(j.filePath(info.Name))
		if err != nil {
			info.Err = err
			continue
		}
		info.Size = stat.Size()
		if info.Writers != 0 {
			continue
		}
		var header segmentHeader
		if seg.status.IsDraft() {
			file, err := j.openFile(seg, false)
			if err != nil {
				info.Err = err
				continue
			}
			reader, err := verifySegment(j, file, seg)
			file.Close()
			if err != nil {
				info.Err = err
				continue
			}
			info.LastRecord = Meta{ID: reader.committedRec, Timestamp: reader.committedTS}
			info.Metadata = reader.committedMetadata
		} else {
			if err = loadSegmentHeader(j, &header, seg); err != nil {
				info.Err = err
				continue
			}
			info.LastRecord = Meta{ID: header.LastRecordNumber, Timestamp: header.LastTimestamp}
			if j.updateSegmentMetadata != nil {
				info.Metadata = header.SegmentInvariant
			}
		}
		if info.LastRecord.ID >= seg.recnum {
			info.Records = info.LastRecord.ID - seg.recnum + 1
		}
	}
	for i := range infos {
		infos[i].Pinned = hasHeads && minimumHead <= max(infos[i].LastRecord.ID, segments[i].recnum)
	}
	return infos, nil
}
