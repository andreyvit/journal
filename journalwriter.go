package journal

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type closeMode int

const (
	closeAndContinueLater closeMode = iota
	closeWithoutCommitting
	closeAndFinalize
)

func (m closeMode) shouldCommit() bool {
	return m != closeWithoutCommitting
}
func (m closeMode) shouldFinalize() bool {
	return m == closeAndFinalize
}

type journalWriter struct {
	j *Journal

	writeLock  sync.Mutex
	writable   bool
	writeErr   error
	segWriter  *segmentWriter
	nextSegNum uint64
	nextRecNum uint64
}

func (jw *journalWriter) StartWriting() {
	jw.writeLock.Lock()
	if jw.writable || jw.writeErr != nil {
		jw.writeLock.Unlock()
		return
	}
	jw.writable = true

	go func() {
		defer jw.writeLock.Unlock()
		jw.fail_locked(jw.prepareToWrite_locked())
	}()
}

func (jw *journalWriter) FinishWriting(mode closeMode) error {
	jw.writeLock.Lock()
	defer jw.writeLock.Unlock()
	if err := jw.finishWriting_locked(mode); err != nil {
		return err
	}
	return jw.writeErr
}

func (jw *journalWriter) fail_locked(err error) error {
	if err == nil {
		return nil
	}

	jw.j.logger.LogAttrs(jw.j.context, slog.LevelError, "journal failed", slog.String("journal", jw.j.debugName), slog.Any("err", err))

	jw.finishWriting_locked(closeWithoutCommitting)

	if jw.writeErr == nil {
		jw.writeErr = err
	}
	jw.j.setLastRecordUnknown()
	jw.j.notifyReaders()
	return err
}

func (jw *journalWriter) fsyncFailed_locked(err error) {
	// TODO: enter a TOTALLY FAILED mode that's preserved across restarts
	// (e.g. by creating a sentinel file)
	jw.fail_locked(err)
}

func (jw *journalWriter) prepareToWrite_locked() error {
	if err := jw.j.validateMetadataOptions(); err != nil {
		return err
	}
	var failed Segment
	for {
		err := jw.prepareToWrite_locked_once(&failed)
		if err == errFileGone {
			jw.j.resetState()
			continue
		}
		return err
	}
}

func (jw *journalWriter) prepareToWrite_locked_once(failed *Segment) error {
	last, err := jw.j.lastSegment()
	if err != nil {
		return err
	}
	if jw.j.verbose {
		jw.j.logger.Debug("journal last file", "journal", jw.j.debugName, "file", last)
	}
	if last.IsZero() {
		jw.nextSegNum = 1
		jw.nextRecNum = 1
		lastRec := Meta{ID: 0, Timestamp: 0}
		jw.j.setLastRecord(lastRec, lastRec)
		return nil
	}
	if last == *failed {
		return fmt.Errorf("journal failed twice to continue with segment file %v", last)
	}

	if last.status.IsDraft() {
		sw, err := continueSegment(jw.j, last)
		if err != nil {
			*failed = last
			return err
		}
		jw.segWriter = sw
		lastRec := sw.lastMeta()
		jw.j.setLastRecord(lastRec, lastRec)
	} else {
		var h segmentHeader
		err := loadSegmentHeader(jw.j, &h, last)
		if err != nil {
			*failed = last
			return err
		}
		if jw.j.verbose {
			jw.j.logger.Debug("journal last segment is finalized", "journal", jw.j.debugName, "seg", last, "last_ts", h.LastTimestamp, "last_rec", h.LastRecordNumber)
		}

		jw.nextSegNum = last.segnum + 1
		jw.nextRecNum = h.LastRecordNumber + 1

		lastRec := Meta{ID: h.LastRecordNumber, Timestamp: h.LastTimestamp}
		jw.j.setLastRecord(lastRec, lastRec)
	}
	return nil
}

func (jw *journalWriter) ensurePreparedToWrite_locked() error {

	if jw.writeErr != nil {
		return jw.writeErr
	}
	if jw.writable {
		return nil
	}
	err := jw.prepareToWrite_locked()
	if err != nil {
		return jw.fail_locked(err)
	}
	jw.writable = true
	return nil
}

func (jw *journalWriter) finishWriting_locked(mode closeMode) error {
	jw.writable = false
	var err error
	if jw.segWriter != nil {
		err = jw.close_locked(mode)
	}
	return err
}

func (jw *journalWriter) EnsurePreparedToWrite() error {
	jw.writeLock.Lock()
	defer jw.writeLock.Unlock()
	return jw.ensurePreparedToWrite_locked()
}

func (jw *journalWriter) WriteRecord(timestamp uint64, data []byte) error {
	if jw.j.maxRecordSize > 0 && int64(len(data)) > jw.j.maxRecordSize {
		return ErrRecordSize
	}
	if len(data) == 0 {
		return nil
	}
	var now uint64
	if timestamp == 0 {
		now = jw.j.Now()
		timestamp = now
	}

	jw.writeLock.Lock()
	defer jw.writeLock.Unlock()

	err := jw.ensurePreparedToWrite_locked()
	if err != nil {
		return err
	}

	rotate := jw.segWriter != nil && jw.segWriter.shouldRotate(len(data))
	var previous SegmentMetadata
	if jw.segWriter != nil && !rotate {
		previous = jw.segWriter.metadata
	}
	metadata, err := jw.j.foldMetadata(previous, data)
	if err != nil {
		return err
	}
	if rotate {
		if jw.j.verbose {
			jw.j.logger.Debug("journal rotating segment", "journal", jw.j.debugName, "segment", jw.segWriter.seg, "segment_size", jw.segWriter.size, "data_size", len(data))
		}
		err := jw.close_locked(closeAndFinalize)
		if err != nil {
			return err
		}
	}

	if jw.segWriter == nil {
		segnum := jw.nextSegNum
		recnum := jw.nextRecNum
		if jw.j.verbose {
			jw.j.logger.Debug("journal starting segment", "journal", jw.j.debugName, "segment", segnum, "record", recnum)
		}
		sw, err := startSegment(jw.j, segnum, timestamp, recnum)
		if err != nil {
			return jw.fail_locked(err)
		}
		jw.segWriter = sw
	}

	if !jw.segWriter.uncommitted {
		if now == 0 {
			now = jw.j.Now()
		}
		jw.segWriter.firstUncommittedWriteTS = now
	}

	jw.j.setLastUncommittedRecord(Meta{ID: jw.segWriter.nextRec, Timestamp: timestamp})

	return jw.fail_locked(jw.segWriter.writeRecord(timestamp, data, metadata))
}

func (jw *journalWriter) Autocommit(now uint64) (bool, error) {
	dur := jw.j.autocommit.Interval
	if dur == 0 {
		return false, nil
	}
	jw.writeLock.Lock()
	defer jw.writeLock.Unlock()
	if jw.segWriter == nil {
		return false, nil
	}
	if !jw.segWriter.uncommitted {
		return false, nil
	}
	elapsed := time.Duration(now-jw.segWriter.firstUncommittedWriteTS) * time.Millisecond
	if elapsed < dur {
		return false, nil
	}
	return true, jw.commit_locked()
}

func (jw *journalWriter) Commit() error {
	jw.writeLock.Lock()
	defer jw.writeLock.Unlock()
	return jw.commit_locked()
}

func (jw *journalWriter) commit_locked() error {
	if jw.writeErr != nil {
		return jw.writeErr
	}
	if jw.segWriter == nil {
		return nil
	}
	err := jw.segWriter.commit()
	if err == nil && jw.j.syncOnCommit {
		err = jw.segWriter.sync()
	}
	err = jw.fail_locked(err)
	if err != nil {
		jw.j.setLastRecordUnknown()
		return err
	}
	jw.handleCommit_locked(jw.segWriter.lastMeta())
	return nil
}

func (jw *journalWriter) handleCommit_locked(lastRec Meta) {
	jw.j.setLastRecord(lastRec, lastRec)
}

func (jw *journalWriter) close_locked(mode closeMode) error {
	lastMeta := jw.segWriter.lastMeta()

	if mode.shouldFinalize() {
		jw.nextSegNum = jw.segWriter.seg.segnum + 1
		jw.nextRecNum = jw.segWriter.nextRec
	}

	err := jw.segWriter.close(mode)
	if err != nil {
		jw.segWriter = nil
		jw.j.setLastRecordUnknown()
		var fsf *fsyncFailedError
		if errors.As(err, &fsf) {
			jw.fsyncFailed_locked(err)
		}
		return err
	}

	if mode.shouldCommit() {
		jw.handleCommit_locked(lastMeta)
	} else {
		jw.j.setLastRecordUnknown()
	}
	jw.segWriter = nil
	return err
}
