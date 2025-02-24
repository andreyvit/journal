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
	nextSegNum uint32
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
		jw.fail(jw.prepareToWrite_locked())
	}()
}

func (jw *journalWriter) FinishWriting(mode closeMode) error {
	jw.writeLock.Lock()
	defer jw.writeLock.Unlock()
	return jw.finishWriting_locked(mode)
}

func (jw *journalWriter) fail(err error) error {
	if err == nil {
		return nil
	}

	jw.j.logger.LogAttrs(jw.j.context, slog.LevelError, "journal: failed", slog.String("journal", jw.j.debugName), slog.Any("err", err))

	jw.finishWriting_locked(closeWithoutCommitting)

	if jw.writeErr != nil {
		jw.writeErr = err
	}
	return err
}

func (jw *journalWriter) fsyncFailed(err error) {
	// TODO: enter a TOTALLY FAILED mode that's preserved across restarts
	// (e.g. by creating a sentinel file)
	jw.fail(err)
}

func (jw *journalWriter) prepareToWrite_locked() error {
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
		return nil
	}
	if last == *failed {
		return fmt.Errorf("journal: failed twice to continue with segment file %v", last)
	}

	if last.status.IsDraft() {
		sw, err := continueSegment(jw.j, last)
		if err != nil {
			*failed = last
			return err
		}
		jw.segWriter = sw
	} else {
		var h segmentHeader
		err := loadSegmentHeader(jw.j, &h, last)
		if err != nil {
			*failed = last
			return err
		}

		jw.nextSegNum = last.segnum + 1
		jw.nextRecNum = h.LastRecordNumber + 1
	}
	return nil
}

func (j *journalWriter) ensurePreparedToWrite_locked() error {
	if j.writeErr != nil {
		return j.writeErr
	}
	if j.writable {
		return nil
	}
	err := j.prepareToWrite_locked()
	if err != nil {
		return j.fail(err)
	}
	j.writable = true
	return nil
}

func (jw *journalWriter) finishWriting_locked(mode closeMode) error {
	jw.writable = false
	var err error
	if jw.segWriter != nil {
		err = jw.close_locked(mode)
		jw.segWriter = nil
	}
	return err
}

func (jw *journalWriter) WriteRecord(timestamp uint64, data []byte) error {
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
		return nil
	}

	if jw.segWriter != nil && jw.segWriter.shouldRotate(len(data)) {
		if jw.j.verbose {
			jw.j.logger.Debug("rotating segment", "journal", jw.j.debugName, "segment", jw.segWriter.seg, "segment_size", jw.segWriter.size, "data_size", len(data))
		}
		err := jw.close_locked(closeAndFinalize)
		if err != nil {
			return err
		}
		jw.segWriter = nil
	}

	if jw.segWriter == nil {
		segnum := jw.nextSegNum
		recnum := jw.nextRecNum
		if jw.j.verbose {
			jw.j.logger.Debug("starting segment", "journal", jw.j.debugName, "segment", segnum, "record", recnum)
		}
		sw, err := startSegment(jw.j, segnum, timestamp, recnum)
		if err != nil {
			return jw.fail(err)
		}
		jw.segWriter = sw
	}

	if !jw.segWriter.uncommitted {
		if now == 0 {
			now = jw.j.Now()
		}
		jw.segWriter.firstUncommittedWriteTS = now
	}

	return jw.fail(jw.segWriter.writeRecord(timestamp, data))
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
	return true, jw.fail(jw.segWriter.commit())
}

func (jw *journalWriter) Commit() error {
	if jw.segWriter == nil {
		return nil
	}
	return jw.fail(jw.segWriter.commit())
}

func (jw *journalWriter) close_locked(mode closeMode) error {
	if mode.shouldFinalize() {
		jw.nextSegNum = jw.segWriter.seg.segnum + 1
		jw.nextRecNum = jw.segWriter.nextRec
	}

	err := jw.segWriter.close(mode)
	if err != nil {
		var fsf *fsyncFailedError
		if errors.As(err, &fsf) {
			jw.fsyncFailed(err)
		}
	}
	return err
}
