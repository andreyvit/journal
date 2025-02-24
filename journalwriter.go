package journal

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
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

	writeLock sync.Mutex
	writable  bool
	writeErr  error
	segWriter *segmentWriter
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
		last, err := jw.prepareToWrite_locked_once(failed)
		if err == errFileGone {
			failed = last
			continue
		}
		return err
	}
}

func (jw *journalWriter) prepareToWrite_locked_once(failed Segment) (Segment, error) {
	dirf, err := os.Open(jw.j.dir)
	if err != nil {
		return Segment{}, err
	}
	defer dirf.Close()

	ds, err := dirf.Stat()
	if err != nil {
		return Segment{}, err
	}
	if !ds.IsDir() {
		return Segment{}, fmt.Errorf("%v: not a directory", jw.j.debugName)
	}

	last, err := jw.j.findLastSegment(dirf)
	if err != nil {
		return Segment{}, err
	}
	if jw.j.verbose {
		jw.j.logger.Debug("journal last file", "journal", jw.j.debugName, "file", last)
	}
	if last.IsZero() {
		return Segment{}, nil
	}
	if last == failed {
		return Segment{}, fmt.Errorf("journal: failed twice to continue with segment file %v", last)
	}

	sw, err := continueSegment(jw.j, last)
	if err != nil {
		return last, err
	}
	jw.segWriter = sw
	return last, nil
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
	if timestamp == 0 {
		timestamp = jw.j.Now()
	}

	jw.writeLock.Lock()
	defer jw.writeLock.Unlock()

	err := jw.ensurePreparedToWrite_locked()
	if err != nil {
		return nil
	}

	var segnum uint32
	var recnum uint64
	if jw.segWriter == nil {
		segnum = 1
		recnum = 1
	} else if jw.segWriter.shouldRotate(len(data)) {
		if jw.j.verbose {
			jw.j.logger.Debug("rotating segment", "journal", jw.j.debugName, "segment", jw.segWriter.seg, "segment_size", jw.segWriter.size, "data_size", len(data))
		}
		segnum = jw.segWriter.seg.segnum + 1
		recnum = jw.segWriter.nextRec
		err := jw.close_locked(closeAndFinalize)
		if err != nil {
			return err
		}
		jw.segWriter = nil
	}

	if jw.segWriter == nil {
		if jw.j.verbose {
			jw.j.logger.Debug("starting segment", "journal", jw.j.debugName, "segment", segnum, "record", recnum)
		}
		sw, err := startSegment(jw.j, segnum, timestamp, recnum)
		if err != nil {
			return jw.fail(err)
		}
		jw.segWriter = sw
	}

	return jw.fail(jw.segWriter.writeRecord(timestamp, data))
}

func (jw *journalWriter) Commit() error {
	if jw.segWriter == nil {
		return nil
	}
	return jw.fail(jw.segWriter.commit())
}

func (jw *journalWriter) close_locked(mode closeMode) error {
	err := jw.segWriter.close(mode)
	if err != nil {
		var fsf *fsyncFailedError
		if errors.As(err, &fsf) {
			jw.fsyncFailed(err)
		}
	}
	return err
}
