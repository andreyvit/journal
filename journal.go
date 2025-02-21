// Package journal implements WAL-like append-only journals. A journal is split
// into segments; the last segment is the one being written to.
//
// Intended use cases:
//
//   - Database WAL files.
//   - Log files of various kinds.
//   - Archival of historical database records.
//
// Features:
//
//   - Suitable for a large number of very short records. Per-record overhead
//     can be as low as 2 bytes.
//
//   - Suitable for very large records, too. (In the future, it will be possible
//     to write records in chunks.)
//
//   - Fault-resistant.
//
//   - Self-healing. Verifies the checksums and truncates corrupted data when
//     opening the journal.
//
//   - Performant.
//
//   - Automatically rotates the files when they reach a certain size.
//
// TODO:
//
//   - Trigger rotation based on time (say, each day gets a new segment).
//     Basically limit how old in-progress segments can be.
//
//   - Allow to rotate a file without writing a new record. (Otherwise
//     rarely-used journals will never get archived.)
//
//   - Give work-in-progress file a prefixed name (W*).
//
//   - Auto-commit every N seconds, after K bytes, after M records.
//
//   - Option for millisecond timestamp precision?
//
//   - Reading API. (Search based on time and record ordinals.)
//
// # File format
//
// Segment files:
//
//   - file = segmentHeader item*
//   - segmentHeader = (see struct)
//   - item = record | commit
//   - record = (size << 1):uvarint timestampDelta:uvarint bytes*
//   - commit = checksum_with_bit_0_set:64
//
// We always set bit 0 of commit checksums, and we use size*2 when encoding
// records; so bit 0 of the first byte of an item indicates whether it's
// a record or a commit.
//
// Timestamps are 32-bit unix times and have 1 second precision. (Rationale
// is that the primary use of timestamps is to search logs by time, and that
// does not require a higher precision. For high-frequency logs, with 1-second
// precision, timestamp deltas will typically fit within 1 byte.)
package journal

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	ErrIncompatible       = fmt.Errorf("incompatible journal")
	ErrUnsupportedVersion = fmt.Errorf("unsupported journal version")
	errCorruptedFile      = fmt.Errorf("corrupted journal segment file")
	errFileGone           = fmt.Errorf("journal segment is gone")
)

type Options struct {
	FileName         string // e.g. "mydb-*.bin"
	MaxFileSize      int64  // new segment after this size
	DebugName        string
	Now              func() time.Time
	JournalInvariant [32]byte
	SegmentInvariant [32]byte

	Context context.Context
	Logger  *slog.Logger
	OnLoad  func()
	Verbose bool
}

const DefaultMaxFileSize = 4 * 1024 * 1024

type Journal struct {
	context          context.Context
	maxFileSize      int64
	fileNamePrefix   string
	fileNameSuffix   string
	debugName        string
	dir              string
	now              func() time.Time
	logger           *slog.Logger
	serialIDs        bool
	verbose          bool
	writable         bool
	journalInvariant [32]byte
	segmentInvariant [32]byte

	writeLock sync.Mutex
	writeErr  error
	segWriter *segmentWriter
}

func New(dir string, o Options) *Journal {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Context == nil {
		o.Context = context.Background()
	}
	if o.FileName == "" {
		o.FileName = "*"
	}
	prefix, suffix, _ := strings.Cut(o.FileName, "*")
	if o.DebugName == "" {
		o.DebugName = "journal"
	}
	if o.MaxFileSize == 0 {
		o.MaxFileSize = DefaultMaxFileSize
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return &Journal{
		context:          o.Context,
		maxFileSize:      o.MaxFileSize,
		fileNamePrefix:   prefix,
		fileNameSuffix:   suffix,
		debugName:        o.DebugName,
		dir:              dir,
		now:              o.Now,
		verbose:          o.Verbose,
		journalInvariant: o.JournalInvariant,
		segmentInvariant: o.SegmentInvariant,
		logger:           o.Logger,
	}
}

func (j *Journal) Now() uint64 {
	v := j.now().UnixMilli()
	if v < 0 {
		panic("time travel disallowed")
	}
	return uint64(v)
}

func (j *Journal) String() string {
	return j.debugName
}

func (j *Journal) StartWriting() {
	j.writeLock.Lock()
	if j.writable || j.writeErr != nil {
		j.writeLock.Unlock()
		return
	}
	j.writable = true

	go func() {
		defer j.writeLock.Unlock()
		j.fail(j.prepareToWrite_locked())
	}()
}

func (j *Journal) prepareToWrite_locked() error {
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

	var failedName string
retry:
	lastName := j.findLastFile(dirf)
	if j.verbose {
		j.logger.Debug("journal last file", "journal", j.debugName, "file", lastName)
	}
	if lastName == "" {
		return nil
	}
	if lastName == failedName {
		return fmt.Errorf("journal: failed twice to continue with segment file %s", lastName)
	}

	sw, err := continueSegment(j, lastName)
	if err == errFileGone {
		goto retry
	} else if err != nil {
		return err
	}
	j.segWriter = sw
	return nil
}

func (j *Journal) ensurePreparedToWrite_locked() error {
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

func (j *Journal) FinishWriting() error {
	j.writeLock.Lock()
	defer j.writeLock.Unlock()
	return j.finishWriting_locked()
}

func (j *Journal) finishWriting_locked() error {
	j.writable = false
	var err error
	if j.segWriter != nil {
		err = j.segWriter.close()
		j.segWriter = nil
	}
	return err
}

func (j *Journal) fail(err error) error {
	if err == nil {
		return nil
	}

	j.logger.LogAttrs(j.context, slog.LevelError, "journal: failed", slog.String("journal", j.debugName), slog.Any("err", err))

	j.finishWriting_locked()

	if j.writeErr != nil {
		j.writeErr = err
	}
	return err
}

func (j *Journal) fsyncFailed(err error) {
	// TODO: enter a TOTALLY FAILED mode that's preserved across restarts
	// (e.g. by creating a sentinel file)
	j.fail(err)
}

func (j *Journal) filePath(name string) string {
	return filepath.Join(j.dir, name)
}
func (j *Journal) openFile(name string, writable bool) (*os.File, error) {
	if writable {
		return os.OpenFile(j.filePath(name), os.O_RDWR|os.O_CREATE, 0o666)
	} else {
		return os.Open(j.filePath(name))
	}
}

func (j *Journal) findLastFile(dirf fs.ReadDirFile) string {
	var lastName string
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
			if name > lastName {
				lastName = name
			}
		}
	}
	return lastName
}

func (j *Journal) WriteRecord(timestamp uint64, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if timestamp == 0 {
		timestamp = j.Now()
	}

	j.writeLock.Lock()
	defer j.writeLock.Unlock()

	err := j.ensurePreparedToWrite_locked()
	if err != nil {
		return nil
	}

	var seg uint32
	var rec uint64
	var prevChecksum uint64
	if j.segWriter == nil {
		seg = 1
		rec = 1
		prevChecksum = 0
	} else if j.segWriter.shouldRotate(len(data)) {
		if j.verbose {
			j.logger.Debug("rotating segment", "journal", j.debugName, "segment", j.segWriter.seg, "segment_size", j.segWriter.size, "data_size", len(data))
		}
		seg = j.segWriter.seg + 1
		rec = j.segWriter.nextRec
		j.segWriter.close()
		prevChecksum = j.segWriter.checksum() // close might do a commit
		j.segWriter = nil
	}

	if j.segWriter == nil {
		if j.verbose {
			j.logger.Debug("starting segment", "journal", j.debugName, "segment", seg, "record", rec)
		}
		sw, err := startSegment(j, seg, timestamp, rec, prevChecksum)
		if err != nil {
			return j.fail(err)
		}
		j.segWriter = sw
	}

	return j.fail(j.segWriter.writeRecord(timestamp, data))
}

func (j *Journal) Commit() error {
	if j.segWriter == nil {
		return nil
	}
	return j.fail(j.segWriter.commit())
}
