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
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/andreyvit/sealer"
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
	Autorotate       AutorotateOptions
	Autocommit       AutocommitOptions

	Context context.Context
	Logger  *slog.Logger
	Verbose bool

	OnChange func()

	SealKeys []*sealer.Key
	SealOpts sealer.SealOptions
}

type AutorotateOptions struct {
	Interval time.Duration
}

type AutocommitOptions struct {
	Interval time.Duration
}

const DefaultMaxFileSize = 10 * 1024 * 1024

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
	journalInvariant [32]byte
	segmentInvariant [32]byte
	onChange         func()
	autorotate       AutorotateOptions
	autocommit       AutocommitOptions
	sealKeys         []*sealer.Key
	sealOpts         sealer.SealOptions

	state    journalState
	writer   journalWriter
	sealLock sync.Mutex
	trimLock sync.Mutex
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
	j := &Journal{
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
		onChange:         o.OnChange,
		autorotate:       o.Autorotate,
		autocommit:       o.Autocommit,
		sealKeys:         o.SealKeys,
		sealOpts:         o.SealOpts,
	}
	j.writer.j = j
	return j
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

func (j *Journal) Initialize() error {
	j.state.lock.Lock()
	defer j.state.lock.Unlock()
	return j.state.ensureInitialized(j)
}

// Summary returns the general information about the journal, including the
// last committed record. If necessary, it opens, verifies and repairs the
// journal in the process.
func (j *Journal) Summary() (Summary, error) {
	s, ok, err := j.immediateSummary()
	if ok || err != nil {
		return s, err
	}
	err = j.writer.EnsurePreparedToWrite()
	if err != nil {
		s, _, _ = j.immediateSummary()
		return s, err
	}
	s, _, err = j.immediateSummary()
	return s, err
}

// QuickSummary returns the general information about the journal, without
// actually reading the journal data. So if the journal hasn't been opened
// yet, last committed record information will not be available.
func (j *Journal) QuickSummary() (Summary, error) {
	s, _, err := j.immediateSummary()
	return s, err
}

func (j *Journal) immediateSummary() (Summary, bool, error) {
	j.state.lock.Lock()
	defer j.state.lock.Unlock()
	if err := j.state.ensureInitialized(j); err != nil {
		return Summary{}, false, err
	}
	if j.state.lastKnown {
		return j.state.summary(), true, nil
	} else {
		return Summary{}, false, nil
	}
}

func (j *Journal) StartWriting() {
	j.writer.StartWriting()
}

func (j *Journal) FinishWriting() error {
	return j.writer.FinishWriting(closeAndContinueLater)
}

func (j *Journal) Rotate() error {
	err := j.writer.FinishWriting(closeAndFinalize)
	if err != nil {
		return err
	}

	last, err := j.lastSegment()
	if err != nil {
		return err
	}
	if last.IsZero() || !last.status.IsDraft() {
		return nil // nothing to do
	}

	j.writer.StartWriting()
	return j.writer.FinishWriting(closeAndFinalize)
}

func (j *Journal) Autorotate(now uint64) (bool, error) {
	needs, err := j.needsRotation(now)
	if err != nil {
		return false, err
	}
	if !needs {
		return false, nil
	}
	return true, j.Rotate()
}

func (j *Journal) WriteRecord(timestamp uint64, data []byte) error {
	return j.writer.WriteRecord(timestamp, data)
}

func (j *Journal) Autocommit(now uint64) (bool, error) {
	return j.writer.Autocommit(now)
}

func (j *Journal) Commit() error {
	return j.writer.Commit()
}

func (j *Journal) filePath(name string) string {
	return filepath.Join(j.dir, name)
}

func (j *Journal) removeFile(seg Segment) error {
	if j.verbose {
		j.logger.Debug("journal deleting segment", "journal", j.debugName, "seg", seg)
	}
	name := seg.fileName(j)
	return os.Remove(j.filePath(name))
}

func (j *Journal) openFile(seg Segment, writable bool) (*os.File, error) {
	name := seg.fileName(j)
	if writable {
		return os.OpenFile(j.filePath(name), os.O_RDWR|os.O_CREATE, 0o666)
	} else {
		return os.Open(j.filePath(name))
	}
}
