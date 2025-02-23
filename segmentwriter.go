package journal

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/cespare/xxhash/v2"
)

type segmentWriter struct {
	j           *Journal
	f           *os.File
	seg         uint32
	ts          uint64
	nextRec     uint64
	size        int64
	hash        xxhash.Digest
	uncommitted bool
	modified    bool
}

func startSegment(j *Journal, seg uint32, ts uint64, rec uint64) (*segmentWriter, error) {
	name := formatSegmentName(j.fileNamePrefix, j.fileNameSuffix, seg, ts, rec)

	f, err := j.openFile(name, true)
	if err != nil {
		return nil, err
	}

	var ok bool
	defer closeAndDeleteUnlessOK(f, &ok)

	sw := &segmentWriter{
		j:        j,
		f:        f,
		seg:      seg,
		ts:       ts,
		nextRec:  rec,
		size:     segmentHeaderSize,
		modified: true,
	}
	sw.hash.Reset()

	var hbuf [segmentHeaderSize]byte
	fillSegmentHeader(hbuf[:], j, magicV1Draft, seg, ts, rec, &sw.hash)

	_, err = f.Write(hbuf[:])
	if err != nil {
		return nil, err
	}

	ok = true
	return sw, nil
}

func continueSegment(j *Journal, fileName string) (*segmentWriter, error) {
	f, err := j.openFile(fileName, true)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errFileGone
		}
		return nil, err
	}
	var ok bool
	defer closeUnlessOK(f, &ok)

	sr, err := verifySegment(j, f, fileName)
	if err == errCorruptedFile {
		if sr == nil || sr.committedRec == 0 {
			j.logger.LogAttrs(j.context, slog.LevelWarn, "journal: deleting completely corrupted file", slog.String("journal", j.debugName), slog.String("file", fileName))
			err := os.Remove(j.filePath(fileName))
			if err != nil {
				return nil, fmt.Errorf("journal: failed to delete corrupted file: %w", err)
			}
			return nil, errFileGone
		} else {
			j.logger.LogAttrs(j.context, slog.LevelWarn, "journal: recovered corrupted file", slog.String("journal", j.debugName), slog.String("file", fileName), slog.Int("record", int(sr.committedRec)))
			err := f.Truncate(sr.committedSize)
			if err != nil {
				return nil, fmt.Errorf("journal: failed to truncate corrupted file: %w", err)
			}

			_, err = f.Seek(0, io.SeekStart)
			if err != nil {
				return nil, fmt.Errorf("fseek (before reverify): %w", err)
			}

			if sr.j.verbose {
				sr.j.logger.Debug("segment recovered", "journal", sr.j.debugName, "file", fileName)
			}

			sr, err = verifySegment(j, f, fileName)
			if err == errCorruptedFile {
				return nil, fmt.Errorf("journal: failured to recover corrupted file")
			} else if err != nil {
				return nil, err
			}
			if sr.size != sr.committedSize {
				panic("journal: unreachable")
			}
		}
	}

	ok = true
	return &segmentWriter{
		j:       j,
		f:       f,
		seg:     sr.seg,
		ts:      sr.ts,
		nextRec: sr.rec + 1,
		size:    sr.committedSize,
		hash:    sr.hash,
	}, nil
}

func (sw *segmentWriter) writeRecord(ts uint64, data []byte) error {
	var tsDelta uint64
	if ts > sw.ts {
		tsDelta = ts - sw.ts
		sw.ts = ts
	}

	var hbuf [maxRecHeaderLen]byte
	h := appendRecordHeader(hbuf[:0], len(data), tsDelta)

	// if sw.j.verbose {
	// 	sw.j.logger.Debug("hash before record", "journal", sw.j.debugName, "record", string(data), "hash", fmt.Sprintf("%08x", sw.hash.Sum64()))
	// }

	sw.hash.Write(h)
	_, err := sw.f.Write(h)
	if err != nil {
		return err
	}

	sw.hash.Write(data)
	_, err = sw.f.Write(data)
	if err != nil {
		return err
	}

	sw.uncommitted = true
	sw.modified = true
	sw.nextRec++
	sw.size += int64(len(h) + len(data))

	// if sw.j.verbose {
	// 	sw.j.logger.Debug("hash after record", "journal", sw.j.debugName, "record", string(data), "hash", fmt.Sprintf("%08x", sw.hash.Sum64()))
	// }

	return nil
}

func (sw *segmentWriter) commit() error {
	if !sw.uncommitted {
		return nil
	}
	sw.uncommitted = false
	sw.modified = true
	sw.size += 8

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], sw.hash.Sum64()|uint64(recordFlagCommit))

	sw.hash.Write(buf[:])
	_, err := sw.f.Write(buf[:])
	if err != nil {
		return err
	}

	return nil
}

func (sw *segmentWriter) finalizeAndClose() error {
	defer func() {
		sw.f.Close()
		sw.f = nil
	}()

	err := sw.commit()
	if err != nil {
		return err
	}

	_, err = sw.f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	var magic [8]byte
	binary.LittleEndian.PutUint64(magic[:], magicV1Final)
	_, err = sw.f.Write(magic[:])
	if err != nil {
		return err
	}

	if sw.modified {
		err := sw.f.Sync()
		if err != nil {
			sw.j.fsyncFailed(err)
			return err
		}
		sw.modified = false
	}

	return nil
}

func (sw *segmentWriter) close() error {
	if sw.f == nil {
		return nil
	}
	defer func() {
		sw.f.Close()
		sw.f = nil
	}()
	err := sw.commit()
	if err != nil {
		return err
	}
	if sw.modified {
		err := sw.f.Sync()
		if err != nil {
			sw.j.fsyncFailed(err)
			return err
		}
	}
	return nil
}

func (sw *segmentWriter) checksum() uint64 {
	return sw.hash.Sum64()
}

func (sw *segmentWriter) shouldRotate(size int) bool {
	return sw.size+int64(size) > sw.j.maxFileSize
}
