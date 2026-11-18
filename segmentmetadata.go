package journal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/cespare/xxhash/v2"
)

type SegmentMetadata [32]byte

var ErrSegmentMetadata = errors.New("journal: invalid segment metadata")

func (j *Journal) foldMetadata(previous SegmentMetadata, data []byte) (next SegmentMetadata, err error) {
	if j.updateSegmentMetadata == nil {
		return previous, nil
	}
	defer func() {
		if v := recover(); v != nil {
			if cause, ok := v.(error); ok {
				err = fmt.Errorf("journal metadata: %w", cause)
			} else {
				err = fmt.Errorf("journal metadata: %v", v)
			}
		}
	}()
	return j.updateSegmentMetadata(previous, data)
}

func (j *Journal) validateMetadataOptions() error {
	if j.updateSegmentMetadata != nil && j.segmentInvariant != ([32]byte{}) {
		return errors.New("journal: segment metadata and SegmentInvariant are mutually exclusive")
	}
	return nil
}

func putSegmentMetadata(header []byte, value SegmentMetadata) {
	copy(header[80:112], value[:])
	binary.LittleEndian.PutUint64(header[segmentHeaderSize-8:], xxhash.Sum64(header[:segmentHeaderSize-8]))
}

func (sr *segmentReader) validateMetadata() error {
	if sr.j.updateSegmentMetadata != nil && !sr.seg.status.IsDraft() && sr.h.SegmentInvariant != ([32]byte{}) && sr.h.SegmentInvariant != sr.committedMetadata {
		return ErrSegmentMetadata
	}
	return nil
}

func (j *Journal) writeHeader(file *os.File, header []byte) error {
	j.headerLock.Lock()
	defer j.headerLock.Unlock()
	_, err := file.WriteAt(header, 0)
	return err
}

func (j *Journal) ReadSegmentMetadata(segment Segment) (SegmentMetadata, error) {
	if j.updateSegmentMetadata == nil {
		return SegmentMetadata{}, ErrSegmentMetadata
	}
	if err := j.validateMetadataOptions(); err != nil {
		return SegmentMetadata{}, err
	}
	j.writer.writeLock.Lock()
	defer j.writer.writeLock.Unlock()
	if j.writer.writeErr != nil {
		return SegmentMetadata{}, j.writer.writeErr
	}
	if writer := j.writer.segWriter; writer != nil && writer.seg.segnum == segment.segnum {
		return writer.committedMetadata, nil
	}
	segments, err := j.FindSegments(Filter{MinRecordID: segment.recnum, MaxRecordID: segment.recnum})
	if err != nil {
		return SegmentMetadata{}, err
	}
	for _, current := range segments {
		if current.segnum == segment.segnum {
			return j.readSegmentMetadata(current)
		}
	}
	return SegmentMetadata{}, ErrTailGap
}

func (j *Journal) readSegmentMetadata(segment Segment) (SegmentMetadata, error) {
	var header segmentHeader
	if err := loadSegmentHeader(j, &header, segment); err != nil {
		return SegmentMetadata{}, err
	}
	if !segment.status.IsDraft() {
		r := header.SegmentInvariant
		if r != (SegmentMetadata{}) {
			return r, nil
		}
	}
	return j.scanSegmentMetadata(segment)
}

func (j *Journal) scanSegmentMetadata(segment Segment) (SegmentMetadata, error) {
	file, reader, err := openSegment(j, segment)
	if err != nil {
		return SegmentMetadata{}, err
	}
	defer file.Close()
	for {
		err = reader.next()
		if err == io.EOF {
			return reader.committedMetadata, nil
		}
		if err != nil {
			return reader.committedMetadata, err
		}
	}
}

func (j *Journal) EnsureSegmentMetadata(minRecordID uint64) error {
	if j.updateSegmentMetadata == nil {
		return nil
	}
	if err := j.validateMetadataOptions(); err != nil {
		return err
	}
	j.sealLock.Lock()
	defer j.sealLock.Unlock()
	j.writer.writeLock.Lock()
	defer j.writer.writeLock.Unlock()
	segments, err := j.FindSegments(Filter{MinRecordID: minRecordID})
	if err != nil {
		return err
	}
	for i, segment := range segments {
		if i+1 < len(segments) && segments[i+1].recnum <= minRecordID {
			continue
		}
		if segment.status.IsDraft() || segment.status.IsSealed() {
			continue
		}
		var header segmentHeader
		if err = loadSegmentHeader(j, &header, segment); err != nil {
			return err
		}
		r := header.SegmentInvariant
		if r != (SegmentMetadata{}) {
			continue
		}
		r, err = j.scanSegmentMetadata(segment)
		if err != nil {
			return err
		}
		if r == (SegmentMetadata{}) {
			continue
		}
		file, err := os.OpenFile(j.filePath(segment.fileName(j)), os.O_RDWR, 0)
		if err != nil {
			return err
		}
		var data [segmentHeaderSize]byte
		fillSegmentHeader(data[:], j, header.Magic, segment.segnum, header.FirstTimestamp, header.FirstRecordNumber, header.LastTimestamp, header.LastRecordNumber)
		putSegmentMetadata(data[:], r)
		err = j.writeHeader(file, data[:])
		if err == nil {
			err = j.syncFile(file)
		}
		err = errors.Join(err, file.Close())
		if err != nil {
			return err
		}
	}
	return nil
}
