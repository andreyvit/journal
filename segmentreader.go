package journal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/cespare/xxhash/v2"
)

type segmentReader struct {
	j             *Journal
	f             *os.File
	r             *bufio.Reader
	hash          xxhash.Digest
	magic         uint64
	seg           uint32
	rec           uint64
	ts            uint64
	size          int64
	recordsInSeg  int
	committedRec  uint64
	committedTS   uint64
	committedSize int64
	data          []byte
	isFinal       bool
	isSealed      bool
}

func verifySegment(j *Journal, f *os.File, fileName string) (*segmentReader, error) {
	sr, err := newSegmentReader(j, f, fileName)
	if err != nil {
		return sr, err
	}

	for {
		err := sr.next()
		if err == io.EOF {
			return sr, nil
		} else if err != nil {
			return sr, err
		}
	}
}

func openSegment(j *Journal, fileName string) (*os.File, *segmentReader, error) {
	f, err := j.openFile(fileName, false)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, errFileGone
		}
		return nil, nil, err
	}

	var ok bool
	defer closeUnlessOK(f, &ok)

	r, err := newSegmentReader(j, f, fileName)
	if err != nil {
		return nil, nil, err
	}

	ok = true
	return f, r, nil
}

func newSegmentReader(j *Journal, f *os.File, fileName string) (*segmentReader, error) {
	seg, ts, rec, err := parseSegmentName(j.fileNamePrefix, j.fileNameSuffix, fileName)
	if err != nil {
		return nil, errCorruptedFile
	}

	sr := &segmentReader{
		j:             j,
		f:             f,
		r:             bufio.NewReader(f),
		seg:           seg,
		rec:           rec - 1,
		ts:            ts,
		size:          0,
		committedRec:  0,
		committedTS:   0,
		committedSize: 0,
	}
	sr.hash.Reset()

	var h segmentHeader
	err = sr.readHeader(&h)
	if err != nil {
		return sr, err
	}
	sr.size = int64(segmentHeaderSize)
	sr.committedSize = int64(segmentHeaderSize)
	sr.isFinal = (h.Magic == magicV1Final || h.Magic == magicV1Sealed)
	sr.isSealed = (h.Magic == magicV1Sealed)
	return sr, nil
}

func (sr *segmentReader) next() error {
	for {
		b, err := sr.r.Peek(maxRecHeaderLen)
		if err == io.EOF {
			if len(b) == 0 {
				// end of file; was there a commit?
				if sr.size == sr.committedSize {
					return io.EOF
				} else {
					if sr.j.verbose {
						sr.j.logger.Debug("corrupted record: end of file without a commit", "journal", sr.j.debugName)
					}
					return errCorruptedFile
				}
			}
		} else if err != nil {
			return err
		}
		if b[0]&recordFlagCommit != 0 {
			var b [8]byte
			_, err := io.ReadFull(sr.r, b[:])
			if err == io.ErrUnexpectedEOF {
				if sr.j.verbose {
					sr.j.logger.Debug("corrupted record: end of file in the middle of commit", "journal", sr.j.debugName)
				}
				return errCorruptedFile
			} else if err != nil {
				return err
			}
			actual := binary.LittleEndian.Uint64(b[:])
			expected := sr.hash.Sum64() | uint64(recordFlagCommit)
			sr.hash.Write(b[:])
			if actual != expected {
				if sr.j.verbose {
					sr.j.logger.Debug("corrupted record: commit checksum mismatch", "journal", sr.j.debugName, "actual", fmt.Sprintf("%08x", actual), "expected", fmt.Sprintf("%08x", expected))
				}
				return errCorruptedFile
			}

			sr.size += 8
			if sr.recordsInSeg > 0 {
				sr.committedRec = sr.rec
				sr.committedTS = sr.ts
				sr.committedSize = sr.size

				if sr.j.verbose {
					sr.j.logger.Debug("commit decoded", "journal", sr.j.debugName)
				}
			} else {
				if sr.j.verbose {
					sr.j.logger.Debug("corrupted record: commit without a prior record", "journal", sr.j.debugName)
				}
				return errCorruptedFile
			}
		} else {
			rawSize, n1 := binary.Uvarint(b)
			if n1 <= 0 {
				if sr.j.verbose {
					sr.j.logger.Debug("corrupted record: cannot decode size", "journal", sr.j.debugName)
				}
				return errCorruptedFile
			}
			dataSize := int(rawSize / 2)

			tsdelta, n2 := binary.Uvarint(b[n1:])
			if n2 <= 0 {
				if sr.j.verbose {
					sr.j.logger.Debug("corrupted record: cannot decode timestamp", "journal", sr.j.debugName)
				}
				return errCorruptedFile
			}

			// if sr.j.verbose {
			// 	sr.j.logger.Debug("hash before record", "journal", sr.j.debugName, "hash", fmt.Sprintf("%08x", sr.hash.Sum64()))
			// }

			n := n1 + n2
			sr.hash.Write(b[:n])
			sr.r.Discard(n)

			if cap(sr.data) < dataSize {
				sr.data = make([]byte, dataSize, allocSize(dataSize))
			} else {
				sr.data = sr.data[:dataSize]
			}

			_, err = io.ReadFull(sr.r, sr.data)
			if err == io.ErrUnexpectedEOF {
				if sr.j.verbose {
					sr.j.logger.Debug("corrupted record: EOF when reading record data", "journal", sr.j.debugName, "offset", fmt.Sprintf("%08x", sr.size+int64(n)), "size", dataSize)
				}
				return errCorruptedFile
			} else if err != nil {
				return err
			}
			sr.hash.Write(sr.data)

			sr.recordsInSeg++
			sr.rec++
			sr.ts += tsdelta
			sr.size += int64(n + dataSize)

			if sr.j.verbose {
				sr.j.logger.Debug("record decoded", "journal", sr.j.debugName, "data", string(sr.data), "hash", fmt.Sprintf("%08x", sr.hash.Sum64()))
			}

			return nil
		}
	}
}

func (sr *segmentReader) readHeader(h *segmentHeader) error {
	var buf [segmentHeaderSize]byte
	_, err := io.ReadFull(sr.r, buf[:])
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		return errCorruptedFile
	} else if err != nil {
		return err
	}
	n, err := binary.Decode(buf[:], binary.LittleEndian, h)
	if err != nil {
		panic(err)
	}
	if n != len(buf) {
		panic("internal size mismatch")
	}

	sr.hash.Write(buf[8 : segmentHeaderSize-8])
	checksum := sr.hash.Sum64()
	sr.hash.Write(buf[segmentHeaderSize-8 : segmentHeaderSize])

	if h.Magic != magicV1Draft && h.Magic != magicV1Final && h.Magic != magicV1Sealed {
		if sr.j.verbose {
			sr.j.logger.Debug("incompatible header: version", "journal", sr.j.debugName)
		}
		return ErrUnsupportedVersion
	}
	if checksum != h.HeaderChecksum {
		if sr.j.verbose {
			sr.j.logger.Debug("corrupted header: checksum", "journal", sr.j.debugName, "actual", fmt.Sprintf("%08x", h.HeaderChecksum), "expected", fmt.Sprintf("%08x", checksum))
		}
		return errCorruptedFile
	}
	if sr.seg != h.SegmentOrdinal {
		if sr.j.verbose {
			sr.j.logger.Debug("corrupted header: segment ordinal", "journal", sr.j.debugName)
		}
		return errCorruptedFile
	}
	if sr.ts != h.Timestamp {
		if sr.j.verbose {
			sr.j.logger.Debug("corrupted header: timestamp", "journal", sr.j.debugName)
		}
		return errCorruptedFile
	}
	if sr.rec+1 != h.RecordOrdinal {
		if sr.j.verbose {
			sr.j.logger.Debug("corrupted header: record ordinal", "journal", sr.j.debugName)
		}
		return errCorruptedFile
	}
	if h.JournalInvariant != sr.j.journalInvariant {
		if sr.j.verbose {
			sr.j.logger.Debug("incompatible header: journal invariant", "journal", sr.j.debugName)
		}
		return ErrIncompatible
	}

	return nil
}
