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
	dataHash      xxhash.Digest
	seg           Segment
	rec           uint64
	ts            uint64
	size          int64
	recordsInSeg  int
	committedRec  uint64
	committedTS   uint64
	committedSize int64
	data          []byte
}

func verifySegment(j *Journal, f *os.File, seg Segment) (*segmentReader, error) {
	sr, err := newSegmentReader(j, f, seg)
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

func openSegment(j *Journal, seg Segment) (*os.File, *segmentReader, error) {
	f, err := j.openFile(seg, false)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, errFileGone
		}
		return nil, nil, err
	}

	var ok bool
	defer closeUnlessOK(f, &ok)

	r, err := newSegmentReader(j, f, seg)
	if err != nil {
		return nil, nil, err
	}

	ok = true
	return f, r, nil
}

func loadSegmentHeader(j *Journal, h *segmentHeader, seg Segment) error {
	f, err := j.openFile(seg, false)
	if err != nil {
		if os.IsNotExist(err) {
			return errFileGone
		}
		return err
	}
	defer f.Close()

	return readSegmentHeader(j, f, h, seg)
}

func newSegmentReader(j *Journal, f *os.File, seg Segment) (*segmentReader, error) {
	sr := &segmentReader{
		j:             j,
		f:             f,
		r:             bufio.NewReader(f),
		seg:           seg,
		rec:           seg.recnum - 1,
		ts:            seg.ts,
		size:          0,
		committedRec:  0,
		committedTS:   0,
		committedSize: 0,
	}
	sr.dataHash.Reset()

	var h segmentHeader
	err := readSegmentHeader(j, f, &h, seg)
	if err != nil {
		return sr, err
	}
	sr.size = int64(segmentHeaderSize)
	sr.committedSize = int64(segmentHeaderSize)
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
			expected := sr.dataHash.Sum64() | uint64(recordFlagCommit)
			sr.dataHash.Write(b[:])
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
			sr.dataHash.Write(b[:n])
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
			sr.dataHash.Write(sr.data)

			sr.recordsInSeg++
			sr.rec++
			sr.ts += tsdelta
			sr.size += int64(n + dataSize)

			if sr.j.verbose {
				sr.j.logger.Debug("record decoded", "journal", sr.j.debugName, "data", string(sr.data), "hash", fmt.Sprintf("%08x", sr.dataHash.Sum64()))
			}

			return nil
		}
	}
}

func readSegmentHeader(j *Journal, r io.Reader, h *segmentHeader, seg Segment) error {
	var buf [segmentHeaderSize]byte
	_, err := io.ReadFull(r, buf[:])
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

	var hash xxhash.Digest
	hash.Reset()
	hash.Write(buf[:segmentHeaderSize-8])
	checksum := hash.Sum64()

	if h.Magic != magicV1Draft && h.Magic != magicV1Sealed && h.Magic != magicV1Finalized {
		if j.verbose {
			j.logger.Debug("incompatible header: version", "journal", j.debugName)
		}
		return ErrUnsupportedVersion
	}
	if seg.status.IsSealed() {
		if h.Magic != magicV1Sealed {
			if j.verbose {
				j.logger.Debug("wrong header magic: unsealed format in a sealed file", "journal", j.debugName)
			}
			return errCorruptedFile
		}
	} else if seg.status.IsDraft() {
		// allow finalized magic because we could have crashed while updating
		// the magic
		if h.Magic != magicV1Draft && h.Magic != magicV1Finalized {
			if j.verbose {
				j.logger.Debug("wrong header magic: sealed format in a draft file", "journal", j.debugName)
			}
			return errCorruptedFile
		}
	} else if seg.status == Finalized {
		if h.Magic != magicV1Finalized {
			if j.verbose {
				j.logger.Debug("wrong header magic: non-finalized format in a finalized file", "journal", j.debugName)
			}
			return errCorruptedFile
		}
	}
	if checksum != h.HeaderChecksum {
		if j.verbose {
			j.logger.Debug("corrupted header: checksum", "journal", j.debugName, "actual", fmt.Sprintf("%08x", h.HeaderChecksum), "expected", fmt.Sprintf("%08x", checksum))
		}
		return errCorruptedFile
	}
	if seg.segnum != h.SegmentNumber {
		if j.verbose {
			j.logger.Debug("corrupted header: segment ordinal", "journal", j.debugName)
		}
		return errCorruptedFile
	}
	if seg.ts != h.FirstTimestamp {
		if j.verbose {
			j.logger.Debug("corrupted header: timestamp", "journal", j.debugName)
		}
		return errCorruptedFile
	}
	if seg.recnum != h.FirstRecordNumber {
		if j.verbose {
			j.logger.Debug("corrupted header: record ordinal", "journal", j.debugName)
		}
		return errCorruptedFile
	}
	if h.JournalInvariant != j.journalInvariant {
		if j.verbose {
			j.logger.Debug("incompatible header: journal invariant", "journal", j.debugName)
		}
		return ErrIncompatible
	}

	return nil
}
