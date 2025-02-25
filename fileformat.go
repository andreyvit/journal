package journal

import (
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
)

const recordFlagCommit byte = 1

const recordFlagShift = 1

const (
	magicV1Draft     = uint64('J')<<0 | uint64('O')<<8 | uint64('U')<<16 | uint64('R')<<24 | uint64('N')<<32 | uint64('L')<<40 | uint64('A')<<48 | uint64('D')<<56
	magicV1Finalized = uint64('J')<<0 | uint64('O')<<8 | uint64('U')<<16 | uint64('R')<<24 | uint64('N')<<32 | uint64('L')<<40 | uint64('A')<<48 | uint64('F')<<56
	magicV1Sealed    = uint64('J')<<0 | uint64('O')<<8 | uint64('U')<<16 | uint64('R')<<24 | uint64('N')<<32 | uint64('L')<<40 | uint64('A')<<48 | uint64('S')<<56
)

type segmentHeader struct {
	Magic             uint64   // offset 0
	SegmentNumber     uint64   // offset 8
	FirstTimestamp    uint64   // offset 16
	FirstRecordNumber uint64   // offset 24
	LastTimestamp     uint64   // offset 32
	LastRecordNumber  uint64   // offset 40
	JournalInvariant  [32]byte // offset 48
	SegmentInvariant  [32]byte // offset 80
	UnsealedDataSize  uint64   // offset 112
	HeaderChecksum    uint64   // offset 120
} // size 128

const segmentHeaderSize = 128

const maxRecHeaderLen = binary.MaxVarintLen64 /* sizeAndFlag */ + binary.MaxVarintLen64 /* timestamp */

func fillSegmentHeader(buf []byte, j *Journal, magic uint64, segnum, firstTS, firstRecNum, lastTS, lastRecNum uint64) {
	h := segmentHeader{
		Magic:             magic,
		SegmentNumber:     segnum,
		FirstTimestamp:    firstTS,
		FirstRecordNumber: firstRecNum,
		LastTimestamp:     lastTS,
		LastRecordNumber:  lastRecNum,
		JournalInvariant:  j.journalInvariant,
		SegmentInvariant:  j.segmentInvariant,
	}

	n, err := binary.Encode(buf[:], binary.LittleEndian, h)
	if err != nil {
		panic(err)
	}
	if n != segmentHeaderSize {
		panic("internal size mismatch")
	}

	var hash xxhash.Digest
	hash.Reset()
	hash.Write(buf[:segmentHeaderSize-8])
	binary.LittleEndian.PutUint64(buf[segmentHeaderSize-8:], hash.Sum64())
}

func appendRecordHeader(b []byte, size int, tsDelta uint64) []byte {
	b = binary.AppendUvarint(b, uint64(size)<<recordFlagShift)
	b = binary.AppendUvarint(b, uint64(tsDelta))
	return b
}
