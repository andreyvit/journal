package journal

import (
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
)

const recordFlagCommit byte = 1

const recordFlagShift = 1

const (
	magicV1Draft  = uint64('J')<<0 | uint64('O')<<8 | uint64('U')<<16 | uint64('R')<<24 | uint64('N')<<32 | uint64('L')<<40 | uint64('A')<<48 | uint64('D')<<56
	magicV1Final  = uint64('J')<<0 | uint64('O')<<8 | uint64('U')<<16 | uint64('R')<<24 | uint64('N')<<32 | uint64('L')<<40 | uint64('A')<<48 | uint64('F')<<56
	magicV1Sealed = uint64('J')<<0 | uint64('O')<<8 | uint64('U')<<16 | uint64('R')<<24 | uint64('N')<<32 | uint64('L')<<40 | uint64('A')<<48 | uint64('S')<<56
)

type segmentHeader struct {
	Magic            uint64    // offset 0
	SegmentOrdinal   uint32    // offset 8
	_                uint32    // offset 12
	Timestamp        uint64    // offset 16
	RecordOrdinal    uint64    // offset 24
	JournalInvariant [32]byte  // offset 32
	SegmentInvariant [32]byte  // offset 64
	_                [3]uint64 // offset 96
	HeaderChecksum   uint64    // offset 120
} // size 128

const segmentHeaderSize = 128

const maxRecHeaderLen = binary.MaxVarintLen64 /* sizeAndFlag */ + binary.MaxVarintLen64 /* timestamp */

func fillSegmentHeader(buf []byte, j *Journal, magic uint64, seg uint32, ts uint64, rec uint64, hash *xxhash.Digest) {
	h := segmentHeader{
		Magic:            magic,
		SegmentOrdinal:   seg,
		Timestamp:        ts,
		RecordOrdinal:    rec,
		JournalInvariant: j.journalInvariant,
		SegmentInvariant: j.segmentInvariant,
	}

	n, err := binary.Encode(buf[:], binary.LittleEndian, h)
	if err != nil {
		panic(err)
	}
	if n != segmentHeaderSize {
		panic("internal size mismatch")
	}

	hash.Write(buf[8 : segmentHeaderSize-8])
	binary.LittleEndian.PutUint64(buf[segmentHeaderSize-8:], hash.Sum64())
	hash.Write(buf[segmentHeaderSize-8 : segmentHeaderSize])
}

func appendRecordHeader(b []byte, size int, tsDelta uint64) []byte {
	b = binary.AppendUvarint(b, uint64(size)<<recordFlagShift)
	b = binary.AppendUvarint(b, uint64(tsDelta))
	return b
}
