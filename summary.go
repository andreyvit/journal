package journal

type Summary struct {
	FirstSealedSegment   Segment
	LastSealedSegment    Segment
	FirstUnsealedSegment Segment
	LastUnsealedSegment  Segment
	SegmentCount         int
	FirstRecordNumber    uint64
	FirstTimestamp       uint64
	LastCommitted        Meta
	LastRaw              Meta
}
