package journal

type Summary struct {
	FirstSealedSegment   Segment
	LastSealedSegment    Segment
	FirstUnsealedSegment Segment
	LastUnsealedSegment  Segment
	SegmentCount         int
	LastCommitted        Meta
	LastRaw              Meta
}

func (s *Summary) FirstRecord() Meta {
	if s.FirstUnsealedSegment.IsZero() {
		return Meta{}
	}
	return s.FirstUnsealedSegment.FirstRecord()
}
