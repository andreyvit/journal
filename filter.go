package journal

type Filter struct {
	MinRecordID  uint64
	MinTimestamp uint64
	MaxRecordID  uint64
	MaxTimestamp uint64
	Limit        int
	Latest       bool
}

// type collectSegmentsSession struct {
// 	prev    Segment
// 	matched []Segment
// }

// func (css *collectSegmentsSession) Add(segment Segment) {
// 	if segment.RecordID < css.MinRecordID || segment.Timestamp < css.MinTimestamp {
// 		return
// 	}
// 	if segment.RecordID > css.MaxRecordID || segment.Timestamp > css.MaxTimestamp {
// 		return
// 	}
// 	css.matched = append(css.matched, segment)
// }
