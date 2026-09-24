package purge

import "fmt"

// SizeCheck is the per-object reconciliation between what this SP physically
// held (bytes the tool listed/deleted) and what the chain says the object is
// (payload_size from bsdb). It never changes whether an object counts as done:
// by the time it runs the bytes are already gone. It only flags anomalies.
type SizeCheck struct {
	Role     string // primary | secondary | none (nothing on this SP) | mixed (anomaly)
	Expected uint64 // valid only when Known
	Known    bool
}

func (c SizeCheck) String() string {
	if !c.Known {
		return "role=" + c.Role
	}
	return fmt.Sprintf("role=%s expected=%d", c.Role, c.Expected)
}

// ExpectedSecondaryBytes is what one secondary SP stores for an object: one EC
// data shard per segment, each shard ceil(segment/dataChunks) bytes, the last
// segment being the remainder. This reproduces SP output byte-for-byte.
func ExpectedSecondaryBytes(payload, segmentSize uint64, dataChunks uint32) uint64 {
	if segmentSize == 0 || dataChunks == 0 {
		return 0
	}
	k := uint64(dataChunks)
	var total uint64
	for payload > 0 {
		seg := segmentSize
		if payload < seg {
			seg = payload
		}
		total += (seg + k - 1) / k
		payload -= seg
	}
	return total
}

// checkSize infers this SP's role for the object from which prefix held data
// and derives the expected byte count. A primary holds the s<oid>_ segments
// (the full payload); a secondary holds e<oid>_ shards. Nothing under either
// prefix means the SP holds no data for it (e.g. an unsealed object, or a
// previous interrupted run already removed everything), so there is nothing
// to compare.
func (r *Runner) checkSize(res Result, payload uint64) SizeCheck {
	switch {
	case res.SKeys > 0 && res.EKeys > 0:
		return SizeCheck{Role: "mixed"}
	case res.SKeys > 0:
		return SizeCheck{Role: "primary", Expected: payload, Known: true}
	case res.EKeys > 0:
		if r.Opt.DataChunks == 0 || r.Opt.SegmentSize == 0 {
			return SizeCheck{Role: "secondary"}
		}
		return SizeCheck{Role: "secondary", Expected: ExpectedSecondaryBytes(payload, r.Opt.SegmentSize, r.Opt.DataChunks), Known: true}
	default:
		return SizeCheck{Role: "none"}
	}
}
