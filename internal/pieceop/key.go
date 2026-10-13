// Package pieceop knows the Greenfield piece-key layout, exactly as the SP writes
// it (base/gfsppieceop SegmentPieceKey / ECPieceKey):
//
//	s<oid>_s<i>               primary segment
//	s<oid>_s<i>_v<n>          primary segment of object version n (n > 0)
//	e<oid>_s<i>_p<r>          secondary EC piece
//	e<oid>_s<i>_p<r>_v<n>     secondary EC piece of object version n (n > 0)
//
// Prefix deletion relies on the underscore after <oid>; without it, s123_ would
// also match s1234_...
package pieceop

import (
	"fmt"
	"strconv"
	"strings"
)

// Prefixes returns the two list prefixes for one object.
func Prefixes(oid uint64) [2]string {
	s := strconv.FormatUint(oid, 10)
	return [2]string{"s" + s + "_", "e" + s + "_"}
}

// Piece is one parsed piece key.
type Piece struct {
	OID        uint64
	Segment    uint32
	Secondary  bool   // e-key (EC piece) rather than s-key (primary segment)
	Redundancy uint32 // EC piece index; only for secondary
	Version    int64  // object version; 0 means no _v suffix
}

// Key renders the piece key the way the SP does.
func (p Piece) Key() string {
	var k string
	if p.Secondary {
		k = fmt.Sprintf("e%d_s%d_p%d", p.OID, p.Segment, p.Redundancy)
	} else {
		k = fmt.Sprintf("s%d_s%d", p.OID, p.Segment)
	}
	if p.Version != 0 {
		k += fmt.Sprintf("_v%d", p.Version)
	}
	return k
}

// Parse accepts only the four layouts above, in the SP's exact rendering: the
// parsed fields must render back to the same key. Anything else (s123_backup,
// leading zeros, _v0, a p-part on an s-key, trailing text) is ok=false.
func Parse(key string) (p Piece, ok bool) {
	parts := strings.Split(key, "_")
	if len(parts) < 2 || len(parts) > 4 || len(parts[0]) < 2 {
		return Piece{}, false
	}
	switch parts[0][0] {
	case 's':
	case 'e':
		p.Secondary = true
	default:
		return Piece{}, false
	}
	num := func(s string, tag byte, bits int) (uint64, bool) {
		if len(s) < 2 || s[0] != tag {
			return 0, false
		}
		v, err := strconv.ParseUint(s[1:], 10, bits)
		return v, err == nil
	}
	oid, ok := num(parts[0], parts[0][0], 64)
	if !ok {
		return Piece{}, false
	}
	p.OID = oid
	seg, ok := num(parts[1], 's', 32)
	if !ok {
		return Piece{}, false
	}
	p.Segment = uint32(seg)
	rest := parts[2:]
	if p.Secondary {
		if len(rest) == 0 {
			return Piece{}, false
		}
		r, ok := num(rest[0], 'p', 32)
		if !ok {
			return Piece{}, false
		}
		p.Redundancy = uint32(r)
		rest = rest[1:]
	}
	switch len(rest) {
	case 0:
	case 1:
		v, ok := num(rest[0], 'v', 63)
		if !ok {
			return Piece{}, false
		}
		p.Version = int64(v)
	default:
		return Piece{}, false
	}
	if p.Key() != key {
		return Piece{}, false
	}
	return p, true
}

// KeepOnly splits keys into well-formed pieces of object want and everything
// else: keys of another object and keys in a layout this tool does not know.
// Callers must not delete the second group.
func KeepOnly(keys []string, want uint64) (kept, rejected []string) {
	for _, k := range keys {
		if p, ok := Parse(k); ok && p.OID == want {
			kept = append(kept, k)
		} else {
			rejected = append(rejected, k)
		}
	}
	return kept, rejected
}
