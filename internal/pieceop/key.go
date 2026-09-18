// Package pieceop knows the Greenfield piece-key layout:
//
//	s<oid>_s<i>[_v<n>]        primary segment
//	e<oid>_s<i>_p<r>[_v<n>]   secondary EC piece
//
// Prefix deletion relies on the underscore after <oid>; without it, s123_ would
// also match s1234_...
package pieceop

import (
	"strconv"
	"strings"
)

// Prefixes returns the two list prefixes for one object.
func Prefixes(oid uint64) [2]string {
	s := strconv.FormatUint(oid, 10)
	return [2]string{"s" + s + "_", "e" + s + "_"}
}

// ParseOID extracts the object id from a piece key. ok=false for anything
// that does not look like a piece key.
func ParseOID(key string) (oid uint64, ok bool) {
	if len(key) < 3 || (key[0] != 's' && key[0] != 'e') {
		return 0, false
	}
	rest := key[1:]
	i := strings.IndexByte(rest, '_')
	if i <= 0 {
		return 0, false
	}
	v, err := strconv.ParseUint(rest[:i], 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// KeepOnly drops any key whose parsed oid differs from want. This is the
// last guard against a prefix-boundary mistake; with a well-formed prefix it
// never drops anything.
func KeepOnly(keys []string, want uint64) (kept, dropped []string) {
	for _, k := range keys {
		if oid, ok := ParseOID(k); ok && oid == want {
			kept = append(kept, k)
		} else {
			dropped = append(dropped, k)
		}
	}
	return kept, dropped
}
