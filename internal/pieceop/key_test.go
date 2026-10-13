package pieceop

import "testing"

func TestPrefixes(t *testing.T) {
	p := Prefixes(42268)
	if p[0] != "s42268_" || p[1] != "e42268_" {
		t.Fatalf("got %v", p)
	}
}

func TestParseAcceptsSPLayouts(t *testing.T) {
	cases := map[string]Piece{
		"s42268_s0":        {OID: 42268, Segment: 0},
		"s42268_s7_v2":     {OID: 42268, Segment: 7, Version: 2},
		"e42268_s61_p5":    {OID: 42268, Segment: 61, Secondary: true, Redundancy: 5},
		"e42268_s0_p0_v3":  {OID: 42268, Segment: 0, Secondary: true, Redundancy: 0, Version: 3},
		"s422680_s0":       {OID: 422680}, // a different object, parsed correctly
		"s0_s0":            {OID: 0},
		"e1_s2047_p5_v100": {OID: 1, Segment: 2047, Secondary: true, Redundancy: 5, Version: 100},
	}
	for k, want := range cases {
		got, ok := Parse(k)
		if !ok || got != want {
			t.Errorf("%q: got (%+v,%v) want %+v", k, got, ok, want)
		}
		if ok && got.Key() != k {
			t.Errorf("%q: renders back as %q", k, got.Key())
		}
	}
}

// Anything the SP would not have written is rejected, never guessed at.
func TestParseRejectsUnknownLayouts(t *testing.T) {
	for _, k := range []string{
		"s123_backup",    // right oid prefix, unknown suffix
		"s123_s0_backup", // trailing text
		"s123_s0_v0",     // the SP never writes _v0
		"s0123_s0",       // leading zero in oid
		"s123_s00",       // leading zero in segment
		"e123_s0_p01",    // leading zero in redundancy index
		"s123_s0_p0",     // p-part on a primary key
		"e123_s0",        // EC key without p-part
		"e123_s0_v2",     // EC key with version but no p-part
		"s123_s0_v2_v3",  // two versions
		"s123_s-1",       // sign
		"s123_s+1",
		"s123_",
		"s123",
		"s_s0",
		"x123_s0",
		"123_s0",
		"sabc_s0",
		"",
	} {
		if p, ok := Parse(k); ok {
			t.Errorf("%q accepted as %+v", k, p)
		}
	}
}

func TestKeepOnly(t *testing.T) {
	kept, rejected := KeepOnly([]string{"s42268_s0", "s422680_s0", "e42268_s0_p3", "s42268_backup", "junk"}, 42268)
	if len(kept) != 2 || len(rejected) != 3 {
		t.Fatalf("kept=%v rejected=%v", kept, rejected)
	}
}
