package pieceop

import "testing"

func TestPrefixes(t *testing.T) {
	p := Prefixes(42268)
	if p[0] != "s42268_" || p[1] != "e42268_" {
		t.Fatalf("got %v", p)
	}
}

func TestParseOID(t *testing.T) {
	cases := map[string]struct {
		oid uint64
		ok  bool
	}{
		"s42268_s0":     {42268, true},
		"s42268_s0_v2":  {42268, true},
		"e42268_s61_p5": {42268, true},
		"s422680_s0":    {422680, true}, // a different object, parsed correctly
		"s4226_s0":      {4226, true},
		"s_s0":          {0, false},
		"x42268_s0":     {0, false},
		"42268_s0":      {0, false},
		"s42268":        {0, false},
		"":              {0, false},
		"sabc_s0":       {0, false},
	}
	for k, want := range cases {
		got, ok := ParseOID(k)
		if ok != want.ok || got != want.oid {
			t.Errorf("%q: got (%d,%v) want (%d,%v)", k, got, ok, want.oid, want.ok)
		}
	}
}

func TestKeepOnly(t *testing.T) {
	kept, dropped := KeepOnly([]string{"s42268_s0", "s422680_s0", "e42268_s0_p3", "junk"}, 42268)
	if len(kept) != 2 || len(dropped) != 2 {
		t.Fatalf("kept=%v dropped=%v", kept, dropped)
	}
}
