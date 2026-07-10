package reachability

import "testing"

func TestParseExpect(t *testing.T) {
	cases := []struct {
		name     string
		in       string
		resolved string
		ok       bool
		match    map[int]bool // code -> expected matches()
	}{
		{
			name: "empty defaults to 200", in: "", resolved: "200", ok: true,
			match: map[int]bool{200: true, 204: false, 500: false},
		},
		{
			name: "single code", in: "204", resolved: "204", ok: true,
			match: map[int]bool{204: true, 200: false},
		},
		{
			name: "range", in: "200-299", resolved: "200-299", ok: true,
			match: map[int]bool{200: true, 250: true, 299: true, 300: false, 199: false},
		},
		{
			name: "class shorthand", in: "2xx", resolved: "2xx", ok: true,
			match: map[int]bool{200: true, 299: true, 300: false},
		},
		{
			name: "uppercase class shorthand", in: "5XX", resolved: "5XX", ok: true,
			match: map[int]bool{500: true, 599: true, 499: false},
		},
		{
			name: "comma list", in: "200, 301 ,404", resolved: "200, 301 ,404", ok: true,
			match: map[int]bool{200: true, 301: true, 404: true, 302: false},
		},
		{
			name: "mixed comma list with range", in: "204,500-599", resolved: "204,500-599", ok: true,
			match: map[int]bool{204: true, 550: true, 200: false},
		},
		{
			name: "unparseable falls back to 200", in: "banana", resolved: "200", ok: false,
			match: map[int]bool{200: true, 204: false},
		},
		{
			name: "inverted range is invalid", in: "299-200", resolved: "200", ok: false,
			match: map[int]bool{200: true, 250: false},
		},
		{
			name: "out of range code is invalid", in: "999", resolved: "200", ok: false,
			match: map[int]bool{200: true},
		},
		{
			name: "partial unparseable list falls back", in: "200,nope", resolved: "200", ok: false,
			match: map[int]bool{200: true, 204: false},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, resolved, ok := parseExpect(tc.in)
			if ok != tc.ok {
				t.Errorf("ok = %v, want %v", ok, tc.ok)
			}
			if resolved != tc.resolved {
				t.Errorf("resolved = %q, want %q", resolved, tc.resolved)
			}
			for code, want := range tc.match {
				if got := m.matches(code); got != want {
					t.Errorf("matches(%d) = %v, want %v", code, got, want)
				}
			}
		})
	}
}
