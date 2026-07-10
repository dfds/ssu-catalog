package reachability

import (
	"strconv"
	"strings"
)

// statusMatcher matches HTTP status codes against a parsed expectation. An
// expectation is one or more comma-separated terms, each a single code ("204"),
// an inclusive range ("200-299"), or an "Nxx" class shorthand ("2xx").
type statusMatcher struct {
	ranges []codeRange
}

type codeRange struct{ lo, hi int }

func (m statusMatcher) matches(code int) bool {
	for _, r := range m.ranges {
		if code >= r.lo && code <= r.hi {
			return true
		}
	}
	return false
}

// defaultExpect is the fallback expectation: a single 200.
const defaultExpect = "200"

func defaultMatcher() statusMatcher {
	return statusMatcher{ranges: []codeRange{{lo: 200, hi: 200}}}
}

// parseExpect parses an expectation string. It returns the matcher, the resolved
// expectation to surface (the trimmed input, or "200" on fallback), and whether
// the input parsed cleanly. An empty input resolves to the default silently
// (ok=true); a non-empty but unparseable input falls back to the default with
// ok=false so the caller can warn.
func parseExpect(raw string) (matcher statusMatcher, resolved string, ok bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultMatcher(), defaultExpect, true
	}
	var ranges []codeRange
	for _, term := range strings.Split(raw, ",") {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		r, termOK := parseTerm(term)
		if !termOK {
			return defaultMatcher(), defaultExpect, false
		}
		ranges = append(ranges, r)
	}
	if len(ranges) == 0 {
		return defaultMatcher(), defaultExpect, false
	}
	return statusMatcher{ranges: ranges}, raw, true
}

// parseTerm parses a single expectation term into an inclusive code range.
func parseTerm(term string) (codeRange, bool) {
	// Range: "200-299".
	if lo, hi, found := strings.Cut(term, "-"); found {
		l, lok := parseCode(lo)
		h, hok := parseCode(hi)
		if !lok || !hok || l > h {
			return codeRange{}, false
		}
		return codeRange{lo: l, hi: h}, true
	}
	// Class shorthand: "2xx" / "5XX".
	if len(term) == 3 && (strings.HasSuffix(term, "xx") || strings.HasSuffix(term, "XX")) {
		d := term[0]
		if d < '1' || d > '5' {
			return codeRange{}, false
		}
		base := int(d-'0') * 100
		return codeRange{lo: base, hi: base + 99}, true
	}
	// Single code: "204".
	c, cok := parseCode(term)
	if !cok {
		return codeRange{}, false
	}
	return codeRange{lo: c, hi: c}, true
}

// parseCode parses a trimmed HTTP status code (100–599).
func parseCode(s string) (int, bool) {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n < 100 || n > 599 {
		return 0, false
	}
	return n, true
}
