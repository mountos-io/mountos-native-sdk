package main

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// parseSpec builds a Spec from inline YAML so each case states its own shape.
func parseSpec(t *testing.T, src string) *Spec {
	t.Helper()
	var s Spec
	if err := yaml.Unmarshal([]byte(src), &s); err != nil {
		t.Fatalf("parse: %v", err)
	}
	return &s
}

const baseSpec = `
api: mountos.filesystem
version: 1
minorVersion: 0
capabilities: [bulk_io]
enums:
  Whence:
    values: {SET: 0, CUR: 1}
messages:
  Empty: {}
  Entry:
    name: {id: 1, type: string, required: true}
    whence: {id: 2, type: Whence}
operations:
  - name: Ping
    id: 2
    kind: unary
    request: Empty
    response: Empty
  - name: ListBulk
    id: 50
    kind: unary
    request:
      paths: {id: 1, type: "repeated string", required: true}
    response: Entry
    errors: [ENOENT, EIO]
    capability: bulk_io
errorCodes:
  - {code: 1001, name: UNSUPPORTED_VERSION}
`

func TestValidateAcceptsBaseSpec(t *testing.T) {
	if errs := validate(parseSpec(t, baseSpec)); len(errs) != 0 {
		t.Fatalf("base spec rejected: %v", errs)
	}
}

func TestValidateRejects(t *testing.T) {
	cases := map[string]struct {
		mutate func(string) string
		want   string
	}{
		"duplicate operation id": {
			mutate: func(s string) string { return strings.Replace(s, "id: 50", "id: 2", 1) },
			want:   "id 2 already used",
		},
		"duplicate field id": {
			mutate: func(s string) string { return strings.Replace(s, "whence: {id: 2", "whence: {id: 1", 1) },
			want:   "field id 1 used",
		},
		"unknown field type": {
			mutate: func(s string) string { return strings.Replace(s, "type: Whence}", "type: Missing}", 1) },
			want:   `unknown type "Missing"`,
		},
		"nested repeated": {
			mutate: func(s string) string {
				return strings.Replace(s, `"repeated string"`, `"repeated repeated string"`, 1)
			},
			want: "nested repeated",
		},
		"undeclared capability": {
			mutate: func(s string) string { return strings.Replace(s, "capability: bulk_io", "capability: nope", 1) },
			want:   `capability "nope" not declared`,
		},
		"unknown error name": {
			mutate: func(s string) string { return strings.Replace(s, "[ENOENT, EIO]", "[ENOENT, EBOGUS]", 1) },
			want:   `error "EBOGUS"`,
		},
		"unknown message reference": {
			mutate: func(s string) string { return strings.Replace(s, "response: Entry", "response: Ghost", 1) },
			want:   `unknown message "Ghost"`,
		},
		"duplicate enum value number": {
			mutate: func(s string) string { return strings.Replace(s, "CUR: 1", "CUR: 0", 1) },
			want:   "value 0 used by",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			errs := validate(parseSpec(t, tc.mutate(baseSpec)))
			joined := ""
			for _, e := range errs {
				joined += e.Error() + "\n"
			}
			if !strings.Contains(joined, tc.want) {
				t.Fatalf("want error containing %q, got:\n%s", tc.want, joined)
			}
		})
	}
}

func TestDiffClassifiesChanges(t *testing.T) {
	prev := snapshotOf(parseSpec(t, baseSpec))

	cases := map[string]struct {
		mutate   func(string) string
		breaking string
		additive string
	}{
		"renamed operation": {
			mutate:   func(s string) string { return strings.Replace(s, "name: Ping", "name: Pong", 1) },
			breaking: "renamed to Pong",
		},
		"new optional field": {
			mutate: func(s string) string {
				return strings.Replace(s, "whence: {id: 2", "extra: {id: 3, type: string}\n    whence: {id: 2", 1)
			},
			additive: "field extra (id 3) added",
		},
		"new required field": {
			mutate: func(s string) string {
				return strings.Replace(s, "whence: {id: 2", "extra: {id: 3, type: string, required: true}\n    whence: {id: 2", 1)
			},
			breaking: "new required field extra",
		},
		"retyped field": {
			mutate: func(s string) string {
				return strings.Replace(s, "name: {id: 1, type: string", "name: {id: 1, type: int64", 1)
			},
			breaking: "type string -> int64",
		},
		"renumbered enum value": {
			mutate:   func(s string) string { return strings.Replace(s, "CUR: 1", "CUR: 7", 1) },
			breaking: "renumbered 1 -> 7",
		},
		"new operation": {
			mutate: func(s string) string {
				op := "  - name: Extra\n    id: 61\n    kind: unary\n    request: Empty\n    response: Empty\n"
				return strings.Replace(s, "errorCodes:", op+"errorCodes:", 1)
			},
			additive: "operation Extra (id 61) added",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			next := snapshotOf(parseSpec(t, tc.mutate(baseSpec)))
			r := diffSnapshots(prev, next)
			if tc.breaking != "" && !anyContains(r.breaking, tc.breaking) {
				t.Fatalf("want breaking containing %q, got breaking=%v additive=%v", tc.breaking, r.breaking, r.additive)
			}
			if tc.additive != "" && !anyContains(r.additive, tc.additive) {
				t.Fatalf("want additive containing %q, got breaking=%v additive=%v", tc.additive, r.breaking, r.additive)
			}
		})
	}
}

func TestDiffIdenticalIsEmpty(t *testing.T) {
	snap := snapshotOf(parseSpec(t, baseSpec))
	r := diffSnapshots(snap, snap)
	if len(r.breaking) != 0 || len(r.additive) != 0 {
		t.Fatalf("identical snapshots differ: %+v", r)
	}
}

func anyContains(list []string, want string) bool {
	for _, s := range list {
		if strings.Contains(s, want) {
			return true
		}
	}
	return false
}

func TestDiffReportIsSortedAndStable(t *testing.T) {
	prev := &Snapshot{
		Messages: map[string]map[string]Field{
			"A": {"a": {ID: 1, Type: "string"}, "b": {ID: 2, Type: "string"}, "c": {ID: 3, Type: "string"}},
			"B": {"a": {ID: 1, Type: "string"}, "b": {ID: 2, Type: "string"}},
		},
		ErrorCodes: map[string]int{"X": 1, "Y": 2, "Z": 3},
	}
	next := &Snapshot{
		Messages: map[string]map[string]Field{
			"A": {"a": {ID: 1, Type: "int64"}},
			"B": {"a": {ID: 1, Type: "int64"}},
		},
		ErrorCodes: map[string]int{},
	}
	first := diffSnapshots(prev, next)
	if !sort.StringsAreSorted(first.breaking) || len(first.breaking) < 8 {
		t.Fatalf("breaking list is not sorted or short: %v", first.breaking)
	}
	for i := 0; i < 20; i++ {
		if got := diffSnapshots(prev, next); !reflect.DeepEqual(got, first) {
			t.Fatalf("report changed between runs:\n%v\n%v", first.breaking, got.breaking)
		}
	}
}

func TestDiffAddedOperationErrorIsAdditive(t *testing.T) {
	op := func(errs ...string) *Snapshot {
		return &Snapshot{Operations: map[string]SnapOp{"7": {Name: "Op", Kind: "unary", Errors: errs}}}
	}
	r := diffSnapshots(op("ENOENT"), op("ENOENT", "EIO"))
	if len(r.breaking) != 0 || !anyContains(r.additive, "now returns EIO") {
		t.Fatalf("breaking=%v additive=%v", r.breaking, r.additive)
	}
	r = diffSnapshots(op("ENOENT", "EIO"), op("ENOENT"))
	if !anyContains(r.breaking, "no longer returns EIO") {
		t.Fatalf("breaking=%v", r.breaking)
	}
}
