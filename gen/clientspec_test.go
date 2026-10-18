package main

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func parseClient(t *testing.T, src string) *ClientSpec {
	t.Helper()
	var cs ClientSpec
	if err := yaml.Unmarshal([]byte(src), &cs); err != nil {
		t.Fatalf("parse: %v", err)
	}
	return &cs
}

const stubSpec = `
api: mountos.filesystem
version: 1
capabilities: []
enums: {}
messages:
  Empty: {}
  Info:
    size: {id: 1, type: int64}
operations:
  - {name: Ping, id: 2, kind: unary, request: Empty, response: Empty}
  - name: Stat
    id: 20
    kind: unary
    request: {path: {id: 1, type: string, required: true}}
    response: {info: {id: 1, type: Info, required: true}}
  - name: Flush
    id: 18
    kind: unary
    request: {handle: {id: 1, type: uint64, required: true}}
    response: Empty
  - name: Two
    id: 30
    kind: unary
    request: {path: {id: 1, type: string}}
    response: {a: {id: 1, type: uint32}, b: {id: 2, type: uint64}}
`

func buildFor(t *testing.T, client string) ([]Stub, []error) {
	t.Helper()
	var s Spec
	if err := yaml.Unmarshal([]byte(stubSpec), &s); err != nil {
		t.Fatal(err)
	}
	return buildStubs(&s, parseClient(t, client))
}

func TestStubsResolveScopesAndFields(t *testing.T) {
	stubs, errs := buildFor(t, `
ops:
  Ping: {doc: "Pings."}
  Stat: {doc: "Stats."}
  Flush: {doc: "Flushes."}
  Two: {result: TwoInfo, doc: "Two."}
`)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	byName := map[string]Stub{}
	for _, st := range stubs {
		byName[st.Op.Name] = st
	}
	if byName["Stat"].Scope != "client" || byName["Flush"].Scope != "file" {
		t.Fatalf("scopes wrong: Stat=%s Flush=%s", byName["Stat"].Scope, byName["Flush"].Scope)
	}
	if got := len(byName["Flush"].Params); got != 0 {
		t.Fatalf("handle leaked into params: %d params", got)
	}
	if byName["Stat"].ReqMsg != "StatRequest" || byName["Ping"].ReqMsg != "Empty" {
		t.Fatalf("message names wrong: %s / %s", byName["Stat"].ReqMsg, byName["Ping"].ReqMsg)
	}
}

func TestStubValidationErrors(t *testing.T) {
	cases := map[string]struct{ client, want string }{
		"missing entry":             {"ops:\n  Ping: {doc: x}\n", `operation "Stat" has no entry`},
		"unknown op":                {"ops:\n  Ping: {doc: x}\n  Stat: {doc: x}\n  Flush: {doc: x}\n  Two: {result: R, doc: x}\n  Ghost: {doc: x}\n", `"Ghost" is not in filesystem.yaml`},
		"no doc":                    {"ops:\n  Ping: {}\n  Stat: {doc: x}\n  Flush: {doc: x}\n  Two: {result: R, doc: x}\n", "Ping needs a doc line"},
		"needs result":              {"ops:\n  Ping: {doc: x}\n  Stat: {doc: x}\n  Flush: {doc: x}\n  Two: {doc: x}\n", "Two has 2 response fields and needs a result name"},
		"default for unknown param": {"ops:\n  Ping: {doc: x}\n  Stat: {defaults: {nope: 1}, doc: x}\n  Flush: {doc: x}\n  Two: {result: R, doc: x}\n", `default for "nope", which is not a parameter`},
		"bad scope":                 {"ops:\n  Ping: {doc: x}\n  Stat: {scope: file, doc: x}\n  Flush: {doc: x}\n  Two: {result: R, doc: x}\n", "has scope file but its request has no handle"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, errs := buildFor(t, tc.client)
			var joined strings.Builder
			for _, e := range errs {
				joined.WriteString(e.Error() + "\n")
			}
			if !strings.Contains(joined.String(), tc.want) {
				t.Fatalf("want %q in:\n%s", tc.want, joined.String())
			}
		})
	}
}

func TestEmitGoProducesCompilableShape(t *testing.T) {
	var s Spec
	if err := yaml.Unmarshal([]byte(stubSpec), &s); err != nil {
		t.Fatal(err)
	}
	stubs, errs := buildStubs(&s, parseClient(t, `
ops:
  Ping: {doc: "Pings."}
  Stat: {doc: "Stats."}
  Flush: {doc: "Flushes."}
  Two: {result: TwoInfo, doc: "Two."}
`))
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	src, err := emitGo(&s, stubs)
	if err != nil {
		t.Fatalf("emit: %v", err)
	}
	out := strings.Join(strings.Fields(string(src)), " ")
	for _, want := range []string{
		"mStat uint16 = 20",
		"func (cl *Client) Stat(path string) (*wire.Info, error)",
		"func (f *File) Flush() error",
		"type TwoInfo struct",
		"// Stat stats.",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("generated Go missing %q:\n%s", want, out)
		}
	}
}

func TestEmitGoRejectsMissingMessageResult(t *testing.T) {
	var s Spec
	if err := yaml.Unmarshal([]byte(stubSpec), &s); err != nil {
		t.Fatal(err)
	}
	stubs, errs := buildStubs(&s, parseClient(t, `
ops:
  Ping: {doc: "Pings."}
  Stat: {doc: "Stats."}
  Flush: {doc: "Flushes."}
  Two: {result: TwoInfo, doc: "Two."}
`))
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	src, err := emitGo(&s, stubs)
	if err != nil {
		t.Fatalf("emit: %v", err)
	}
	out := strings.Join(strings.Fields(string(src)), " ")
	for _, want := range []string{
		`if r.GetInfo() == nil { return nil, missingResult("Stat", "info") }`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("generated Go missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, `missingResult("Ping"`) || strings.Contains(out, `missingResult("Two"`) {
		t.Fatalf("check emitted for an op without a single message result:\n%s", out)
	}
}
