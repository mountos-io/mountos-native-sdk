package main

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func emitTSFor(t *testing.T, client string) string {
	t.Helper()
	var s Spec
	src := strings.NewReplacer("Info:", "FileInfo:", "type: Info", "type: FileInfo").Replace(stubSpec)
	if err := yaml.Unmarshal([]byte(src), &s); err != nil {
		t.Fatal(err)
	}
	stubs, errs := buildStubs(&s, parseClient(t, client))
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	out, err := emitTS(&s, stubs)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

const tsClient = `
ops:
  Ping: {doc: "Pings."}
  Stat: {doc: "Stats."}
  Flush: {doc: "Flushes."}
  Two: {result: TwoInfo, doc: "Two."}
`

func TestEmitTSMethodIDsAndHeader(t *testing.T) {
	out := emitTSFor(t, tsClient)
	for _, want := range []string{
		"DO NOT EDIT.",
		"export const Method = {",
		"  Ping: 2,",
		"  Stat: 20,",
		"} as const;",
		`import * as pb from "./gen/mountos_fs.js";`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestEmitTSStubShapes(t *testing.T) {
	out := emitTSFor(t, tsClient)
	for _, want := range []string{
		"export abstract class ClientOps {",
		"export abstract class FileOps {",
		"protected abstract readonly id: bigint;",
		"/** Pings. */\n  async ping(): Promise<void> {\n    await this.call(Method.Ping, Buffer.alloc(0));",
		"async stat(path: string): Promise<FileInfo> {",
		"return statInfo(r.info);",
		"enc(pb.FlushRequest, { handle: this.id })",
		"export interface TwoInfo {\n  a: number;\n  b: bigint;\n}",
		"async two(path: string): Promise<TwoInfo> {",
		"return { a: r.a, b: r.b };",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestEmitTSSkipsCustomAndPlanned(t *testing.T) {
	out := emitTSFor(t, `
ops:
  Ping: {custom: true}
  Stat: {planned: true}
  Flush: {custom: true}
  Two: {custom: true}
`)
	if strings.Contains(out, "abstract class") || strings.Contains(out, "async ") {
		t.Errorf("custom or planned operations produced stubs:\n%s", out)
	}
	if !strings.Contains(out, "  Two: 30,") {
		t.Errorf("method id missing for custom operation:\n%s", out)
	}
}

func TestTSTypeRules(t *testing.T) {
	var s Spec
	if got, _ := tsParamType(&s, StubField{Name: "size", Type: "int64"}); got != "number" {
		t.Errorf("size param: %s", got)
	}
	if got, _ := tsParamType(&s, StubField{Name: "mtime_ns", Type: "int64"}); got != "bigint" {
		t.Errorf("time param: %s", got)
	}
	if got, _ := tsReturnType(&s, StubField{Name: "removed", Type: "uint64"}, false); got != "number" {
		t.Errorf("lone count: %s", got)
	}
	if got, _ := tsReturnType(&s, StubField{Name: "blocks", Type: "uint64"}, true); got != "bigint" {
		t.Errorf("result field: %s", got)
	}
	if _, err := tsParamType(&s, StubField{Name: "x", Type: "Nope"}); err == nil {
		t.Error("unknown type accepted")
	}
}
