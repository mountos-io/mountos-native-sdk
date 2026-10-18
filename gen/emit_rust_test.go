package main

import (
	"os/exec"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const rustStubSpec = `
api: mountos.filesystem
version: 1
capabilities: []
enums:
  Kind: {values: {A: 0}}
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
  - name: RemoveAll
    id: 55
    kind: unary
    request: {path: {id: 1, type: string}, type: {id: 2, type: Kind}, data: {id: 3, type: bytes}}
    response: {removed: {id: 1, type: uint64}}
  - name: Two
    id: 30
    kind: unary
    request: {path: {id: 1, type: string}}
    response: {a: {id: 1, type: uint32}, b: {id: 2, type: Info}}
  - {name: Watch, id: 60, kind: server_stream, request: Empty, response: Empty}
`

func renderRustFor(t *testing.T) string {
	t.Helper()
	var s Spec
	if err := yaml.Unmarshal([]byte(rustStubSpec), &s); err != nil {
		t.Fatal(err)
	}
	stubs, errs := buildStubs(&s, parseClient(t, `
ops:
  Ping: {doc: "Pings."}
  Stat: {doc: "Stats."}
  Flush: {doc: "Flushes."}
  RemoveAll: {doc: "Removes all."}
  Two: {result: TwoInfo, doc: "Two."}
  Watch: {planned: true}
`))
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	return string(renderRust(&s, stubs))
}

func TestRenderRustShape(t *testing.T) {
	out := strings.Join(strings.Fields(renderRustFor(t)), " ")
	for _, want := range []string{
		"pub const STAT: u16 = 20;",
		"pub const REMOVE_ALL: u16 = 55;",
		"pub const WATCH: u16 = 60;",
		"impl Client {",
		"impl File {",
		"/// Stats. pub fn stat(&self, path: &str) -> Result<Info> {",
		"r.info.ok_or_else(|| Error::Protocol(\"stat reply has no info\".into()))",
		"pub fn ping(&self) -> Result<()> { self.unary::<Empty>(method::PING, &Empty {}).map(drop) }",
		"pub fn flush(&self) -> Result<()> { let st = self.state()?;",
		"&FlushRequest { handle: st.handle }",
		"pub fn remove_all(&self, path: &str, r#type: Kind, data: &[u8]) -> Result<u64> {",
		"RemoveAllRequest { path: path.into(), r#type: r#type as i32, data: data.to_vec() }",
		"Ok(r.removed)",
		"pub struct TwoInfo { pub a: u32, pub b: Option<Info>, }",
		"Ok(TwoInfo { a: r.a, b: r.b })",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("generated Rust missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "fn watch") {
		t.Fatal("planned operation got a stub")
	}
	if strings.Contains(out, "impl Dir") {
		t.Fatal("unused scope got an impl block")
	}
}

func TestRustIdentAndSnakeCase(t *testing.T) {
	for in, want := range map[string]string{"type": "r#type", "move": "r#move", "self": "self_", "path": "path"} {
		if got := rustIdent(in); got != want {
			t.Errorf("rustIdent(%q) = %q, want %q", in, got, want)
		}
	}
	for in, want := range map[string]string{"RemoveAll": "remove_all", "Stat": "stat", "ReadFS": "read_fs", "StatBatch": "stat_batch"} {
		if got := snakeCase(in); got != want {
			t.Errorf("snakeCase(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestEmitRustFormats(t *testing.T) {
	if _, err := exec.LookPath("rustfmt"); err != nil {
		t.Skip("rustfmt not on PATH")
	}
	var s Spec
	if err := yaml.Unmarshal([]byte(rustStubSpec), &s); err != nil {
		t.Fatal(err)
	}
	stubs, errs := buildStubs(&s, parseClient(t, "ops:\n  Ping: {doc: P}\n  Stat: {doc: S}\n  Flush: {doc: F}\n  RemoveAll: {doc: R}\n  Two: {result: TwoInfo, doc: T}\n  Watch: {planned: true}\n"))
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	src, err := emitRust(&s, stubs)
	if err != nil {
		t.Fatalf("emit: %v", err)
	}
	if !strings.Contains(string(src), "\n  pub const STAT: u16 = 20;") {
		t.Fatalf("output not formatted with 2-space indent:\n%s", src)
	}
}
