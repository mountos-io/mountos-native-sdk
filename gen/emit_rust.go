package main

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
	"unicode"
)

func init() {
	emitters = append(emitters, emitter{
		flag: "rust-out",
		desc: "write the generated Rust client stubs (ops_gen.rs) into this directory",
		file: "ops_gen.rs",
		emit: emitRust,
	})
}

// rustKeywords are reserved words that need a raw identifier. self, Self,
// super, and crate cannot be raw, so they get a trailing underscore, which is
// also what the protobuf code generator does for field names.
var rustKeywords = map[string]bool{
	"as": true, "async": true, "await": true, "break": true, "const": true, "continue": true,
	"dyn": true, "else": true, "enum": true, "extern": true, "false": true, "fn": true,
	"for": true, "if": true, "impl": true, "in": true, "let": true, "loop": true,
	"match": true, "mod": true, "move": true, "mut": true, "pub": true, "ref": true,
	"return": true, "static": true, "struct": true, "trait": true, "true": true, "type": true,
	"unsafe": true, "use": true, "where": true, "while": true,
	"abstract": true, "become": true, "box": true, "do": true, "final": true, "macro": true,
	"override": true, "priv": true, "try": true, "typeof": true, "unsized": true,
	"virtual": true, "yield": true,
}

var rustPathKeywords = map[string]bool{"self": true, "Self": true, "super": true, "crate": true}

// rustIdent returns a snake_case name as a valid Rust identifier.
func rustIdent(name string) string {
	switch {
	case rustPathKeywords[name]:
		return name + "_"
	case rustKeywords[name]:
		return "r#" + name
	}
	return name
}

// snakeCase turns RemoveAll into remove_all and ReadFS into read_fs.
func snakeCase(name string) string {
	rs := []rune(name)
	var b strings.Builder
	for i, r := range rs {
		if unicode.IsUpper(r) && i > 0 {
			prev := rs[i-1]
			nextLower := i+1 < len(rs) && unicode.IsLower(rs[i+1])
			if unicode.IsLower(prev) || unicode.IsDigit(prev) || (unicode.IsUpper(prev) && nextLower) {
				b.WriteByte('_')
			}
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}

// rustOwned maps a spec type to the owned Rust type of a stub return value.
func rustOwned(s *Spec, t string) string {
	t = strings.TrimSpace(t)
	if rest, ok := strings.CutPrefix(t, "repeated "); ok {
		return "Vec<" + rustOwned(s, rest) + ">"
	}
	switch t {
	case "bytes":
		return "Vec<u8>"
	case "string":
		return "String"
	case "bool":
		return "bool"
	case "int32":
		return "i32"
	case "int64":
		return "i64"
	case "uint32":
		return "u32"
	case "uint64":
		return "u64"
	}
	return t
}

// rustParam maps a spec type to the borrowed Rust type of a stub parameter.
func rustParam(s *Spec, t string) string {
	t = strings.TrimSpace(t)
	if rest, ok := strings.CutPrefix(t, "repeated "); ok {
		return "&[" + rustOwned(s, rest) + "]"
	}
	switch t {
	case "string":
		return "&str"
	case "bytes":
		return "&[u8]"
	}
	return rustOwned(s, t)
}

// rustParamExpr converts a parameter to the type of the request message field.
func rustParamExpr(s *Spec, name, t string) string {
	t = strings.TrimSpace(t)
	if rest, ok := strings.CutPrefix(t, "repeated "); ok {
		if _, isEnum := s.Enums[rest]; isEnum {
			return name + ".iter().map(|v| *v as i32).collect()"
		}
		return name + ".to_vec()"
	}
	if _, isEnum := s.Enums[t]; isEnum {
		return name + " as i32"
	}
	switch t {
	case "string":
		return name + ".into()"
	case "bytes":
		return name + ".to_vec()"
	}
	return name
}

// rustFieldType is the type the protobuf code generator gives a field, used by
// result structs: enums are i32 and messages are optional.
func rustFieldType(s *Spec, t string) string {
	t = strings.TrimSpace(t)
	if rest, ok := strings.CutPrefix(t, "repeated "); ok {
		return "Vec<" + rustFieldType(s, rest) + ">"
	}
	if _, ok := s.Enums[t]; ok {
		return "i32"
	}
	if _, ok := s.Messages[t]; ok {
		return "Option<" + t + ">"
	}
	return rustOwned(s, t)
}

// rustRustfmtArgs match rust/rustfmt.toml, so the generated file is stable
// whichever directory the generator runs from.
var rustRustfmtArgs = []string{"--edition", "2021", "--config", "tab_spaces=2,max_width=120"}

// emitRust renders the Rust source and formats it with rustfmt.
func emitRust(s *Spec, stubs []Stub) ([]byte, error) {
	src := renderRust(s, stubs)
	cmd := exec.Command("rustfmt", rustRustfmtArgs...)
	cmd.Stdin = bytes.NewReader(src)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return src, fmt.Errorf("rustfmt generated Rust (it must be on PATH): %w: %s", err, strings.TrimSpace(errb.String()))
	}
	return out.Bytes(), nil
}

// renderRust renders the method constants, result structs, and operation stubs
// before formatting. Custom and planned operations only get a constant.
func renderRust(s *Spec, stubs []Stub) []byte {
	var b bytes.Buffer
	scopes := map[string][]Stub{}
	needsError := false
	for _, st := range stubs {
		if !st.Emit {
			continue
		}
		scopes[st.Scope] = append(scopes[st.Scope], st)
		if rustNeedsError(s, st) {
			needsError = true
		}
	}

	b.WriteString("// Code generated by mountos-native-sdk gen from api/filesystem.yaml and api/client.yaml. DO NOT EDIT.\n\n")
	for _, sc := range []struct{ scope, mod, typ string }{{"client", "client", "Client"}, {"dir", "dir", "Dir"}, {"file", "file", "File"}} {
		if len(scopes[sc.scope]) > 0 {
			fmt.Fprintf(&b, "use crate::%s::%s;\n", sc.mod, sc.typ)
		}
	}
	if needsError {
		b.WriteString("use crate::error::{Error, Result};\n")
	} else {
		b.WriteString("use crate::error::Result;\n")
	}
	b.WriteString("use crate::wire::*;\n\n")

	b.WriteString("/// Method IDs from the API spec.\npub mod method {\n")
	for _, op := range s.Operations {
		fmt.Fprintf(&b, "pub const %s: u16 = %d;\n", strings.ToUpper(snakeCase(op.Name)), op.ID)
	}
	b.WriteString("}\n\n")

	for _, st := range stubs {
		if st.Emit && st.Result != "" {
			writeRustResult(&b, s, st)
		}
	}
	for _, sc := range []struct{ scope, typ string }{{"client", "Client"}, {"dir", "Dir"}, {"file", "File"}} {
		if len(scopes[sc.scope]) == 0 {
			continue
		}
		fmt.Fprintf(&b, "impl %s {\n", sc.typ)
		for _, st := range scopes[sc.scope] {
			writeRustStub(&b, s, st)
		}
		b.WriteString("}\n\n")
	}
	return b.Bytes()
}

// rustNeedsError reports whether a stub body names the Error type.
func rustNeedsError(s *Spec, st Stub) bool {
	if len(st.Returns) != 1 {
		return false
	}
	t := strings.TrimSpace(st.Returns[0].Type)
	if _, ok := s.Messages[t]; ok {
		return true
	}
	_, ok := s.Enums[t]
	return ok
}

func writeRustResult(b *bytes.Buffer, s *Spec, st Stub) {
	fmt.Fprintf(b, "/// Result of `%s`.\n#[derive(Clone, Debug, Default, PartialEq)]\n#[non_exhaustive]\npub struct %s {\n", snakeCase(st.Name), st.Result)
	for _, r := range st.Returns {
		fmt.Fprintf(b, "pub %s: %s,\n", rustIdent(r.Name), rustFieldType(s, r.Type))
	}
	b.WriteString("}\n\n")
}

func writeRustStub(b *bytes.Buffer, s *Spec, st Stub) {
	params := []string{"&self"}
	fields := make([]string, 0, len(st.Params)+1)
	if st.Scope != "client" {
		fields = append(fields, "handle: st.handle")
	}
	for _, p := range st.Params {
		id := rustIdent(p.Name)
		params = append(params, fmt.Sprintf("%s: %s", id, rustParam(s, p.Type)))
		if expr := rustParamExpr(s, id, p.Type); expr == id {
			fields = append(fields, id)
		} else {
			fields = append(fields, fmt.Sprintf("%s: %s", id, expr))
		}
	}
	req := "Empty {}"
	if st.ReqMsg != "Empty" {
		req = fmt.Sprintf("%s { %s }", st.ReqMsg, strings.Join(fields, ", "))
	}

	ret := "()"
	var tail string // statements after the call that turn the reply into the return value
	switch {
	case len(st.Returns) == 1:
		r := st.Returns[0]
		ret = rustOwned(s, r.Type)
		field := "r." + rustIdent(r.Name)
		t := strings.TrimSpace(r.Type)
		switch {
		case isMessage(s, t):
			tail = fmt.Sprintf("%s.ok_or_else(|| Error::Protocol(\"%s reply has no %s\".into()))", field, strings.ToLower(st.Op.Name), r.Name)
		case isEnum(s, t):
			tail = fmt.Sprintf("%s::try_from(%s).map_err(|_| Error::Protocol(\"%s reply has an unknown %s\".into()))", t, field, strings.ToLower(st.Op.Name), r.Name)
		default:
			tail = "Ok(" + field + ")"
		}
	case len(st.Returns) > 1:
		ret = st.Result
		parts := make([]string, 0, len(st.Returns))
		for _, r := range st.Returns {
			parts = append(parts, fmt.Sprintf("%s: r.%s", rustIdent(r.Name), rustIdent(r.Name)))
		}
		tail = fmt.Sprintf("Ok(%s { %s })", st.Result, strings.Join(parts, ", "))
	}

	fmt.Fprintf(b, "/// %s\n", st.Doc)
	if len(params) > 7 {
		b.WriteString("#[allow(clippy::too_many_arguments)]\n")
	}
	fmt.Fprintf(b, "pub fn %s(%s) -> Result<%s> {\n", rustIdent(snakeCase(st.Name)), strings.Join(params, ", "), ret)
	if st.Scope != "client" {
		b.WriteString("let st = self.state()?;\n")
	}
	if len(st.Returns) == 0 {
		fmt.Fprintf(b, "self.unary::<Empty>(method::%s, &%s).map(drop)\n}\n\n", strings.ToUpper(snakeCase(st.Op.Name)), req)
		return
	}
	fmt.Fprintf(b, "let r: %s = self.unary(method::%s, &%s)?;\n%s\n}\n\n", st.RespMsg, strings.ToUpper(snakeCase(st.Op.Name)), req, tail)
}

func isMessage(s *Spec, t string) bool { _, ok := s.Messages[t]; return ok }
func isEnum(s *Spec, t string) bool    { _, ok := s.Enums[t]; return ok }
