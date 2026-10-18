package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

const (
	committedProto = "../api/proto/mountos_fs.proto"
	protoPackage   = "mountos.filesystem.v1"
)

var scalarDescriptorTypes = map[string]descriptorpb.FieldDescriptorProto_Type{
	"bool":   descriptorpb.FieldDescriptorProto_TYPE_BOOL,
	"int32":  descriptorpb.FieldDescriptorProto_TYPE_INT32,
	"int64":  descriptorpb.FieldDescriptorProto_TYPE_INT64,
	"uint32": descriptorpb.FieldDescriptorProto_TYPE_UINT32,
	"uint64": descriptorpb.FieldDescriptorProto_TYPE_UINT64,
	"string": descriptorpb.FieldDescriptorProto_TYPE_STRING,
	"bytes":  descriptorpb.FieldDescriptorProto_TYPE_BYTES,
}

// compileProto runs protoc on path and returns the descriptor of that file.
// It skips the test when protoc is not installed.
func compileProto(t *testing.T, path string) *descriptorpb.FileDescriptorProto {
	t.Helper()
	protoc, err := exec.LookPath("protoc")
	if err != nil {
		t.Skip("protoc not installed: install protoc to run the proto descriptor coherence checks")
	}
	out := filepath.Join(t.TempDir(), "fs.desc")
	cmd := exec.Command(protoc, "--descriptor_set_out="+out,
		"--proto_path="+filepath.Dir(path), path)
	if msg, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("protoc %s: %v\n%s", path, err, msg)
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var set descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(raw, &set); err != nil {
		t.Fatalf("parse descriptor set: %v", err)
	}
	if len(set.File) != 1 {
		t.Fatalf("descriptor set has %d files, want 1", len(set.File))
	}
	return set.File[0]
}

// expectedMessages derives the message set the proto must declare from the
// spec alone: named messages plus the inline request and response shapes.
func expectedMessages(s *Spec) map[string]map[string]Field {
	msgs := map[string]map[string]Field{}
	for name, fields := range s.Messages {
		msgs[name] = fields
	}
	for _, op := range s.Operations {
		if op.Request.Ref == "" {
			msgs[op.Name+"Request"] = op.Request.Fields
		}
		if op.Response.Ref == "" {
			msgs[op.Name+"Response"] = op.Response.Fields
		}
	}
	return msgs
}

// descriptorDiffs lists every way the compiled proto departs from the spec.
func descriptorDiffs(s *Spec, fd *descriptorpb.FileDescriptorProto) []string {
	var diffs []string
	add := func(format string, args ...any) { diffs = append(diffs, fmt.Sprintf(format, args...)) }

	if fd.GetSyntax() != "proto3" {
		add("syntax is %q, want proto3", fd.GetSyntax())
	}
	if fd.GetPackage() != protoPackage {
		add("package is %q, want %q", fd.GetPackage(), protoPackage)
	}
	if len(fd.Service) > 0 || len(fd.Extension) > 0 {
		add("proto declares services or extensions, the contract has none")
	}

	gotEnums := map[string]*descriptorpb.EnumDescriptorProto{}
	for _, e := range fd.EnumType {
		gotEnums[e.GetName()] = e
	}
	for name, want := range s.Enums {
		got, ok := gotEnums[name]
		if !ok {
			add("enum %s is missing from the proto", name)
			continue
		}
		prefix := snakeUpper(name) + "_"
		gotValues := map[string]int32{}
		for _, v := range got.Value {
			gotValues[v.GetName()] = v.GetNumber()
		}
		for vname, num := range want.Values {
			full := prefix + vname
			n, ok := gotValues[full]
			switch {
			case !ok:
				add("enum %s value %s is missing from the proto", name, full)
			case int(n) != num:
				add("enum %s value %s is %d in the proto, %d in the spec", name, full, n, num)
			}
			delete(gotValues, full)
		}
		for extra := range gotValues {
			add("enum %s has value %s that the spec lacks", name, extra)
		}
	}
	for name := range gotEnums {
		if _, ok := s.Enums[name]; !ok {
			add("proto has enum %s that the spec lacks", name)
		}
	}

	wantMsgs := expectedMessages(s)
	gotMsgs := map[string]*descriptorpb.DescriptorProto{}
	for _, m := range fd.MessageType {
		gotMsgs[m.GetName()] = m
	}
	for name, fields := range wantMsgs {
		got, ok := gotMsgs[name]
		if !ok {
			add("message %s is missing from the proto", name)
			continue
		}
		if len(got.OneofDecl) > 0 {
			add("message %s declares a oneof, the contract has none", name)
		}
		if len(got.NestedType) > 0 || len(got.EnumType) > 0 {
			add("message %s declares nested types, the contract has none", name)
		}
		gotFields := map[string]*descriptorpb.FieldDescriptorProto{}
		for _, f := range got.Field {
			gotFields[f.GetName()] = f
		}
		for fname, wf := range fields {
			gf, ok := gotFields[fname]
			if !ok {
				add("message %s field %s is missing from the proto", name, fname)
				continue
			}
			delete(gotFields, fname)
			if int(gf.GetNumber()) != wf.ID {
				add("message %s field %s is number %d in the proto, %d in the spec", name, fname, gf.GetNumber(), wf.ID)
			}
			diffs = append(diffs, fieldTypeDiffs(s, name, fname, wf, gf)...)
		}
		for extra := range gotFields {
			add("message %s has field %s that the spec lacks", name, extra)
		}
	}
	for name := range gotMsgs {
		if _, ok := wantMsgs[name]; !ok {
			add("proto has message %s that the spec lacks", name)
		}
	}
	sort.Strings(diffs)
	return diffs
}

func fieldTypeDiffs(s *Spec, msg, fname string, wf Field, gf *descriptorpb.FieldDescriptorProto) []string {
	var diffs []string
	where := fmt.Sprintf("message %s field %s", msg, fname)
	typ := strings.TrimSpace(wf.Type)
	repeated := false
	if rest, ok := strings.CutPrefix(typ, "repeated "); ok {
		repeated, typ = true, strings.TrimSpace(rest)
	}
	if gotRepeated := gf.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REPEATED; gotRepeated != repeated {
		diffs = append(diffs, fmt.Sprintf("%s repeated is %v in the proto, %v in the spec", where, gotRepeated, repeated))
	}
	if gf.OneofIndex != nil {
		diffs = append(diffs, where+" belongs to a oneof, the contract has none")
	}
	if gf.GetProto3Optional() {
		diffs = append(diffs, where+" is proto3 optional, the contract has none")
	}
	if st, ok := scalarDescriptorTypes[typ]; ok {
		if gf.GetType() != st {
			diffs = append(diffs, fmt.Sprintf("%s type is %s in the proto, %s in the spec", where, gf.GetType(), typ))
		}
		return diffs
	}
	wantKind := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	if _, ok := s.Enums[typ]; ok {
		wantKind = descriptorpb.FieldDescriptorProto_TYPE_ENUM
	}
	wantName := "." + protoPackage + "." + typ
	if gf.GetType() != wantKind || gf.GetTypeName() != wantName {
		diffs = append(diffs, fmt.Sprintf("%s type is %s %s in the proto, %s in the spec", where, gf.GetType(), gf.GetTypeName(), wantName))
	}
	return diffs
}

func loadCommittedSpec(t *testing.T) *Spec {
	t.Helper()
	s, err := loadSpec("../api/filesystem.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// The committed proto must compile to exactly the messages, field numbers,
// names, types, repeated flags and enum values the spec describes, even when
// the proto text happens to match.
func TestCommittedProtoDescriptorMatchesSpec(t *testing.T) {
	s := loadCommittedSpec(t)
	fd := compileProto(t, committedProto)
	if diffs := descriptorDiffs(s, fd); len(diffs) > 0 {
		t.Fatalf("proto and spec disagree, run make api-gen after fixing the spec:\n  %s", strings.Join(diffs, "\n  "))
	}
}

// The committed proto must be byte-identical to the generator output.
func TestCommittedProtoTextMatchesGenerator(t *testing.T) {
	s := loadCommittedSpec(t)
	want := genProto(s, protoPackage, "github.com/mountos-io/mountos-native-sdk/go/wire;wire")
	got, err := os.ReadFile(committedProto)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatal("api/proto/mountos_fs.proto differs from the generator output, run make api-gen")
	}
}

// Each mutation of the committed proto must be reported by the descriptor check.
func TestDescriptorCheckDetectsMutations(t *testing.T) {
	s := loadCommittedSpec(t)
	src, err := os.ReadFile(committedProto)
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	mutations := map[string]struct {
		from, to string
		want     string
	}{
		"field number":    {"  string path = 1;\n  uint32 mode = 2;\n}\n\nmessage BatchItemError", "  string path = 9;\n  uint32 mode = 2;\n}\n\nmessage BatchItemError", "path is number 9"},
		"field name":      {"  string path = 1;\n  uint32 mode = 2;\n}\n\nmessage BatchItemError", "  string file = 1;\n  uint32 mode = 2;\n}\n\nmessage BatchItemError", "field path is missing"},
		"field type":      {"  uint32 mode = 2;\n}\n\nmessage BatchItemError", "  uint64 mode = 2;\n}\n\nmessage BatchItemError", "field mode type"},
		"enum value":      {"OPEN_FLAG_CREATE = 64;", "OPEN_FLAG_CREATE = 65;", "OPEN_FLAG_CREATE is 65"},
		"enum value drop": {"  WHENCE_END = 2;\n", "", "WHENCE_END is missing"},
		"extra message":   {"message AccessRequest {", "message Stray {\n  uint32 x = 1;\n}\n\nmessage AccessRequest {", "message Stray that the spec lacks"},
		"oneof":           {"message AccessRequest {\n  string path = 1;\n  uint32 mode = 2;\n}", "message AccessRequest {\n  oneof o {\n    string path = 1;\n  }\n  uint32 mode = 2;\n}", "oneof"},
		"package":         {"package mountos.filesystem.v1;", "package mountos.other.v1;", "package is"},
	}
	for name, m := range mutations {
		t.Run(name, func(t *testing.T) {
			if !strings.Contains(text, m.from) {
				t.Fatalf("mutation anchor not found in the proto: %q", m.from)
			}
			path := filepath.Join(t.TempDir(), "mountos_fs.proto")
			if err := os.WriteFile(path, []byte(strings.Replace(text, m.from, m.to, 1)), 0o644); err != nil {
				t.Fatal(err)
			}
			diffs := descriptorDiffs(s, compileProto(t, path))
			if !anyContains(diffs, m.want) {
				t.Fatalf("mutation not reported, want a diff containing %q, got %v", m.want, diffs)
			}
		})
	}
}

// A spec change that was not regenerated into the proto must be reported.
func TestDescriptorCheckDetectsSpecIDChange(t *testing.T) {
	s := loadCommittedSpec(t)
	f := s.Messages["BatchItemError"]["index"]
	f.ID = 7
	s.Messages["BatchItemError"]["index"] = f
	diffs := descriptorDiffs(s, compileProto(t, committedProto))
	if !anyContains(diffs, "field index is number 1 in the proto, 7 in the spec") {
		t.Fatalf("spec id change not reported: %v", diffs)
	}
}
