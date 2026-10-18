package client

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// receiveOnly lists exported structs with exported fields that callers read and never build.
// Every other exported struct with exported fields must start with `_ struct{}`, so an
// unkeyed literal does not compile and a new field is not a breaking change.
var receiveOnly = map[string]string{
	"client.WireError":           "error value built by the client",
	"client.DialError":           "error value built by the client",
	"client.ProtocolError":       "error value built by the client",
	"client.TimeoutError":        "error value built by the client",
	"client.TooManyEntriesError": "error value built by the client",
	"client.WalkTooDeepError":    "error value built by the client",
	"client.Event":               "delivered by the client, has an unexported field already",
}

func isBlankEmptyStruct(f *ast.Field) bool {
	if len(f.Names) != 1 || f.Names[0].Name != "_" {
		return false
	}
	st, ok := f.Type.(*ast.StructType)
	return ok && len(st.Fields.List) == 0
}

func hasExportedField(st *ast.StructType) bool {
	for _, f := range st.Fields.List {
		if len(f.Names) == 0 {
			if ast.IsExported(exprName(f.Type)) {
				return true
			}
			continue
		}
		for _, n := range f.Names {
			if n.IsExported() {
				return true
			}
		}
	}
	return false
}

func exprName(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.StarExpr:
		return exprName(v.X)
	case *ast.SelectorExpr:
		return v.Sel.Name
	}
	return ""
}

func TestExportedStructsStartWithBlankField(t *testing.T) {
	seen := map[string]bool{}
	for _, dir := range []string{".", "../frame"} {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range files {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			src, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, src, parser.SkipObjectResolution)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			for _, decl := range file.Decls {
				gd, ok := decl.(*ast.GenDecl)
				if !ok || gd.Tok != token.TYPE {
					continue
				}
				for _, spec := range gd.Specs {
					ts := spec.(*ast.TypeSpec)
					st, ok := ts.Type.(*ast.StructType)
					if !ok || !ts.Name.IsExported() || !hasExportedField(st) {
						continue
					}
					name := file.Name.Name + "." + ts.Name.Name
					if _, skip := receiveOnly[name]; skip {
						seen[name] = true
						continue
					}
					if len(st.Fields.List) == 0 || !isBlankEmptyStruct(st.Fields.List[0]) {
						t.Errorf("%s (%s): first field must be `_ struct{}`, or list the type in receiveOnly with a reason", name, path)
					}
				}
			}
		}
	}
	for name := range receiveOnly {
		if !seen[name] {
			t.Errorf("receiveOnly lists %s, which is not an exported struct with exported fields", name)
		}
	}
}
