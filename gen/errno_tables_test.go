package main

import (
	"os"
	"strings"
	"testing"
)

// Every SDK must name each POSIX error the spec allows, so a code a server sends is
// never unknown to one language.
func TestSDKErrorTablesCoverSpecErrnos(t *testing.T) {
	files := map[string]string{
		"go":   "../go/client/errors.go",
		"ts":   "../ts/src/errors.ts",
		"rust": "../rust/src/error.rs",
	}
	for lang, path := range files {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%s: %v", lang, err)
		}
		for name := range posixErrnos {
			if !strings.Contains(string(src), `"`+name+`"`) {
				t.Errorf("%s table (%s) lacks %s", lang, path, name)
			}
		}
	}
}

// Every error an operation lists must be a POSIX name or a declared protocol code.
func TestSpecOperationErrorsAreKnown(t *testing.T) {
	s, err := loadSpec("../api/filesystem.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if errs := validate(s); len(errs) != 0 {
		t.Fatalf("spec invalid: %v", errs)
	}
}
