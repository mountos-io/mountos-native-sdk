// Command gen validates api/filesystem.yaml, and checks it against the
// committed contract snapshot so breaking changes and un-bumped additions fail.
//
//	go run . -spec ../api/filesystem.yaml -snapshot ../api/contract-snapshot.json          # check
//	go run . -spec ../api/filesystem.yaml -snapshot ../api/contract-snapshot.json -update  # write snapshot
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	specPath := flag.String("spec", "api/filesystem.yaml", "path to the API spec")
	snapPath := flag.String("snapshot", "api/contract-snapshot.json", "path to the committed contract snapshot")
	update := flag.Bool("update", false, "write the snapshot after validation and compatibility checks pass")
	protoOut := flag.String("proto-out", "", "write the generated proto3 file to this path")
	clientPath := flag.String("client", "api/client.yaml", "path to the public client API spec")
	outDirs := make([]*string, len(emitters))
	for i, e := range emitters {
		outDirs[i] = flag.String(e.flag, "", e.desc)
	}
	verify := flag.Bool("verify", false, "fail when a generated file differs from disk instead of writing it")
	flag.Parse()

	spec, err := loadSpec(*specPath)
	if err != nil {
		fail(err)
	}
	if errs := validate(spec); len(errs) > 0 {
		fail(errors.Join(errs...))
	}
	if *protoOut != "" {
		src := genProto(spec, "mountos.filesystem.v1", "github.com/mountos-io/mountos-native-sdk/go/wire;wire")
		if err := writeOrVerify(*protoOut, []byte(src), *verify); err != nil {
			fail(fmt.Errorf("write proto %s: %w", *protoOut, err))
		}
	}

	cs, err := loadClientSpec(*clientPath)
	if err != nil {
		fail(err)
	}
	stubs, errs := buildStubs(spec, cs)
	if len(errs) > 0 {
		fail(errors.Join(errs...))
	}
	for i, e := range emitters {
		if *outDirs[i] == "" {
			continue
		}
		src, err := e.emit(spec, stubs)
		if err != nil {
			fail(fmt.Errorf("%s: %w", e.flag, err))
		}
		if err := writeOrVerify(filepath.Join(*outDirs[i], e.file), src, *verify); err != nil {
			fail(fmt.Errorf("%s: %w", e.flag, err))
		}
	}

	next := snapshotOf(spec)
	prev, err := readSnapshot(*snapPath)
	if err != nil {
		fail(err)
	}
	if prev != nil {
		report := diffSnapshots(prev, next)
		if len(report.breaking) > 0 {
			fail(fmt.Errorf("breaking changes against %s:\n  %s", *snapPath, joinLines(report.breaking)))
		}
		if len(report.additive) > 0 && spec.MinorVersion <= prev.MinorVersion {
			fail(fmt.Errorf("additive changes need a minorVersion bump (currently %d):\n  %s",
				spec.MinorVersion, joinLines(report.additive)))
		}
	}

	if *update {
		if err := writeSnapshot(*snapPath, next); err != nil {
			fail(err)
		}
		fmt.Printf("wrote %s\n", *snapPath)
		return
	}
	fmt.Println("spec ok")
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "gen:", err)
	os.Exit(1)
}

func joinLines(lines []string) string {
	out := ""
	for i, l := range lines {
		if i > 0 {
			out += "\n  "
		}
		out += l
	}
	return out
}

// Snapshot is the reviewed, committed view of the contract. It holds resolved
// shapes so a diff never depends on YAML formatting.
type Snapshot struct {
	Version      int                         `json:"version"`
	MinorVersion int                         `json:"minorVersion"`
	Capabilities []string                    `json:"capabilities"`
	Enums        map[string]map[string]int   `json:"enums"`
	Messages     map[string]map[string]Field `json:"messages"`
	Operations   map[string]SnapOp           `json:"operations"`
	ErrorCodes   map[string]int              `json:"errorCodes"`
}

type SnapOp struct {
	Name       string   `json:"name"`
	Kind       string   `json:"kind"`
	Request    Shape    `json:"request"`
	Response   Shape    `json:"response"`
	Errors     []string `json:"errors"`
	Capability string   `json:"capability,omitempty"`
}

func snapshotOf(s *Spec) *Snapshot {
	snap := &Snapshot{
		Version:      s.Version,
		MinorVersion: s.MinorVersion,
		Capabilities: s.Capabilities,
		Enums:        map[string]map[string]int{},
		Messages:     s.Messages,
		Operations:   map[string]SnapOp{},
		ErrorCodes:   map[string]int{},
	}
	for name, e := range s.Enums {
		snap.Enums[name] = e.Values
	}
	for _, op := range s.Operations {
		snap.Operations[fmt.Sprint(op.ID)] = SnapOp{
			Name: op.Name, Kind: op.Kind, Request: op.Request, Response: op.Response,
			Errors: op.Errors, Capability: op.Capability,
		}
	}
	for _, ec := range s.ErrorCodes {
		snap.ErrorCodes[ec.Name] = ec.Code
	}
	return snap
}

func readSnapshot(path string) (*Snapshot, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read snapshot %s: %w", path, err)
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("parse snapshot %s: %w", path, err)
	}
	return &snap, nil
}

func writeSnapshot(path string, snap *Snapshot) error {
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

// writeOrVerify writes a generated file, or in verify mode reports whether the
// file on disk differs, so CI can catch outputs that were not regenerated.
func writeOrVerify(path string, data []byte, verify bool) error {
	if !verify {
		return os.WriteFile(path, data, 0o644)
	}
	cur, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("verify %s: %w", path, err)
	}
	if !bytes.Equal(cur, data) {
		return fmt.Errorf("%s is out of date, run make api-gen", path)
	}
	return nil
}
