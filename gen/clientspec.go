package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// ClientOp is one entry of api/client.yaml. It shapes how an operation appears
// in the SDKs without changing the wire contract.
type ClientOp struct {
	Scope  string `yaml:"scope"`
	Name   string `yaml:"name"`
	Doc    string `yaml:"doc"`
	Result string `yaml:"result"`
	// Defaults gives integer parameters a default value, keyed by wire field name.
	Defaults map[string]int64 `yaml:"defaults"`
	Custom   bool             `yaml:"custom"`
	Planned  bool             `yaml:"planned"`
}

type ClientSpec struct {
	Ops map[string]ClientOp `yaml:"ops"`
}

func loadClientSpec(path string) (*ClientSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read client spec %s: %w", path, err)
	}
	var cs ClientSpec
	if err := yaml.Unmarshal(data, &cs); err != nil {
		return nil, fmt.Errorf("parse client spec %s: %w", path, err)
	}
	return &cs, nil
}

// StubField is one request or response field in wire order.
type StubField struct {
	Name string // snake_case wire name
	ID   int
	Type string // spec type
}

// Stub is one operation resolved for code generation. Emit is false for custom
// and planned operations, which keep their hand-written code or have none yet.
type Stub struct {
	Op     Operation
	Name   string // public method name
	Scope  string // client, file, or dir
	Doc    string
	Result string // result struct name when the response has several fields
	// Defaults maps a parameter's wire name to its default value.
	Defaults map[string]int64
	ReqMsg   string // wire message name for the request
	RespMsg  string // wire message name for the response
	Params   []StubField
	Returns  []StubField
	Emit     bool
}

// shapeFields returns the fields of a request or response, whether it names a
// message or is inline.
func shapeFields(s *Spec, sh Shape) map[string]Field {
	if sh.Ref != "" {
		return s.Messages[sh.Ref]
	}
	return sh.Fields
}

func shapeMsg(op Operation, sh Shape, suffix string) string {
	if sh.Ref != "" {
		return sh.Ref
	}
	return op.Name + suffix
}

func orderedFields(m map[string]Field) []StubField {
	out := make([]StubField, 0, len(m))
	for name, f := range m {
		out = append(out, StubField{Name: name, ID: f.ID, Type: f.Type})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// buildStubs resolves every operation against the client spec and reports every
// mismatch, so one run lists all problems.
func buildStubs(s *Spec, cs *ClientSpec) ([]Stub, []error) {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	known := map[string]bool{}
	for _, op := range s.Operations {
		known[op.Name] = true
	}
	for name := range cs.Ops {
		if !known[name] {
			add("client.yaml: operation %q is not in filesystem.yaml", name)
		}
	}

	var stubs []Stub
	taken := map[string]string{} // scope+name -> operation
	results := map[string]string{}
	for _, op := range s.Operations {
		entry, ok := cs.Ops[op.Name]
		if !ok {
			add("client.yaml: operation %q has no entry", op.Name)
			continue
		}
		if entry.Custom && entry.Planned {
			add("client.yaml: %s is both custom and planned", op.Name)
		}
		params := orderedFields(shapeFields(s, op.Request))
		returns := orderedFields(shapeFields(s, op.Response))

		hasHandle := false
		for _, p := range params {
			if p.Name == "handle" {
				hasHandle = true
			}
		}
		scope := entry.Scope
		if scope == "" {
			scope = "client"
			if hasHandle {
				scope = "file"
			}
		}
		if scope != "client" && scope != "file" && scope != "dir" {
			add("client.yaml: %s has unknown scope %q", op.Name, scope)
		}
		if (scope == "file" || scope == "dir") && !hasHandle {
			add("client.yaml: %s has scope %s but its request has no handle field", op.Name, scope)
		}

		st := Stub{
			Op:       op,
			Name:     entry.Name,
			Scope:    scope,
			Doc:      entry.Doc,
			Result:   entry.Result,
			ReqMsg:   shapeMsg(op, op.Request, "Request"),
			RespMsg:  shapeMsg(op, op.Response, "Response"),
			Emit:     !entry.Custom && !entry.Planned,
			Defaults: entry.Defaults,
		}
		if st.Name == "" {
			st.Name = op.Name
		}
		for _, p := range params {
			if p.Name != "handle" {
				st.Params = append(st.Params, p)
			}
		}
		st.Returns = returns
		for field := range entry.Defaults {
			found := false
			for _, p := range st.Params {
				found = found || p.Name == field
			}
			if !found {
				add("client.yaml: %s has a default for %q, which is not a parameter", op.Name, field)
			}
		}

		if st.Emit {
			if op.Kind != "unary" {
				add("client.yaml: %s has kind %s, mark it custom", op.Name, op.Kind)
			}
			if st.Doc == "" {
				add("client.yaml: %s needs a doc line", op.Name)
			}
			if len(returns) > 1 && st.Result == "" {
				add("client.yaml: %s has %d response fields and needs a result name", op.Name, len(returns))
			}
			if st.Result != "" {
				if len(returns) <= 1 {
					add("client.yaml: %s names a result but has %d response fields", op.Name, len(returns))
				}
				if prev, dup := results[st.Result]; dup {
					add("client.yaml: result %s used by %s and %s", st.Result, prev, op.Name)
				}
				results[st.Result] = op.Name
			}
			key := scope + "." + st.Name
			if prev, dup := taken[key]; dup {
				add("client.yaml: %s and %s both produce %s %s", prev, op.Name, scope, st.Name)
			}
			taken[key] = op.Name
		}
		stubs = append(stubs, st)
	}
	return stubs, errs
}

// camelParts splits a snake_case wire name.
func camelParts(name string) []string { return strings.Split(name, "_") }

// upperCamel turns link_path into LinkPath.
func upperCamel(name string) string {
	var b strings.Builder
	for _, p := range camelParts(name) {
		if p == "" {
			continue
		}
		b.WriteString(strings.ToUpper(p[:1]) + p[1:])
	}
	return b.String()
}

// lowerCamel turns link_path into linkPath.
func lowerCamel(name string) string {
	u := upperCamel(name)
	if u == "" {
		return u
	}
	return strings.ToLower(u[:1]) + u[1:]
}

// lowerFirst lowercases the first letter, so "Creates a directory." can follow
// the method name as a sentence.
func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToLower(s[:1]) + s[1:]
}
