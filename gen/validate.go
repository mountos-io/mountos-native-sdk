package main

import (
	"fmt"
	"strings"
)

// validate checks the spec's internal consistency. It returns every problem
// found, not just the first, so one run reports the full list.
func validate(s *Spec) []error {
	var errs []error
	add := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	if s.API == "" {
		add("api: missing")
	}
	if s.Version < 1 {
		add("version: must be >= 1, got %d", s.Version)
	}
	if s.MinorVersion < 0 {
		add("minorVersion: must be >= 0, got %d", s.MinorVersion)
	}

	caps := map[string]bool{}
	for _, c := range s.Capabilities {
		if caps[c] {
			add("capabilities: duplicate %q", c)
		}
		caps[c] = true
	}

	enumNames := map[string]bool{}
	for name, e := range s.Enums {
		enumNames[name] = true
		nums := map[int]string{}
		for v, n := range e.Values {
			if prev, dup := nums[n]; dup {
				add("enum %s: value %d used by %s and %s", name, n, prev, v)
			}
			nums[n] = v
		}
	}

	msgNames := map[string]bool{}
	for name := range s.Messages {
		if enumNames[name] {
			add("message %s: name also used by an enum", name)
		}
		msgNames[name] = true
	}

	checkField := func(where string, f Field, ids map[int]string, fname string) {
		if f.ID <= 0 {
			add("%s: field %s has id %d, must be > 0", where, fname, f.ID)
		}
		if prev, dup := ids[f.ID]; dup {
			add("%s: field id %d used by %s and %s", where, f.ID, prev, fname)
		}
		ids[f.ID] = fname
		if err := checkType(f.Type, enumNames, msgNames); err != nil {
			add("%s: field %s: %v", where, fname, err)
		}
	}

	for name, fields := range s.Messages {
		ids := map[int]string{}
		for fname, f := range fields {
			checkField("message "+name, f, ids, fname)
		}
	}

	seenOpID := map[int]string{}
	seenOpName := map[string]bool{}
	for _, op := range s.Operations {
		where := "operation " + op.Name
		if op.Name == "" {
			add("operation id %d: missing name", op.ID)
		}
		if seenOpName[op.Name] {
			add("%s: duplicate name", where)
		}
		seenOpName[op.Name] = true
		if op.ID <= 0 {
			add("%s: id %d must be > 0", where, op.ID)
		}
		if prev, dup := seenOpID[op.ID]; dup {
			add("%s: id %d already used by %s", where, op.ID, prev)
		}
		seenOpID[op.ID] = op.Name
		if !opKinds[op.Kind] {
			add("%s: unknown kind %q", where, op.Kind)
		}
		if op.Capability != "" && !caps[op.Capability] {
			add("%s: capability %q not declared in capabilities", where, op.Capability)
		}
		for _, sh := range []struct {
			label string
			shape Shape
		}{{"request", op.Request}, {"response", op.Response}} {
			errs = append(errs, checkShape(where+" "+sh.label, sh.shape, msgNames, enumNames)...)
		}
		for _, e := range op.Errors {
			if !posixErrnos[e] && !isProtocolError(s, e) {
				add("%s: error %q is neither a POSIX errno nor a declared errorCode", where, e)
			}
		}
	}

	seenCode := map[int]string{}
	seenName := map[string]bool{}
	for _, ec := range s.ErrorCodes {
		if prev, dup := seenCode[ec.Code]; dup {
			add("errorCode %s: code %d already used by %s", ec.Name, ec.Code, prev)
		}
		seenCode[ec.Code] = ec.Name
		if seenName[ec.Name] {
			add("errorCode %s: duplicate name", ec.Name)
		}
		seenName[ec.Name] = true
		if posixErrnos[ec.Name] {
			add("errorCode %s: clashes with a POSIX errno name", ec.Name)
		}
	}
	return errs
}

func checkShape(where string, sh Shape, msgNames, enumNames map[string]bool) []error {
	var errs []error
	if sh.Ref == "" && sh.Fields == nil {
		return append(errs, fmt.Errorf("%s: missing", where))
	}
	if sh.Ref != "" && !msgNames[sh.Ref] {
		errs = append(errs, fmt.Errorf("%s: unknown message %q", where, sh.Ref))
	}
	ids := map[int]string{}
	for fname, f := range sh.Fields {
		if f.ID <= 0 {
			errs = append(errs, fmt.Errorf("%s: field %s has id %d, must be > 0", where, fname, f.ID))
		}
		if prev, dup := ids[f.ID]; dup {
			errs = append(errs, fmt.Errorf("%s: field id %d used by %s and %s", where, f.ID, prev, fname))
		}
		ids[f.ID] = fname
		if err := checkType(f.Type, enumNames, msgNames); err != nil {
			errs = append(errs, fmt.Errorf("%s: field %s: %w", where, fname, err))
		}
	}
	return errs
}

// checkType accepts a scalar, an enum or message name, or one level of
// `repeated <type>`.
func checkType(t string, enumNames, msgNames map[string]bool) error {
	t = strings.TrimSpace(t)
	if rest, ok := strings.CutPrefix(t, "repeated "); ok {
		if strings.HasPrefix(rest, "repeated ") {
			return fmt.Errorf("nested repeated in %q", t)
		}
		t = rest
	}
	if scalarTypes[t] || enumNames[t] || msgNames[t] {
		return nil
	}
	return fmt.Errorf("unknown type %q", t)
}

func isProtocolError(s *Spec, name string) bool {
	for _, ec := range s.ErrorCodes {
		if ec.Name == name {
			return true
		}
	}
	return false
}
