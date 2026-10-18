package main

import (
	"fmt"
	"reflect"
	"sort"
)

// changeReport splits contract differences by compatibility. Breaking changes
// fail the check. Additive changes need a minorVersion bump.
type changeReport struct {
	breaking []string
	additive []string
}

// diffSnapshots compares the committed snapshot (prev) with the current spec
// (next). Removing or retyping anything that existed is breaking. Adding a
// new ID is additive. A new required field on an existing message is breaking,
// since old clients never send it. Both lists come back sorted, so the report
// does not depend on map order.
func diffSnapshots(prev, next *Snapshot) changeReport {
	var r changeReport
	breaking := func(format string, args ...any) { r.breaking = append(r.breaking, fmt.Sprintf(format, args...)) }
	additive := func(format string, args ...any) { r.additive = append(r.additive, fmt.Sprintf(format, args...)) }

	if !reflect.DeepEqual(prev.Capabilities, next.Capabilities) {
		for _, c := range prev.Capabilities {
			if !contains(next.Capabilities, c) {
				breaking("capability %q removed", c)
			}
		}
		for _, c := range next.Capabilities {
			if !contains(prev.Capabilities, c) {
				additive("capability %q added", c)
			}
		}
	}

	for name, pv := range prev.Enums {
		nv, ok := next.Enums[name]
		if !ok {
			breaking("enum %s removed", name)
			continue
		}
		for v, n := range pv {
			got, ok := nv[v]
			switch {
			case !ok:
				breaking("enum %s: value %s removed", name, v)
			case got != n:
				breaking("enum %s: value %s renumbered %d -> %d", name, v, n, got)
			}
		}
		for v := range nv {
			if _, ok := pv[v]; !ok {
				additive("enum %s: value %s added", name, v)
			}
		}
	}
	for name := range next.Enums {
		if _, ok := prev.Enums[name]; !ok {
			additive("enum %s added", name)
		}
	}

	for name, pm := range prev.Messages {
		nm, ok := next.Messages[name]
		if !ok {
			breaking("message %s removed", name)
			continue
		}
		diffFields("message "+name, pm, nm, &r)
	}
	for name := range next.Messages {
		if _, ok := prev.Messages[name]; !ok {
			additive("message %s added", name)
		}
	}

	for _, id := range sortedKeys(prev.Operations) {
		po := prev.Operations[id]
		no, ok := next.Operations[id]
		if !ok {
			breaking("operation %s (id %s) removed", po.Name, id)
			continue
		}
		where := fmt.Sprintf("operation %s (id %s)", po.Name, id)
		if po.Name != no.Name {
			breaking("%s renamed to %s", where, no.Name)
		}
		if po.Kind != no.Kind {
			breaking("%s kind %s -> %s", where, po.Kind, no.Kind)
		}
		if po.Capability != no.Capability {
			breaking("%s capability %q -> %q", where, po.Capability, no.Capability)
		}
		for _, e := range po.Errors {
			if !contains(no.Errors, e) {
				breaking("%s no longer returns %s", where, e)
			}
		}
		for _, e := range no.Errors {
			if !contains(po.Errors, e) {
				additive("%s now returns %s", where, e)
			}
		}
		diffShape(where+" request", po.Request, no.Request, &r)
		diffShape(where+" response", po.Response, no.Response, &r)
	}
	for _, id := range sortedKeys(next.Operations) {
		if _, ok := prev.Operations[id]; !ok {
			additive("operation %s (id %s) added", next.Operations[id].Name, id)
		}
	}

	for name, pc := range prev.ErrorCodes {
		nc, ok := next.ErrorCodes[name]
		switch {
		case !ok:
			breaking("errorCode %s removed", name)
		case nc != pc:
			breaking("errorCode %s renumbered %d -> %d", name, pc, nc)
		}
	}
	for name := range next.ErrorCodes {
		if _, ok := prev.ErrorCodes[name]; !ok {
			additive("errorCode %s added", name)
		}
	}
	sort.Strings(r.breaking)
	sort.Strings(r.additive)
	return r
}

func diffShape(where string, prev, next Shape, r *changeReport) {
	if prev.Ref != next.Ref {
		r.breaking = append(r.breaking, fmt.Sprintf("%s: message %q -> %q", where, prev.Ref, next.Ref))
	}
	if prev.Fields != nil || next.Fields != nil {
		diffFields(where, prev.Fields, next.Fields, r)
	}
}

// diffFields matches fields by wire ID. A field ID that disappears or changes
// type or required-ness is breaking. A new optional field is additive.
func diffFields(where string, prev, next map[string]Field, r *changeReport) {
	byID := func(m map[string]Field) map[int]string {
		out := make(map[int]string, len(m))
		for name, f := range m {
			out[f.ID] = name
		}
		return out
	}
	pIDs, nIDs := byID(prev), byID(next)

	for id, pname := range pIDs {
		nname, ok := nIDs[id]
		if !ok {
			r.breaking = append(r.breaking, fmt.Sprintf("%s: field %s (id %d) removed", where, pname, id))
			continue
		}
		pf, nf := prev[pname], next[nname]
		if pname != nname {
			r.breaking = append(r.breaking, fmt.Sprintf("%s: field id %d renamed %s -> %s", where, id, pname, nname))
		}
		if pf.Type != nf.Type {
			r.breaking = append(r.breaking, fmt.Sprintf("%s: field %s type %s -> %s", where, nname, pf.Type, nf.Type))
		}
		if pf.Required != nf.Required && nf.Required {
			r.breaking = append(r.breaking, fmt.Sprintf("%s: field %s became required", where, nname))
		}
	}
	for id, nname := range nIDs {
		if _, ok := pIDs[id]; ok {
			continue
		}
		nf := next[nname]
		if nf.Required {
			r.breaking = append(r.breaking, fmt.Sprintf("%s: new required field %s (id %d)", where, nname, id))
		} else {
			r.additive = append(r.additive, fmt.Sprintf("%s: field %s (id %d) added", where, nname, id))
		}
	}
}

func contains(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
