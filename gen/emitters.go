package main

// emitter renders one language's client stubs from the resolved operations.
// Each emit_*.go file registers itself, so adding a language touches no other file.
type emitter struct {
	flag string // command line flag that names the output directory
	desc string
	file string // generated file name inside that directory
	emit func(s *Spec, stubs []Stub) ([]byte, error)
}

var emitters []emitter
