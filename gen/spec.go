package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Field is one message field. ID is the permanent wire field number.
type Field struct {
	ID       int    `yaml:"id" json:"id"`
	Type     string `yaml:"type" json:"type"`
	Required bool   `yaml:"required,omitempty" json:"required,omitempty"`
}

// Shape is a request or response: either a named message or an inline field set.
type Shape struct {
	Ref    string           `json:"ref,omitempty"`
	Fields map[string]Field `json:"fields,omitempty"`
}

func (s *Shape) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		s.Ref = n.Value
		return nil
	}
	return n.Decode(&s.Fields)
}

type Enum struct {
	Values map[string]int `yaml:"values"`
}

type Operation struct {
	Name       string   `yaml:"name"`
	ID         int      `yaml:"id"`
	Kind       string   `yaml:"kind"`
	Request    Shape    `yaml:"request"`
	Response   Shape    `yaml:"response"`
	Errors     []string `yaml:"errors"`
	Capability string   `yaml:"capability"`
}

type ErrorCode struct {
	Code int    `yaml:"code"`
	Name string `yaml:"name"`
}

type Spec struct {
	API          string                      `yaml:"api"`
	Version      int                         `yaml:"version"`
	MinorVersion int                         `yaml:"minorVersion"`
	Capabilities []string                    `yaml:"capabilities"`
	Enums        map[string]Enum             `yaml:"enums"`
	Messages     map[string]map[string]Field `yaml:"messages"`
	Operations   []Operation                 `yaml:"operations"`
	ErrorCodes   []ErrorCode                 `yaml:"errorCodes"`
}

func loadSpec(path string) (*Spec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read spec %s: %w", path, err)
	}
	var s Spec
	if err := yaml.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("parse spec %s: %w", path, err)
	}
	return &s, nil
}

// scalarTypes are the wire scalars a field may use.
var scalarTypes = map[string]bool{
	"bool": true, "int32": true, "int64": true,
	"uint32": true, "uint64": true, "string": true, "bytes": true,
}

// posixErrnos are the filesystem error names an operation may declare.
// Each SDK maps them to its platform errno values.
var posixErrnos = map[string]bool{
	"EACCES": true, "EBADF": true, "EEXIST": true, "EFBIG": true, "EIO": true,
	"EISDIR": true, "ELOOP": true, "ENOENT": true, "ENOSPC": true,
	"ENOTDIR": true, "ENOTEMPTY": true, "EPERM": true, "EROFS": true,
	"EXDEV": true, "EINVAL": true, "EAGAIN": true,
}

var opKinds = map[string]bool{
	"unary": true, "data_request": true, "data_response": true, "server_stream": true,
	"server_push": true,
}
