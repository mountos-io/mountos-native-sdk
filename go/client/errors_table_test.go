package client

import (
	"errors"
	"syscall"
	"testing"
)

func TestWireErrorMatchesAddedErrnos(t *testing.T) {
	cases := map[string]syscall.Errno{
		"ENAMETOOLONG": syscall.ENAMETOOLONG, "EBUSY": syscall.EBUSY, "EMLINK": syscall.EMLINK,
		"EDQUOT": syscall.EDQUOT, "ESTALE": syscall.ESTALE,
	}
	for name, errno := range cases {
		if !errors.Is(&WireError{Code: name}, errno) {
			t.Errorf("%s does not match %v", name, errno)
		}
	}
}
