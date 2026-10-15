//go:build cgo

package main

// #include <unistd.h>
import "C"

import "fmt"

// With CGO_ENABLED=1 this file makes probe_go a real cgo binary, dynamically linked against glibc, as Dolt and
// Doltgres are: LD_PRELOAD then loads the shim into it. cFsync calls glibc's fsync through cgo -- the one path from
// Go code that the shim can see.
const cgoBuild = true

func cFsync(fd int) error {
	if C.fsync(C.int(fd)) != 0 {
		return fmt.Errorf("C.fsync(%d) failed", fd)
	}
	return nil
}
