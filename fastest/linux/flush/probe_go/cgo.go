//go:build cgo

package main

// #include <unistd.h>
import "C"

// With CGO_ENABLED=1 this file makes probe_go a real cgo binary, dynamically linked against glibc, as Dolt and
// Doltgres are: LD_PRELOAD then loads the shim into it, and the shim must still see none of Go's raw syscalls.
// Without a cgo package the Go linker would emit a static binary even under CGO_ENABLED=1.
func init() { cgoUsed = 1 + 0*int(C.getpid()) }
