//go:build !cgo

package main

import "errors"

// CGO_ENABLED=0: a static binary; there is no C path, and full() never calls cFsync.
const cgoBuild = false

func cFsync(fd int) error { return errors.New("no cgo in this build") }
