// probe_go -- V1 Linux fire-check probe in Go. Dolt and Doltgres are Go, and on Linux the Go runtime and the os,
// syscall and x/sys/unix packages make raw syscalls (cgo or not), so LD_PRELOAD sees none of the Go-code calls
// below; the strace counter sees every one. Built twice: CGO_ENABLED=0 (static: the shim never loads) and
// CGO_ENABLED=1 (dynamic: the shim loads and attaches, and must count exactly the K fsyncs made through cgo).
//
//	probe_go full|child <K> <dir>
//
// full, in order: K x os.File.Sync (fsync), K x unix.Fdatasync, K x SyncFileRange(WAIT_BEFORE|WRITE|WAIT_AFTER),
// K x SyncFileRange(WRITE), K x Syncfs, K x Msync(MS_SYNC), K x Msync(MS_ASYNC); K x Write and K x WriteAt on an
// O_DSYNC file; K x Write on an O_SYNC file; K x Pwritev2(RWF_DSYNC) on a plain file; K x FICLONE, K x FICLONERANGE,
// K x copy_file_range; one Sync; in the cgo build only, K x C.fsync; then os/exec of itself in "child" mode, which
// makes K x os.File.Sync.
//
// Expected counts are derived from K and the build by firecheck.py; the pids printed only say which is which.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"

	"golang.org/x/sys/unix"
)

func must(err error, what string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "probe_go: %s: %v\n", what, err)
		os.Exit(1)
	}
}

func open(dir, name string, flags int) *os.File {
	f, err := os.OpenFile(filepath.Join(dir, name), flags|os.O_CREATE, 0o644)
	must(err, "open "+name)
	return f
}

func full(k int, dir string) {
	f := open(dir, "go_a", os.O_RDWR|os.O_TRUNC)
	buf := make([]byte, 65536)
	_, err := f.Write(buf)
	must(err, "write a")
	fd := int(f.Fd())
	for i := 0; i < k; i++ {
		must(f.Sync(), "File.Sync")
	}
	for i := 0; i < k; i++ {
		must(unix.Fdatasync(fd), "Fdatasync")
	}
	for i := 0; i < k; i++ {
		must(unix.SyncFileRange(fd, 0, 0, unix.SYNC_FILE_RANGE_WAIT_BEFORE|unix.SYNC_FILE_RANGE_WRITE|unix.SYNC_FILE_RANGE_WAIT_AFTER), "sfr wait")
	}
	for i := 0; i < k; i++ {
		must(unix.SyncFileRange(fd, 0, 0, unix.SYNC_FILE_RANGE_WRITE), "sfr write")
	}
	for i := 0; i < k; i++ {
		must(unix.Syncfs(fd), "Syncfs")
	}
	m, err := unix.Mmap(fd, 0, len(buf), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	must(err, "mmap")
	for i := 0; i < k; i++ {
		m[i%4096] ^= 1
		must(unix.Msync(m, unix.MS_SYNC), "msync SYNC")
	}
	for i := 0; i < k; i++ {
		m[i%4096] ^= 1
		must(unix.Msync(m, unix.MS_ASYNC), "msync ASYNC")
	}
	must(unix.Munmap(m), "munmap")

	d := open(dir, "go_dsync", os.O_WRONLY|os.O_TRUNC|unix.O_DSYNC)
	for i := 0; i < k; i++ {
		_, err := d.Write(buf[:512])
		must(err, "dsync write")
	}
	for i := 0; i < k; i++ {
		_, err := d.WriteAt(buf[:512], 4096)
		must(err, "dsync pwrite")
	}
	must(d.Close(), "close dsync")

	s := open(dir, "go_osync", os.O_WRONLY|os.O_TRUNC|unix.O_SYNC)
	for i := 0; i < k; i++ {
		_, err := s.Write(buf[:512])
		must(err, "osync write")
	}
	must(s.Close(), "close osync")

	p := open(dir, "go_plainv2", os.O_WRONLY|os.O_TRUNC)
	for i := 0; i < k; i++ {
		_, err := unix.Pwritev2(int(p.Fd()), [][]byte{buf[:512]}, 0, unix.RWF_DSYNC)
		must(err, "pwritev2 RWF_DSYNC")
	}
	must(p.Close(), "close plainv2")

	src := open(dir, "go_clsrc", os.O_RDWR|os.O_TRUNC)
	_, err = src.Write(buf)
	must(err, "write clsrc")
	dst := open(dir, "go_cldst", os.O_RDWR|os.O_TRUNC)
	sfd, dfd := int(src.Fd()), int(dst.Fd())
	cloneFail, rangeFail := 0, 0
	for i := 0; i < k; i++ {
		if unix.IoctlFileClone(dfd, sfd) != nil {
			cloneFail++
		}
	}
	for i := 0; i < k; i++ {
		r := unix.FileCloneRange{Src_fd: int64(sfd), Src_offset: 0, Src_length: 4096, Dest_offset: 0}
		if unix.IoctlFileCloneRange(dfd, &r) != nil {
			rangeFail++
		}
	}
	for i := 0; i < k; i++ {
		var in, out int64
		n, err := unix.CopyFileRange(sfd, &in, dfd, &out, 4096, 0)
		must(err, "copy_file_range")
		if n != 4096 {
			must(fmt.Errorf("short copy %d", n), "copy_file_range")
		}
	}
	must(src.Close(), "close clsrc")
	must(dst.Close(), "close cldst")

	unix.Sync()

	if cgoBuild {
		for i := 0; i < k; i++ {
			must(cFsync(fd), "C.fsync through cgo")
		}
	}
	must(f.Close(), "close a")

	self, err := os.Executable()
	must(err, "self")
	cmd := exec.Command(self, "child", strconv.Itoa(k), dir)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	must(cmd.Run(), "child")
	cg := 0
	if cgoBuild {
		cg = 1
	}
	fmt.Printf("probe_go full K=%d root=%d child=%d cgo=%d clone_fail=%d range_fail=%d\n", k, os.Getpid(),
		cmd.Process.Pid, cg, cloneFail, rangeFail)
}

func child(k int, dir string) {
	f := open(dir, "go_child", os.O_RDWR|os.O_TRUNC)
	_, err := f.Write(make([]byte, 4096))
	must(err, "write child")
	for i := 0; i < k; i++ {
		must(f.Sync(), "child File.Sync")
	}
	must(f.Close(), "close child")
}

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: probe_go full|child K dir")
		os.Exit(2)
	}
	k, err := strconv.Atoi(os.Args[2])
	if err != nil || k < 1 {
		fmt.Fprintln(os.Stderr, "probe_go: K must be >= 1")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "full":
		full(k, os.Args[3])
	case "child":
		child(k, os.Args[3])
	default:
		fmt.Fprintln(os.Stderr, "probe_go: unknown mode")
		os.Exit(2)
	}
}
