// probe_go -- V1 Linux fire-check probe in Go. Dolt and Doltgres are Go, and on Linux the Go runtime makes raw
// syscalls (cgo or not), so LD_PRELOAD sees none of these calls and the strace counter must see every one.
//
//	probe_go full|noise|child <K> <dir>
//
// full:  phase marks (the strace marker lseek) around K x os.File.Sync (fsync), K x Fdatasync, K x
//
//	sync_file_range(WAIT_BEFORE|WRITE|WAIT_AFTER), K x sync_file_range(WRITE), K x Syncfs, K x Msync(MS_SYNC),
//	K x Msync(MS_ASYNC), noise, K x Write + K x WriteAt on an O_DSYNC file, K x Write on an O_SYNC file,
//	K x Pwritev2(RWF_DSYNC) on a plain file, K x FICLONE, K x FICLONERANGE, K x copy_file_range, one Sync, then
//	os/exec of itself in "child" mode, which does K x os.File.Sync.
//
// noise: only non-flush calls (F_GETFL, plain Write/WriteAt, Seek); must count zero.
// Expected counts are derived from K by firecheck.py.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"

	"golang.org/x/sys/unix"
)

const markFD = -22065 // V1_MARK_FD in syncshim.h

var cgoUsed int // set by cgo.go when built with CGO_ENABLED=1

func mark(m uint64) {
	fd := markFD
	unix.Syscall(unix.SYS_LSEEK, uintptr(fd), uintptr(m), 0) // fails EBADF; strace reads the mark from it
}

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

func noise(k int, dir string) {
	f := open(dir, "go_noise", os.O_RDWR|os.O_TRUNC)
	buf := make([]byte, 512)
	mark(8)
	for i := 0; i < 3*k; i++ {
		_, err := unix.FcntlInt(f.Fd(), unix.F_GETFL, 0)
		must(err, "F_GETFL")
	}
	for i := 0; i < k; i++ {
		_, err := f.Write(buf)
		must(err, "write")
		_, err = f.WriteAt(buf, 8192)
		must(err, "pwrite")
		_, err = f.Seek(0, 0)
		must(err, "seek")
	}
	must(f.Close(), "close noise")
}

func full(k int, dir string) {
	f := open(dir, "go_a", os.O_RDWR|os.O_TRUNC)
	buf := make([]byte, 65536)
	_, err := f.Write(buf)
	must(err, "write a")
	fd := int(f.Fd())
	mark(1)
	for i := 0; i < k; i++ {
		must(f.Sync(), "File.Sync")
	}
	mark(2)
	for i := 0; i < k; i++ {
		must(unix.Fdatasync(fd), "Fdatasync")
	}
	mark(3)
	for i := 0; i < k; i++ {
		must(unix.SyncFileRange(fd, 0, 0, unix.SYNC_FILE_RANGE_WAIT_BEFORE|unix.SYNC_FILE_RANGE_WRITE|unix.SYNC_FILE_RANGE_WAIT_AFTER), "sfr wait")
	}
	mark(4)
	for i := 0; i < k; i++ {
		must(unix.SyncFileRange(fd, 0, 0, unix.SYNC_FILE_RANGE_WRITE), "sfr write")
	}
	mark(5)
	for i := 0; i < k; i++ {
		must(unix.Syncfs(fd), "Syncfs")
	}
	m, err := unix.Mmap(fd, 0, len(buf), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	must(err, "mmap")
	mark(6)
	for i := 0; i < k; i++ {
		m[i%4096] ^= 1
		must(unix.Msync(m, unix.MS_SYNC), "msync SYNC")
	}
	mark(7)
	for i := 0; i < k; i++ {
		m[i%4096] ^= 1
		must(unix.Msync(m, unix.MS_ASYNC), "msync ASYNC")
	}
	must(unix.Munmap(m), "munmap")
	must(f.Close(), "close a")
	noise(k, dir)

	mark(9)
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

	mark(10)
	s := open(dir, "go_osync", os.O_WRONLY|os.O_TRUNC|unix.O_SYNC)
	for i := 0; i < k; i++ {
		_, err := s.Write(buf[:512])
		must(err, "osync write")
	}
	must(s.Close(), "close osync")

	mark(11)
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
	mark(12)
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

	mark(13)
	unix.Sync()

	self, err := os.Executable()
	must(err, "self")
	mark(14)
	cmd := exec.Command(self, "child", strconv.Itoa(k), dir)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	must(cmd.Run(), "child")
	mark(0)
	fmt.Printf("probe_go full K=%d root=%d child=%d cgo=%d clone_fail=%d range_fail=%d\n", k, os.Getpid(),
		cmd.Process.Pid, cgoUsed, cloneFail, rangeFail)
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
		fmt.Fprintln(os.Stderr, "usage: probe_go full|noise|child K dir")
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
	case "noise":
		noise(k, os.Args[3])
		mark(0)
	case "child":
		child(k, os.Args[3])
	default:
		fmt.Fprintln(os.Stderr, "probe_go: unknown mode")
		os.Exit(2)
	}
}
