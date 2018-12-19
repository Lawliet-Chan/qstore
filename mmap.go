package qstore

import (
	"os"
	"syscall"
	"unsafe"
)

func mmapRead(f *os.File, start int64, len int) ([]byte, error) {
	data, err := syscall.Mmap(int(f.Fd()), start, len, syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}
	err = madvise(data, syscall.MADV_SEQUENTIAL)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func madvise(b []byte, advice int) (err error) {
	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(advice))
	if e1 != 0 {
		err = e1
	}
	return
}
