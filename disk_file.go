package qstore

import "os"

type diskFile struct {
	Number   int
	startIdx int64
	endIdx   int64
	idxFile  *os.File
	dataFile *os.File
}

func newDiskFile() (*diskFile, error) {

}

func (df *diskFile) writeIdx() {

}

func (df *diskFile) write() {

}

func (df *diskFile) readIdx() {

}

func (df *diskFile) read() {

}
