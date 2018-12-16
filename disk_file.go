package qstore

import (
	"os"
	"strconv"
)

type diskFile struct {
	number   int
	preName  string
	startIdx uint64
	endIdx   uint64
	idxFile  *os.File
	dataFile *os.File
}

func newDiskFile(number int, preName string, startIdx uint64) (*diskFile, error) {
	name := preName + "-" + strconv.Itoa(number)
	idxFile, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	dataFile, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &diskFile{
		number:   number,
		preName:  preName,
		startIdx: startIdx,
		endIdx:   startIdx,
		idxFile:  idxFile,
		dataFile: dataFile,
	}, nil
}

func (df *diskFile) writeIdx() {

}

func (df *diskFile) write(b []byte) error {

}

func (df *diskFile) readIdx() {

}

func (df *diskFile) read() {

}

func (df *diskFile) truncate() {

}
