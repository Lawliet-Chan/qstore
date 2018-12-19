package qstore

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"strconv"
)

type diskFile struct {
	number  int
	preName string
	opt     *Options

	startIdx uint64
	endIdx   uint64
	idxOff   []byte

	idxFile    *os.File
	dataFile   *os.File
	dataFileSz uint64
}

func newDiskFile(number int, preName string, startIndex uint64, opt *Options) (*diskFile, error) {
	name := preName + "-" + strconv.Itoa(number)
	idxFile, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	dataFile, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	ifd, _ := idxFile.Stat()
	var startIdx, endIdx uint64
	var idxOff []byte
	idxLen := ifd.Size()
	if idxLen >= 16 {
		idxOff, err = ioutil.ReadAll(idxFile)
		if err != nil {
			return nil, err
		}
		//idxOff := make([]byte, 16)
		//idxFile.ReadAt(idxOff, 0)
		startIdx, _ = decode(idxOff[:16])
		//idxFile.ReadAt(idxOff, ifd.Size()-8)
		endIdx, _ = decode(idxOff[idxLen-16:])
	} else {
		startIdx, endIdx = startIndex, startIndex
		idxOff = make([]byte, 0)
	}
	dfd, _ := dataFile.Stat()
	return &diskFile{
		number:     number,
		preName:    preName,
		startIdx:   startIdx,
		endIdx:     endIdx,
		idxFile:    idxFile,
		dataFile:   dataFile,
		dataFileSz: uint64(dfd.Size()),
		opt:        opt,
		idxOff:     idxOff,
	}, nil
}

func (df *diskFile) writeIdx(idx, offset uint64) error {
	byt := encode(idx, offset)
	_, err := df.idxFile.Write(byt)
	if err != nil {
		return err
	}
	if !df.opt.NoSync {
		df.idxFile.Sync()
	}
	if idx < df.startIdx {
		df.startIdx = idx
	}
	if idx > df.endIdx {
		df.endIdx = idx
	}
	df.idxOff = append(df.idxOff, byt)
	return nil
}

func (df *diskFile) write(b []byte) (uint64, error) {
	n, err := df.dataFile.Write(b)
	if err != nil {
		return 0, err
	}
	if !df.opt.NoSync {
		df.dataFile.Sync()
	}
	return df.dataFileSz + uint64(n), nil
}

func (df *diskFile) readIdx(idx uint64) (uint64, error) {
	off := int64((idx - df.startIdx) * 16)
	idxOffByt := make([]byte, 16)
	if len(df.idxOff) >= 16 {
		idxOffByt = df.idxOff[off : off+16]
	} else {
		_, err := df.idxFile.ReadAt(idxOffByt, off)
		if err != nil {
			return 0, err
		}
	}
	_, offset := decode(idxOffByt)
	return offset, nil
}

func (df *diskFile) read(startOff, endOff uint64) ([]byte, error) {
	len := int(endOff - startOff)
	if df.opt.Mmap {
		return mmapRead(df.dataFile, int64(startOff), len)
	}
	data := make([]byte, len)
	_, err := df.dataFile.ReadAt(data, int64(startOff))
	if err != nil {
		return nil, err
	}
	return data, nil
}

func encode(idx, offset uint64) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[:8], idx)
	binary.BigEndian.PutUint64(b[8:], offset)
	return b
}

func decode(b []byte) (idx, offset uint64) {
	return binary.BigEndian.Uint64(b[:8]), binary.BigEndian.Uint64(b[8:])
}
