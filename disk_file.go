package qstore

import (
	"encoding/binary"
	"fmt"
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

	idxFile  *os.File
	dataFile *os.File

	//committed data file size
	dataFileSz uint64
	//put committed-data-file-size in disk
	committed *os.File

	//copy-on-write file data, the data are committed
	cowData []byte
	//writing but uncommitted data
	writingData []byte
}

func newDiskFile(number int, preName string, startIndex uint64, opt *Options) (*diskFile, error) {
	name := preName + "-" + strconv.Itoa(number)
	idxFile, err := os.OpenFile(name+".idx", os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return nil, err
	}
	dataFile, err := os.OpenFile(name+".data", os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return nil, err
	}
	committed, err := os.OpenFile(name+".cmt", os.O_RDWR|os.O_CREATE, os.ModePerm)
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
		startIdx = decodeUint64(idxOff[:16])
		endIdx = decodeUint64(idxOff[idxLen-16:])
	} else {
		startIdx, endIdx = startIndex, startIndex
		idxOff = make([]byte, 0)
	}
	var dataFileSz uint64
	cfd, _ := committed.Stat()
	if cfd.Size() > 0 {
		data, err := ioutil.ReadAll(committed)
		if err != nil {
			return nil, err
		}
		dataFileSz = decodeUint64(data)
	}
	dataFile.Truncate(int64(dataFileSz))
	return &diskFile{
		number:     number,
		preName:    preName,
		startIdx:   startIdx,
		endIdx:     endIdx,
		idxFile:    idxFile,
		dataFile:   dataFile,
		committed:  committed,
		dataFileSz: dataFileSz,
		opt:        opt,
		idxOff:     idxOff,
		cowData:    make([]byte, 0),
	}, nil
}

func (df *diskFile) writeIdx(idx, offset uint64, len int) error {
	byt := encode(idx, offset)
	_, err := df.idxFile.Write(byt)
	if err != nil {
		return err
	}

	cbyt := encodeUint64(df.dataFileSz + uint64(len))
	//fmt.Println("length is ", df.dataFileSz+uint64(len))
	//fmt.Println("datafile size is ", cbyt)
	_, err = df.committed.WriteAt(cbyt, 0)
	if err != nil {
		return err
	}
	if !df.opt.NoSync {
		df.idxFile.Sync()
		df.committed.Sync()
	}

	df.dataFileSz += uint64(len)
	if idx < df.startIdx {
		df.startIdx = idx
	}
	if idx > df.endIdx {
		df.endIdx = idx
	}
	df.idxOff = append(df.idxOff, byt...)

	df.cowData = append(df.cowData, df.writingData...)
	df.writingData = nil

	return nil
}

//return idx,offset,error
func (df *diskFile) write(b []byte) (uint64, uint64, error) {
	n, err := df.dataFile.Write(b)
	if err != nil {
		return 0, 0, err
	}
	if !df.opt.NoSync {
		df.dataFile.Sync()
	}

	df.writingData = b

	return df.endIdx, df.dataFileSz + uint64(n), nil
}

func (df *diskFile) readIdx(idx uint64) (uint64, error) {
	off := int64((idx - df.startIdx) * 16)
	idxOffByt := make([]byte, 16)
	if len(df.idxOff) >= 16 {
		fmt.Printf("idx is %d,startIdx is %d \n", idx, df.startIdx)
		fmt.Println("idxOff is ", decodeUint64(df.idxOff))
		fmt.Println("idxOff length is ", len(df.idxOff))
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

func (df *diskFile) read(startOff, endOff uint64, readCow bool) ([]byte, error) {
	len := int(endOff - startOff)

	if readCow {
		return df.cowData[int(startOff):int(endOff)], nil
	}
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

func (df *diskFile) readAll() ([]byte, error) {
	if df.opt.Mmap {
		return mmapRead(df.dataFile, 0, int(df.dataFileSz))
	}
	return ioutil.ReadAll(df.dataFile)
}

func (df *diskFile) truncate() {
	df.dataFile.Truncate(int64(df.dataFileSz))
}

func (df *diskFile) dataFileSize() int64 {
	return int64(df.dataFileSz)
}

func (df *diskFile) startIndex() uint64 {
	return df.startIdx
}

func (df *diskFile) endIndex() uint64 {
	return df.endIdx
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

func encodeUint64(u uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, u)
	return b
}

func decodeUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
