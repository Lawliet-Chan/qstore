package qstore

import (
	"github.com/pkg/errors"
	"sync"
)

type diskQueue struct {
	key string

	diskFiles *diskFiles

	//current writing disk_file number, and it's the biggest
	currentFileNum int
	//current writing disk_file
	currentFile *diskFile

	opt *Options
}

func newDiskQueue(key string, opt *Options) (*diskQueue, error) {
	cf, err := newDiskFile(0, key, 0, opt)
	if err != nil {
		return nil, err
	}
	dkfs := &diskFiles{
		dfs: make([]*diskFile, 0),
	}
	dkfs.addDiskFile(cf)
	return &diskQueue{
		key:            key,
		diskFiles:      dkfs,
		currentFileNum: 0,
		currentFile:    cf,
		opt:            opt,
	}, nil
}

func (dq *diskQueue) writeIdx(idx, offset uint64, len int) error {
	return dq.currentFile.writeIdx(idx, offset, len)
}

func (dq *diskQueue) write(b []byte) (uint64, uint64, error) {
	if dq.currentFile.dataFileSize()+int64(len(b)) > dq.opt.FileMaxSize {
		cf, err := newDiskFile(dq.currentFileNum+1, dq.key, dq.currentFile.endIndex()+1, dq.opt)
		if err != nil {
			return 0, 0, err
		}
		dq.currentFileNum++
		dq.currentFile = cf
		//append to diskFiles
		dq.diskFiles.addDiskFile(cf)
	}
	return dq.currentFile.write(b)
}

func (dq *diskQueue) read(startIdx, endIdx uint64) ([]byte, error) {
	sdf, out := dq.diskFiles.getDiskFile(startIdx)
	if out || sdf == nil {
		return nil, errors.New("startIdx missing!")
	}
	startOff, err := sdf.readIdx(startIdx)
	if err != nil {
		return nil, err
	}
	edf, out := dq.diskFiles.getDiskFile(endIdx)
	var endOff uint64
	if out {
		endOff = uint64(edf.dataFileSize())
	} else {
		endOff, err = edf.readIdx(endIdx)
		if err != nil {
			return nil, err
		}
	}
	//fmt.Printf("startOff is %d, endOff is %d\n",startOff,endOff)

	if sdf == edf {
		return sdf.read(startOff, endOff, edf == dq.currentFile)
	}
	byt, err := sdf.read(startOff, sdf.endIndex(), false)
	if err != nil {
		return nil, err
	}
	for i := sdf.number + 1; i < edf.number; i++ {
		bytAll, err := dq.diskFiles.getByNum(i).readAll()
		if err != nil {
			return nil, err
		}
		byt = append(byt, bytAll...)
	}
	endByt, err := edf.read(edf.startIndex(), endOff, edf == dq.currentFile)
	if err != nil {
		return nil, err
	}
	byt = append(byt, endByt...)
	return byt, nil
}

func (dq *diskQueue) truncate() {
	dq.currentFile.truncate()
}

type diskFiles struct {
	sync.RWMutex
	dfs []*diskFile
}

func (fs *diskFiles) getByNum(i int) *diskFile {
	fs.RLock()
	defer fs.RUnlock()
	return fs.dfs[i]
}

//bool is if out of the max endIdx
func (fs *diskFiles) getDiskFile(idx uint64) (*diskFile, bool) {
	fs.RLock()
	defer fs.RUnlock()
	last := len(fs.dfs) - 1
	if fs.dfs[last].endIndex() < idx {
		return fs.dfs[last], true
	}
	//if fs.dfs[0].startIndex() > idx {
	//
	//}
	for _, df := range fs.dfs {
		//fmt.Printf("startIdx is %d, endIdx is %d \n", df.startIdx, df.endIdx)
		if df.startIndex() <= idx && df.endIndex() >= idx {
			return df, false
		}
	}
	return nil, false
}

func (fs *diskFiles) addDiskFile(d *diskFile) {
	fs.Lock()
	defer fs.Unlock()
	fs.dfs = append(fs.dfs, d)
}
