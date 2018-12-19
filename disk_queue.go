package qstore

import (
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
	sdf := dq.diskFiles.getDiskFile(startIdx)
	startOff, err := sdf.readIdx(startIdx)
	if err != nil {
		return nil, err
	}
	edf := dq.diskFiles.getDiskFile(endIdx)
	endOff, err := edf.readIdx(endIdx)
	if err != nil {
		return nil, err
	}

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

func (fs *diskFiles) getDiskFile(idx uint64) *diskFile {
	fs.RLock()
	defer fs.RUnlock()
	for _, df := range fs.dfs {
		if df.startIndex() <= idx && df.endIndex() >= idx {
			return df
		}
	}
	return nil
}

func (fs *diskFiles) addDiskFile(d *diskFile) {
	fs.Lock()
	defer fs.Unlock()
	fs.dfs = append(fs.dfs, d)
}
