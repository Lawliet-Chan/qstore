package qstore

import (
	"github.com/pkg/errors"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
)

type diskQueue struct {
	dkey string

	diskFiles *diskFiles

	//current writing disk_file number, and it's the biggest
	currentFileNum int
	//current writing disk_file
	currentFile *diskFile

	opt *Options
}

func newDiskQueue(dir, key string, opt *Options) (*diskQueue, error) {
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	dkfs := &diskFiles{
		dfs: make([]*diskFile, 0),
	}
	for _, fi := range fis {
		if strings.HasSuffix(fi.Name(), ".data") {
			names := strings.Split(fi.Name(), "-")
			num, err := strconv.Atoi(strings.TrimRight(names[len(names)-1], ".data"))
			if err != nil {
				return nil, err
			}
			df, err := newDiskFile(num, dir+key, 0, opt)
			if err != nil {
				return nil, err
			}
			dkfs.addDiskFile(df)
		}
	}

	if dkfs.isEmpty() {
		cf, err := newDiskFile(0, dir+key, 0, opt)
		if err != nil {
			return nil, err
		}
		dkfs.addDiskFile(cf)
	}

	return &diskQueue{
		dkey:           dir + key,
		diskFiles:      dkfs,
		currentFileNum: 0,
		currentFile:    dkfs.lastDiskFile(),
		opt:            opt,
	}, nil
}

func (dq *diskQueue) writeIdx(idx, offset uint64, len int) error {
	return dq.currentFile.writeIdx(idx, offset, len)
}

func (dq *diskQueue) write(b []byte) (uint64, uint64, error) {
	if dq.currentFile.dataFileSize()+int64(len(b)) > dq.opt.FileMaxSize {
		cf, err := newDiskFile(dq.currentFileNum+1, dq.dkey, dq.currentFile.endIndex()+1, dq.opt)
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
	dfs, out := dq.diskFiles.getDiskFiles(startIdx, endIdx)
	if dfs == nil {
		return nil, errors.New("idx missing!")
	}
	startOff, err := dfs[0].readIdx(startIdx)
	if err != nil {
		return nil, err
	}
	var endOff uint64
	lastFileNum := len(dfs) - 1
	if out {
		endOff = uint64(dfs[lastFileNum].dataFileSize())
	} else {
		endOff, err = dfs[len(dfs)-1].readIdx(endIdx)
		if err != nil {
			return nil, err
		}
	}
	if len(dfs) == 1 {
		df := dfs[0]
		return df.read(startOff, endOff, df == dq.currentFile)
	}

	//fmt.Printf("startOff is %d, endOff is %d\n",startOff,endOff)
	byt, err := dfs[0].read(startOff, uint64(dfs[0].dataFileSize()), false)
	if err != nil {
		return nil, err
	}
	for i := 1; i < lastFileNum; i++ {
		bytAll, err := dfs[i].readAll()
		if err != nil {
			return nil, err
		}
		byt = append(byt, bytAll...)
	}
	endByt, err := dfs[lastFileNum].read(0, endOff, dfs[lastFileNum] == dq.currentFile)
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
func (fs *diskFiles) getDiskFiles(startIdx, endIdx uint64) (dfls []*diskFile, out bool) {
	fs.RLock()
	defer fs.RUnlock()
	//fmt.Printf("type in startidx is %d,endIdx is %d\n", startIdx, endIdx)
	last := len(fs.dfs) - 1
	if fs.dfs[0].startIndex() > startIdx || fs.dfs[last].endIndex() < startIdx {
		//fmt.Printf("ALL diskFiles startIdx is %d, endIdx is %d\n", fs.dfs[0].startIndex(), fs.dfs[last].endIndex())
		return
	}
	var startFileNum, endFileNum int
	for i, df := range fs.dfs {
		//fmt.Printf("startIdx is %d, endIdx is %d \n", df.startIdx, df.endIdx)
		if df.startIndex() <= startIdx && df.endIndex() >= startIdx {
			startFileNum = i
		}
		if df.startIndex() <= endIdx && df.endIndex() > endIdx {
			endFileNum = i
		}
	}
	dfls = fs.dfs[startFileNum : endFileNum+1]

	if fs.dfs[last].endIndex() <= endIdx {
		return dfls, true
	}

	return
}

func (fs *diskFiles) addDiskFile(d *diskFile) {
	fs.Lock()
	defer fs.Unlock()
	fs.dfs = append(fs.dfs, d)
}

func (fs *diskFiles) isEmpty() bool {
	fs.RLock()
	defer fs.RUnlock()
	return len(fs.dfs) == 0
}

func (fs *diskFiles) lastDiskFile() *diskFile {
	fs.RLock()
	defer fs.RUnlock()
	return fs.dfs[len(fs.dfs)-1]
}
