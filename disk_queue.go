package qstore

import "sync"

type diskQueue struct {
	path string
	//map[string][]byte:
	//key is filename , value is startIdxOff and endIdxOff. length is 32
	diskFiles *sync.Map
}

func newDiskQueue(key string) (*diskQueue, error) {
	return &diskQueue{
		path:      key,
		diskFiles: &sync.Map{},
	}, nil
}

func (dq *diskQueue) write(b []byte) error {

}

func (dq *diskQueue) read(idx uint64) ([]byte, error) {

}
