package qstore

import "sync"

type Qstore interface {
	//write
	OpenTx(key string) (*tx, error)
	//read
	Read(key string, idx int64) ([]byte, error)
	//readFrom
	ReadFrom(key string, idx int64) ([]byte, error)
	//readFromTo
	ReadFromTo(key string, fromIdx, ToIdx int64) ([]byte, error)
}

type qstore struct {
	path     string
	keyQueue *sync.Map // key is string,value is *diskQueue
	opt      *Options
}

type Options struct {
	NoSync      bool
	Mmap        bool
	FileMaxSize int64
}

var defaultFileMaxSize = 1024 * 1024 * 1024

func NewQstore(path string, opt *Options) (Qstore, error) {
	if opt == nil {

	}
	return &qstore{
		path:     path,
		keyQueue: &sync.Map{},
		opt:      opt,
	}, nil
}

func (q *qstore) OpenTx(key string) (t *tx, err error) {
	key = q.path + key
	dq, load := q.keyQueue.Load(key)
	if !load {
		dq, err = newDiskQueue(key)
		if err != nil {
			return
		}
		q.keyQueue.Store(key, dq)
	}
	t = &tx{dq: dq.(*diskQueue)}
	return
}

func (q *qstore) Read(key string, idx int64) ([]byte, error) {
	key = q.path + key
}

func (q *qstore) ReadFrom(key string, idx int64) ([]byte, error) {
	key = q.path + key
}

func (q *qstore) ReadFromTo(key string, fromIdx, ToIdx int64) ([]byte, error) {
	key = q.path + key
}
