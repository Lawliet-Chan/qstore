package qstore

import (
	"github.com/pkg/errors"
	"os"
	"strings"
	"sync"
)

type Qstore interface {
	//write
	OpenTx(key string) (*Tx, error)
	//read
	Read(key string, idx uint64) ([]byte, error)
	//readbatch
	ReadBatch(key string, idx uint64, len int) ([]byte, error)
}

type qstore struct {
	dir      string
	keyQueue *sync.Map // key is string(dirkey),value is *diskQueue
	opt      *Options
}

type Options struct {
	NoSync      bool
	Mmap        bool
	FileMaxSize int64
}

var defaultFileMaxSize = 1024 * 1024 * 1024

func NewQstore(dir string, opt *Options) (Qstore, error) {
	err := os.MkdirAll(dir, 0666)
	if err != nil {
		return nil, err
	}

	if !strings.HasSuffix(dir, "/") {
		dir += "/"
	}

	if opt == nil {
		opt.FileMaxSize = int64(defaultFileMaxSize)
	}

	return &qstore{
		dir:      dir,
		keyQueue: &sync.Map{},
		opt:      opt,
	}, nil
}

func (q *qstore) OpenTx(key string) (t *Tx, err error) {
	key = q.dirkey(key)
	dq, load := q.keyQueue.Load(key)
	if !load {
		dq, err = newDiskQueue(key, q.opt)
		if err != nil {
			return
		}
		q.keyQueue.Store(key, dq)
	}
	t = &Tx{dq: dq.(*diskQueue)}
	return
}

func (q *qstore) Read(key string, idx uint64) ([]byte, error) {
	return q.ReadBatch(key, idx, 1)
}

func (q *qstore) ReadBatch(key string, idx uint64, len int) ([]byte, error) {
	key = q.dirkey(key)
	queue, load := q.keyQueue.Load(key)
	if !load {
		return nil, errors.New("no key!")
	}
	return queue.(*diskQueue).read(idx, idx+uint64(len))
}

func (q *qstore) dirkey(key string) string {
	return q.dir + key
}
