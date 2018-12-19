package qstore

type Tx struct {
	idx uint64
	off uint64
	len int
	dq  *diskQueue
}

func (t *Tx) Write(b []byte) (uint64, error) {
	idx, off, err := t.dq.write(b)
	if err != nil {
		return 0, err
	}
	t.idx = idx
	t.off = off
	t.len = len(b)
	return idx, nil
}

func (t *Tx) Commit() error {
	return t.dq.writeIdx(t.idx, t.off, t.len)
}

func (t *Tx) Abort() {
	t.dq.truncate()
}
