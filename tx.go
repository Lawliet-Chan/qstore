package qstore

type tx struct {
	dq *diskQueue
}

func (t *tx) Write(b []byte) (int64, error) {

}

func (t *tx) Commit() error {

}

func (t *tx) Abort() {
	t.dq.truncate()
}
