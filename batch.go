package qstore

type Batch struct {
}

func (b *Batch) Write(data []byte) (uint64, error) {

}

func (b *Batch) WriteBatch() {

}

func (b *Batch) Flush() error {

}

func (b *Batch) Cancel() error {

}
