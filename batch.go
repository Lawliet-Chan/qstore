package qstore

type Batch struct {
	t     *Tx
	cache [][]byte
}

func (b *Batch) Write(data []byte) {
	b.cache = append(b.cache, data)
}

func (b *Batch) Flush() (uint32, error) {

	return b.t.Commit()
}
