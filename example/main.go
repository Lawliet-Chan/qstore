package main

import (
	"fmt"
	"qstore"
)

func main() {
	q, err := qstore.NewQstore("./exqstore", nil)
	if err != nil {
		panic("newQstore error: " + err.Error())
	}
	tx, err := q.OpenTx("topic")

	if err != nil {
		panic("open tx error: " + err.Error())
	}
	idx, err := tx.Write([]byte("lovecraft"))
	if err != nil {
		panic("tx write error: " + err.Error())
	}
	fmt.Println("idx is ", idx)
	err = tx.Commit()
	if err != nil {
		panic("commit error: " + err.Error())
	}
	fmt.Println("commit success")
	d, err := q.Read("topic", 0)
	if err != nil {
		panic("read data error: " + err.Error())
	}
	fmt.Println("data is ", string(d))
}
