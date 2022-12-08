package main

import "fmt"

type BuffErr struct {
	Op  string
	Err error
}

func (be *BuffErr) Error() string {
	return fmt.Sprintf("Failed %s to pipe. %s", be.Op, be.Err.Error())
}
