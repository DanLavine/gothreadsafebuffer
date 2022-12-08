package gothreadsafebuffer

import "fmt"

type BuffErr struct {
	Op  string
	Err error
}

func (be *BuffErr) Error() string {
	return fmt.Sprintf("Failed buffer %s: %s", be.Op, be.Err.Error())
}
