package main

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/DanLavine/gonotify"
)

type message struct {
	bytes int
	err   error
}

// ThreadSafeBuffer can be used by any number of goroutines to safely call
// any of the provided functions.
type ThreadSafeBuffer struct {
	once *sync.Once
	done chan struct{}

	readLock *sync.Mutex
	ready    chan struct{}

	config Config

	notify *gonotify.Notify

	bufferLock    *sync.Mutex
	buffer        *bytes.Buffer
	bufferSize    uint64
	maxBufferSize uint64
}

// Create a new thread safe buffer
//
// PARAMS:
// * config - Thread configuration
func NewThreadSafeBuffer(config Config) *ThreadSafeBuffer {
	threadSafeBuffer := &ThreadSafeBuffer{
		once: new(sync.Once),
		done: make(chan struct{}),

		readLock: new(sync.Mutex),
		ready:    make(chan struct{}),

		config: config,

		notify: gonotify.New(),

		bufferLock: new(sync.Mutex),
		buffer:     new(bytes.Buffer),
	}

	return threadSafeBuffer
}

// Write is used to add data into the buffer and record the size of the buffer.
func (tsb *ThreadSafeBuffer) Write(b []byte) (int, error) {
	select {
	case <-tsb.done:
		return 0, &BuffErr{Op: "write", Err: fmt.Errorf("thread safe buffer is closed")}
	default:
		tsb.bufferLock.Lock()
		defer tsb.bufferLock.Unlock()

		if tsb.config.MaxBuffer && tsb.buffer.Len()+len(b) > tsb.config.MaxBufferSize {
			return 0, &BuffErr{Op: "write", Err: fmt.Errorf("write exceeds max buffer size")}
		}

		n, err := tsb.buffer.Write(b)
		tsb.notify.Add()
		return n, err
	}
}

// Read is uesd to remove data from the buffer. This blocks until there
// is enough data to be read the len(b), or the buffer is told to close
// and we reached out drain timeout
func (tsb *ThreadSafeBuffer) Read(b []byte) (int, error) {
	tsb.readLock.Lock()
	defer tsb.readLock.Unlock()

	for {
		select {
		case _, ok := <-tsb.notify.Ready():
			tsb.bufferLock.Lock()

			// we are closing the buffer
			if !ok {
				// check to see if we should drain
				if tsb.shouldDrain() {
					// ensure we can read everything since no more writes will come through on a closed buffer
					if tsb.buffer.Len() >= len(b) {
						n, err := tsb.buffer.Read(b)

						// there is still more data to be read so notify again
						if tsb.buffer.Len() != 0 {
							tsb.notify.Add()
						}

						tsb.bufferLock.Unlock()
						return n, err
					} else {
						tsb.bufferLock.Unlock()
						return 0, &BuffErr{Op: "read", Err: fmt.Errorf("Thread safe buffer is closed. Attempting to read more data than is in the buffer")}
					}
				}

				tsb.bufferLock.Unlock()
				return 0, &BuffErr{Op: "read", Err: fmt.Errorf("thread safe buffer is closed")}
			}

			// wait untill the buffer is full so we can read from it
			if tsb.buffer.Len() >= len(b) {
				n, err := tsb.buffer.Read(b)

				// there is still more data to be read so notify again
				if tsb.buffer.Len() != 0 {
					tsb.notify.Add()
				}

				tsb.bufferLock.Unlock()
				return n, err
			} else {
				// not reading yet so unlock to allow more writes
				tsb.bufferLock.Unlock()
			}
		}
	}
}

func (tsb *ThreadSafeBuffer) shouldDrain() bool {
	if tsb.config.DrainRead {
		tsb.bufferLock.Lock()
		defer tsb.bufferLock.Unlock()

		if tsb.buffer.Len() != 0 {
			return true
		}

		return false
	}

	// not draining
	return false
}

// Close can be used to block any further read and write to this buffer. Will
// also clean up any Read functions currently waiting
func (tsb *ThreadSafeBuffer) Close() {
	tsb.once.Do(func() {
		close(tsb.done)
		tsb.notify.Stop()
	})
}
