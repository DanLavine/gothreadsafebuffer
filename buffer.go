package gothreadsafebuffer

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/DanLavine/gonotify"
)

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

	drainTime time.Time
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
	tsb.bufferLock.Lock()
	defer tsb.bufferLock.Unlock()

	select {
	case <-tsb.done:
		return 0, &BuffErr{Op: "write", Err: fmt.Errorf("Thread safe buffer is closed")}
	default:
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

	if tsb.config.ReadTimeout != 0 {
		ticker := time.NewTicker(tsb.config.ReadTimeout)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				return 0, &BuffErr{Op: "read", Err: fmt.Errorf("Failed to read in time")}
			case _, ok := <-tsb.notify.Ready():
				n, err := tsb.readLoop(b, !ok)
				if err != nil {
					return n, err
				}

				if n != 0 {
					return n, err
				}
			}
		}
	} else {
		for {
			select {
			case _, ok := <-tsb.notify.Ready():
				n, err := tsb.readLoop(b, !ok)
				if err != nil {
					return n, err
				}

				if n != 0 {
					return n, err
				}
			}
		}
	}
}

// function to loop over waiting for the data in the buffer, or an error
func (tsb *ThreadSafeBuffer) readLoop(b []byte, draining bool) (int, error) {
	tsb.bufferLock.Lock()
	defer tsb.bufferLock.Unlock()

	// we are closing the buffer
	select {
	case <-tsb.done:
		// check to see if we should drain
		if tsb.shouldDrain() {
			// ensure we can read everything since no more writes will come through on a closed buffer
			if tsb.buffer.Len() >= len(b) {
				n, err := tsb.buffer.Read(b)

				// there is still more data to be read so notify again
				if tsb.buffer.Len() != 0 {
					tsb.notify.Add()
				}

				return n, err
			} else {
				return 0, &BuffErr{Op: "read", Err: fmt.Errorf("Thread safe buffer is closed. Attempting to read more data than is in the buffer")}
			}
		}

		return 0, &BuffErr{Op: "read", Err: fmt.Errorf("Thread safe buffer is closed")}
	default:
		// wait untill the buffer is full so we can read from it
		if tsb.buffer.Len() >= len(b) {
			n, err := tsb.buffer.Read(b)

			// there is still more data to be read so notify again
			if tsb.buffer.Len() != 0 {
				tsb.notify.Add()
			}

			return n, err
		}

		return 0, nil
	}
}

func (tsb *ThreadSafeBuffer) shouldDrain() bool {
	if tsb.config.DrainRead {
		if tsb.config.DrainTime != 0 && time.Since(tsb.drainTime) > tsb.config.DrainTime {
			// we took to long to drain, so return false
			return false
		}

		if tsb.buffer.Len() != 0 {
			// buffer still has data, should drain
			return true
		}
	}

	// should not drain
	return false
}

// Close can be used to block any further read and write to this buffer. Will
// also clean up any Read functions currently waiting and cleanup goroutines manged
// by the thread safe buffer
func (tsb *ThreadSafeBuffer) Close() {
	tsb.once.Do(func() {
		// don't want to close the buffer in the middle of a write operation
		tsb.bufferLock.Lock()
		defer tsb.bufferLock.Unlock()

		tsb.notify.Stop()
		close(tsb.done)

		if tsb.config.DrainTime != 0 {
			tsb.drainTime = time.Now()
		}
	})
}
