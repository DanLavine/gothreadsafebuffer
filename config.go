package gothreadsafebuffer

import "time"

type Config struct {
	// When set to true, will use the MaxBufferSize. Otherwise it will be ignored
	MaxBuffer bool
	// max size of the buffer. if this size was to be exceeded on writes, thrown an error instead
	MaxBufferSize int

	// after a Close() call if true. Will allow Read to be called untill the buffer is drained
	DrainRead bool

	// How long it should take for a read operation before reporting an error.
	// To have this be infinite set this to 0
	ReadTimeout time.Duration

	// how long to wait for Read() operations to drain before just reporting errors
	// To have this be infinite set this to 0
	DrainTime time.Duration
}

func UnlimitedBuffer() Config {
	return Config{
		MaxBuffer: false,
		DrainRead: true,
	}
}
