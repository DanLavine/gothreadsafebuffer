package gothreadsafebuffer_test

import (
	"sync"
	"testing"
	"time"

	"github.com/DanLavine/gothreadsafebuffer"
	. "github.com/onsi/gomega"
)

func Test_Write(t *testing.T) {
	g := NewGomegaWithT(t)
	unlimitedConfig := gothreadsafebuffer.UnlimitedBuffer()

	t.Run("can read valid data", func(t *testing.T) {
		t.Parallel()
		threadSafeBuffer := gothreadsafebuffer.NewThreadSafeBuffer(unlimitedConfig)

		n, err := threadSafeBuffer.Write([]byte(`hello`))
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(n).To(Equal(5))

		buffer := make([]byte, 5)
		n, err = threadSafeBuffer.Read(buffer)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(n).To(Equal(5))
		g.Expect(buffer).To(Equal([]byte(`hello`)))

		threadSafeBuffer.Close()
	})

	t.Run("returns an error when the buffer is closed", func(t *testing.T) {
		t.Parallel()
		threadSafeBuffer := gothreadsafebuffer.NewThreadSafeBuffer(unlimitedConfig)
		threadSafeBuffer.Close()

		n, err := threadSafeBuffer.Write([]byte(`hello`))
		g.Expect(err).To(HaveOccurred())
		g.Expect(err.Error()).To(Equal("Failed buffer write: Thread safe buffer is closed"))
		g.Expect(n).To(Equal(0))
	})

	t.Run("returns an error if the write is larger than the buffer size", func(t *testing.T) {
		t.Parallel()
		threadSafeBuffer := gothreadsafebuffer.NewThreadSafeBuffer(gothreadsafebuffer.Config{MaxBuffer: true, MaxBufferSize: 0})

		n, err := threadSafeBuffer.Write([]byte(`hello`))
		g.Expect(err).To(HaveOccurred())
		g.Expect(err.Error()).To(Equal("Failed buffer write: write exceeds max buffer size"))
		g.Expect(n).To(Equal(0))

		threadSafeBuffer.Close()
	})
}

func Test_Read(t *testing.T) {
	g := NewGomegaWithT(t)
	unlimitedConfig := gothreadsafebuffer.UnlimitedBuffer()

	t.Run("can be called multiple times per write", func(t *testing.T) {
		t.Parallel()
		threadSafeBuffer := gothreadsafebuffer.NewThreadSafeBuffer(unlimitedConfig)

		n, err := threadSafeBuffer.Write([]byte(`hello`))
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(n).To(Equal(5))

		buffer := make([]byte, 1)
		n, err = threadSafeBuffer.Read(buffer)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(n).To(Equal(1))
		g.Expect(buffer).To(Equal([]byte(`h`)))

		buffer = make([]byte, 4)
		n, err = threadSafeBuffer.Read(buffer)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(n).To(Equal(4))
		g.Expect(buffer).To(Equal([]byte(`ello`)))

		threadSafeBuffer.Close()
	})

	t.Run("returns an error if the pipe is already closed and drained", func(t *testing.T) {
		t.Parallel()
		threadSafeBuffer := gothreadsafebuffer.NewThreadSafeBuffer(unlimitedConfig)
		threadSafeBuffer.Close()

		buffer := make([]byte, 5)
		n, err := threadSafeBuffer.Read(buffer)
		g.Expect(err).To(HaveOccurred())
		g.Expect(err.Error()).To(Equal("Failed buffer read: Thread safe buffer is closed"))
		g.Expect(n).To(Equal(0))
	})

	t.Run("returns an error if read takes longer than config.ReadTimeout", func(t *testing.T) {
		t.Parallel()
		cfg := unlimitedConfig
		cfg.ReadTimeout = time.Nanosecond

		threadSafeBuffer := gothreadsafebuffer.NewThreadSafeBuffer(cfg)

		// no write, so this will block and time out
		buffer := make([]byte, 5)
		n, err := threadSafeBuffer.Read(buffer)
		g.Expect(err).To(HaveOccurred())
		g.Expect(err.Error()).To(Equal("Failed buffer read: Failed to read in time"))
		g.Expect(n).To(Equal(0))

		threadSafeBuffer.Close()
	})

	t.Run("when draining", func(t *testing.T) {
		t.Parallel()
		t.Run("we can read the full buffer", func(t *testing.T) {
			threadSafeBuffer := gothreadsafebuffer.NewThreadSafeBuffer(unlimitedConfig)

			n, err := threadSafeBuffer.Write([]byte(`hello`))
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(n).To(Equal(5))

			threadSafeBuffer.Close()

			// first read should be fine
			buffer := make([]byte, 5)
			n, err = threadSafeBuffer.Read(buffer)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(n).To(Equal(5))
			g.Expect(buffer).To(Equal([]byte(`hello`)))

			// 2nd read should trigger an error
			buffer = make([]byte, 5)
			n, err = threadSafeBuffer.Read(buffer)
			g.Expect(err).To(HaveOccurred())
			g.Expect(err.Error()).To(Equal("Failed buffer read: Thread safe buffer is closed"))
			g.Expect(n).To(Equal(0))
		})

		t.Run("returns an error if trying to read more than the buffer", func(t *testing.T) {
			t.Parallel()
			threadSafeBuffer := gothreadsafebuffer.NewThreadSafeBuffer(unlimitedConfig)

			n, err := threadSafeBuffer.Write([]byte(`hello`))
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(n).To(Equal(5))

			threadSafeBuffer.Close()

			// read is to large, buffer only has 5 bytes!
			buffer := make([]byte, 100)
			n, err = threadSafeBuffer.Read(buffer)
			g.Expect(err).To(HaveOccurred())
			g.Expect(err.Error()).To(Equal("Failed buffer read: Thread safe buffer is closed. Attempting to read more data than is in the buffer"))
			g.Expect(n).To(Equal(0))
		})

		t.Run("returns an error if we are pass the config.DrainTime", func(t *testing.T) {
			t.Parallel()
			cfg := unlimitedConfig
			cfg.DrainTime = time.Nanosecond

			threadSafeBuffer := gothreadsafebuffer.NewThreadSafeBuffer(cfg)

			n, err := threadSafeBuffer.Write([]byte(`hello`))
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(n).To(Equal(5))

			// just be safe and sleep before we try to read
			threadSafeBuffer.Close()
			time.Sleep(time.Millisecond)

			buffer := make([]byte, 5)
			n, err = threadSafeBuffer.Read(buffer)
			g.Expect(err).To(HaveOccurred())
			g.Expect(err.Error()).To(Equal("Failed buffer read: Thread safe buffer is closed"))
			g.Expect(n).To(Equal(0))
		})
	})

	t.Run("when not configured to drain", func(t *testing.T) {
		t.Run("returns an error if closed", func(t *testing.T) {
			t.Parallel()
			cfg := unlimitedConfig
			cfg.DrainRead = false
			threadSafeBuffer := gothreadsafebuffer.NewThreadSafeBuffer(cfg)

			n, err := threadSafeBuffer.Write([]byte(`hello`))
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(n).To(Equal(5))

			threadSafeBuffer.Close()

			buffer := make([]byte, 1)
			n, err = threadSafeBuffer.Read(buffer)
			g.Expect(err).To(HaveOccurred())
			g.Expect(err.Error()).To(Equal("Failed buffer read: Thread safe buffer is closed"))
			g.Expect(n).To(Equal(0))
		})
	})
}

func Test_Parallelism(t *testing.T) {
	t.Parallel()
	g := NewGomegaWithT(t)
	startWrite := make(chan struct{})
	startRead := make(chan struct{})
	unlimitedConfig := gothreadsafebuffer.UnlimitedBuffer()

	threadSafeBuffer := gothreadsafebuffer.NewThreadSafeBuffer(unlimitedConfig)

	for i := 0; i < 5; i++ {
		var char byte

		switch i {
		case 0:
			char = 'a'
		case 1:
			char = 'b'
		case 2:
			char = 'c'
		case 3:
			char = 'd'
		case 4:
			char = 'e'
		}

		go func(val byte) {
			<-startWrite
			buffer := make([]byte, 4096)

			for index, _ := range buffer {
				buffer[index] = val
			}

			_, err := threadSafeBuffer.Write(buffer)
			g.Expect(err).ToNot(HaveOccurred())
		}(char)
	}

	close(startWrite)

	wg := new(sync.WaitGroup)
	for i := 0; i < 5; i++ {
		wg.Add(1)

		var char byte
		switch i {
		case 0:
			char = 'a'
		case 1:
			char = 'b'
		case 2:
			char = 'c'
		case 3:
			char = 'd'
		case 4:
			char = 'e'
		}

		go func(val byte) {
			defer wg.Done()
			<-startRead
			buffer := make([]byte, 4096)

			_, err := threadSafeBuffer.Read(buffer)
			g.Expect(err).ToNot(HaveOccurred())

			initialChar := buffer[0]
			for _, val := range buffer {
				g.Expect(val).To(Equal(initialChar))
			}
		}(char)
	}

	close(startRead)

	wg.Wait()
}
