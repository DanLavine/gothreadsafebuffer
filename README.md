# gothreadsafebuffer

A thread safe buffer that can be used by multiple clients to read and write to.

### Configuration

The buffer provides some basic [configuration](./config.go), including:
* Max Buffer Size - limits to make sure the buffer doesn't get to large
* Drain Options - ensure messages can be read from the buffer after closing
* Read Timeouts - ensure that read operations don't block forever
