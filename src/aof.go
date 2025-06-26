package main

import (
	"bufio"
	"os"
	"sync"
	"time"
)

type Aof struct {
	file     *os.File
	writer   *bufio.Writer
	writeCh  chan []byte
	doneCh   chan struct{}
	wg       sync.WaitGroup
	flushDur time.Duration
}

func NewAof(path string) (*Aof, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	aof := &Aof{
		file:     file,
		writer:   bufio.NewWriterSize(file, 4096),
		writeCh:  make(chan []byte, 1024),
		doneCh:   make(chan struct{}),
		flushDur: 250 * time.Millisecond,
	}

	aof.wg.Add(1)
	go aof.loop()

	return aof, nil
}

// loop handles background writes and periodic flushing
func (a *Aof) loop() {
	defer a.wg.Done()
	ticker := time.NewTicker(a.flushDur)
	defer ticker.Stop()

	for {
		select {
		case data := <-a.writeCh:
			a.writer.Write(data)

		case <-ticker.C:
			a.writer.Flush()

		case <-a.doneCh:
			// drain remaining items
			for {
				select {
				case data := <-a.writeCh:
					a.writer.Write(data)
				default:
					a.writer.Flush()
					a.file.Sync()
					return
				}
			}
		}
	}
}

func (a *Aof) Write(v Value) error {
	data := v.Marshal()
	select {
	case a.writeCh <- data:
		return nil
	default:
		return os.ErrInvalid
	}
}

func (a *Aof) Close() error {
	close(a.doneCh)
	a.wg.Wait()
	return a.file.Close()
}
