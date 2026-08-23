package utils

import (
	"io"
	"sync"
)

type SafeWriter struct {
	w  io.Writer
	mu *sync.Mutex
}

func (sw SafeWriter) Write(p []byte) (n int, err error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.w.Write(p)
}

func NewSafeWriter(w io.Writer, mus ...*sync.Mutex) (sw SafeWriter) {
	mu := new(sync.Mutex)
	if len(mus) != 0 {
		mu = mus[0]

	}
	return SafeWriter{w, mu}
}
