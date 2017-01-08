package components

import (
	"io"
	"sync"
)

type inMemoryFile struct {
	mu   sync.Mutex
	data [][]byte
	idx  int
}

func newInMemoryFile() *inMemoryFile {
	return &inMemoryFile{}
}

func (f *inMemoryFile) Length() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return uint64(len(f.data))
}

func (f *inMemoryFile) Read() ([]byte, error) {
	f.mu.Lock()
	defer func() {
		f.idx++
		f.mu.Unlock()
	}()

	if f.idx >= len(f.data) {
		return nil, io.EOF
	}

	return f.data[f.idx], nil
}

func (f *inMemoryFile) Write(data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = append(f.data, data)

	return nil
}
