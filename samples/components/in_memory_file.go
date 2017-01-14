package components

import (
	"io"
	"sync"
)

type inMemoryFile struct {
	mu   sync.Mutex
	data [][]byte
}

func newInMemoryFile() *inMemoryFile {
	return &inMemoryFile{}
}

func (f *inMemoryFile) Reader(start, end uint64) *inMemoryFileReader {
	return &inMemoryFileReader{file: f, idx: start, end: end}
}

func (f *inMemoryFile) Length() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return uint64(len(f.data))
}

func (f *inMemoryFile) Write(data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = append(f.data, data)

	return nil
}

type inMemoryFileReader struct {
	file *inMemoryFile
	idx  uint64
	end  uint64
}

func (r *inMemoryFileReader) Read() ([]byte, error) {
	r.file.mu.Lock()
	defer func() {
		r.idx++
		r.file.mu.Unlock()
	}()

	if r.idx >= r.end || r.idx >= uint64(len(r.file.data)) {
		return nil, io.EOF
	}

	return r.file.data[r.idx], nil
}
