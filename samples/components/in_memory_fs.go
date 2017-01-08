package components

import (
	"fmt"
	"sync"

	"github.com/apoydence/mapreduce"
)

type InMemoryFS struct {
	mu sync.Mutex
	m  map[string]*inMemoryFile
}

func NewInMemoryFS() mapreduce.FileSystem {
	return &InMemoryFS{
		m: make(map[string]*inMemoryFile),
	}
}

// type FileReader interface {
// 	Length() uint64
// 	Read() ([]byte, error)
// }

// type FileWriter interface {
// 	Write(data []byte) error
// }

// type FileSystem interface {
// 	ReadFile(name string) (FileReader, error)
// 	WriteToFile(name string) (FileWriter, error)
// }

func (f *InMemoryFS) CreateFile(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.m[name]; ok {
		return fmt.Errorf("file '%s' already exists", name)
	}

	f.m[name] = newInMemoryFile()

	return nil
}

func (f *InMemoryFS) ReadFile(name string) (mapreduce.FileReader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	file, ok := f.m[name]
	if !ok {
		return nil, fmt.Errorf("file '%s' does not exists", name)
	}

	return file, nil
}

func (f *InMemoryFS) WriteToFile(name string) (mapreduce.FileWriter, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	file, ok := f.m[name]
	if !ok {
		return nil, fmt.Errorf("file '%s' does not exists", name)
	}

	return file, nil

}
