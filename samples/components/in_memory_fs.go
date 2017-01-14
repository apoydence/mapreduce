package components

import (
	"fmt"
	"sync"

	"github.com/apoydence/mapreduce"
)

type InMemoryFS struct {
	mu         sync.Mutex
	m          map[string]*inMemoryFile
	numOfNodes int
}

func NewInMemoryFS(numOfNodes int) mapreduce.FileSystem {
	return &InMemoryFS{
		m:          make(map[string]*inMemoryFile),
		numOfNodes: numOfNodes,
	}
}

func (f *InMemoryFS) Nodes(name string) ([]string, error) {
	var nodes []string
	for i := 0; i < f.numOfNodes; i++ {
		nodes = append(nodes, fmt.Sprintf("%d", i))
	}
	return nodes, nil
}

func (f *InMemoryFS) Length(name string) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	file, ok := f.m[name]
	if !ok {
		return 0, fmt.Errorf("file '%s' does not exists", name)
	}

	return uint64(len(file.data)), nil
}

func (f *InMemoryFS) CreateFile(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.m[name]; ok {
		return fmt.Errorf("file '%s' already exists", name)
	}

	f.m[name] = newInMemoryFile()

	return nil
}

func (f *InMemoryFS) ReadFile(name string, start, end uint64) (mapreduce.FileReader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	file, ok := f.m[name]
	if !ok {
		return nil, fmt.Errorf("file '%s' does not exists", name)
	}

	return file.Reader(start, end), nil
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
