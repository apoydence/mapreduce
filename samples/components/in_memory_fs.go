package components

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"

	"golang.org/x/net/context"

	"github.com/apoydence/mapreduce"
)

type InMemoryFS struct {
	numOfNodes int
	size       int
}

func NewInMemoryFS(numOfNodes, size int) mapreduce.FileSystem {
	return &InMemoryFS{
		numOfNodes: numOfNodes,
		size:       size,
	}
}

func (f *InMemoryFS) Files(route string, ctx context.Context) (map[string][]string, error) {
	var nodeIDs []string
	for i := 0; i < f.numOfNodes; i++ {
		nodeIDs = append(nodeIDs, fmt.Sprint(i))
	}

	return map[string][]string{
		"a": nodeIDs,
		"b": nodeIDs,
		"c": nodeIDs,
	}, nil

}

func (f *InMemoryFS) Reader(name string, ctx context.Context) (func() ([]byte, error), error) {
	var count int
	return func() ([]byte, error) {
		count++
		if count > f.size {
			return nil, io.EOF
		}

		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, uint64(rand.Int63()))
		return data, nil
	}, nil
}
