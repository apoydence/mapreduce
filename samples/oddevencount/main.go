package main

import (
	"encoding/binary"
	"fmt"
	"log"

	"github.com/apoydence/mapreduce"
	"github.com/apoydence/mapreduce/samples/components"
)

func main() {
	fileSystem := components.NewInMemoryFS()
	populateFile("some-name", fileSystem)

	chain := mapreduce.Build(mapreduce.MapFunc(func(data []byte) (key []byte, ok bool) {
		i := binary.LittleEndian.Uint32(data)
		if i%2 == 0 {
			return []byte{1}, true
		}

		return []byte{0}, true
	})).Reduce(mapreduce.ReduceFunc(func(data [][]byte) [][]byte {
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, uint32(len(data)))
		return [][]byte{b}
	})).Map(mapreduce.MapFunc(func(data []byte) (key []byte, ok bool) {
		fmt.Println(binary.LittleEndian.Uint32(data))
		return nil, false
	}))

	mapReduce := mapreduce.New(fileSystem, nil, chain)

	mapReduce.Calculate("some-name")
}

func populateFile(name string, fs mapreduce.FileSystem) {
	if err := fs.CreateFile(name); err != nil {
		log.Fatalf("Failed to create file %s: %s", name, err)
	}

	writer, err := fs.WriteToFile(name)
	if err != nil {
		log.Fatalf("Failed to fetch writer for %s: %s", name, err)
	}

	for i := 0; i < 1000; i++ {
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, uint32(i))
		if err := writer.Write(b); err != nil {
			log.Fatalf("Failed to write to %s: %s", name, err)
		}
	}
}
