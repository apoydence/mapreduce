package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/apoydence/mapreduce"
	"github.com/apoydence/mapreduce/samples/components"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	algs := map[string]mapreduce.Algorithm{
		"oddeven": {
			Mapper: mapreduce.MapFunc(func(data []byte) (key string, output []byte, err error) {
				i := binary.LittleEndian.Uint32(data)
				one := make([]byte, 8)
				binary.LittleEndian.PutUint64(one, 1)
				if i%2 == 0 {
					return "even", one, nil
				}

				return "odd", one, nil

			}),
			Reducer: mapreduce.ReduceFunc(func(data [][]byte) ([][]byte, error) {
				// Sum
				var sum uint32
				for _, d := range data {
					sum += binary.LittleEndian.Uint32(d)
				}

				b := make([]byte, 4)
				binary.LittleEndian.PutUint32(b, sum)
				return [][]byte{b}, nil
			}),
		},
	}

	size := rand.Intn(100000)
	println(size)
	fileSystem := components.NewInMemoryFS(3, size)
	executor := mapreduce.NewExecutor(mapreduce.AlgFetcherMap(algs), fileSystem)
	network := components.NewInProcessNetwork(executor)

	mapReduce := mapreduce.New(fileSystem, network, mapreduce.AlgFetcherMap(algs))

	results, err := mapReduce.Calculate("some-name", "oddeven", context.Background(), nil)
	if err != nil {
		log.Fatalf("Failed to calculate: %s", err)
	}

	for key, data := range results {
		fmt.Println(key, "->", binary.LittleEndian.Uint32(data))
	}
}
