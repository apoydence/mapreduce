package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/apoydence/mapreduce"
	"github.com/apoydence/mapreduce/samples/components"
)

type Point struct {
	Time int64
	X    float64
	Y    float64
}

func main() {
	fs := components.NewInMemoryFS()
	populateFile("some-name", fs)

	chain := mapreduce.Build(mapreduce.MapFunc(func(data []byte) (key []byte, ok bool) {
		var pt Point
		err := json.Unmarshal(data, &pt)
		if err != nil {
			log.Fatal(err)
		}

		t := time.Unix(0, pt.Time).Truncate(time.Second).Unix()
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(t))

		return b, true
	})).FinalReduce(mapreduce.FinalReduceFunc(func(data [][]byte) [][]byte {
		var maxPt Point
		for _, d := range data {
			var pt Point
			err := json.Unmarshal(d, &pt)
			if err != nil {
				log.Fatal(err)
			}

			if pt.X > maxPt.X {
				maxPt.X = pt.X
			}

			if pt.Y > maxPt.Y {
				maxPt.Y = pt.Y
			}
		}

		results, _ := json.Marshal(maxPt)
		return [][]byte{results}
	}))

	mapReduce := mapreduce.New(fs, nil, chain)

	results, err := mapReduce.Calculate("some-name")
	if err != nil {
		log.Fatalf("Failed to calculate: %s", err)
	}

	for _, key := range results.ChildrenKeys() {
		child := results.Child(key)
		value, _ := child.Leaf()

		var pt Point
		if err := json.Unmarshal(value, &pt); err != nil {
			log.Fatalf("unable to unmarshal: %s", err)
		}
		t := binary.LittleEndian.Uint64(key)
		pt.Time = int64(t)

		fmt.Printf("%d -> %#v\n", t, pt)
	}
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
		pt := Point{
			Time: time.Now().Add(10 * time.Duration(i) * time.Millisecond).UnixNano(),
			X:    float64(i) * 50,
			Y:    float64(i) * 20,
		}

		b, _ := json.Marshal(pt)
		if err := writer.Write(b); err != nil {
			log.Fatalf("Failed to write to %s: %s", name, err)
		}
	}
}
