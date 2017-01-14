package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
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
	fs := components.NewInMemoryFS(100)
	populateFile("some-name", fs)

	chain := mapreduce.Build(mapreduce.MapFunc(func(data []byte) (key []byte, ok bool) {
		var pt Point
		err := json.Unmarshal(data, &pt)
		if err != nil {
			log.Fatal(err)
		}

		t := time.Unix(0, pt.Time).Truncate(time.Second).Unix()
		return []byte(fmt.Sprintf("%d", t)), true

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

			t := time.Unix(0, pt.Time).Truncate(time.Second).Unix()
			if maxPt.Time != 0 && maxPt.Time != t {
				log.Panicf("ADS %d %d", maxPt.Time, t)
			}

			maxPt.Time = t
		}

		results, _ := json.Marshal(maxPt)
		return [][]byte{results}
	}))

	mapReduce := mapreduce.New(fs, nil, chain)

	start := time.Now()
	results, err := mapReduce.Calculate("some-name")
	if err != nil {
		log.Fatalf("Failed to calculate: %s", err)
	}
	duration := time.Since(start)

	var pts []Point
	for _, key := range results.ChildrenKeys() {
		child := results.Child(key)
		value, _ := child.Leaf()

		var pt Point
		if err := json.Unmarshal(value, &pt); err != nil {
			log.Fatalf("unable to unmarshal: %s", err)
		}

		t, err := strconv.ParseUint(string(key), 10, 64)
		if err != nil {
			log.Fatalf("Failed to parse time %s: %s", string(key), err)
		}

		pt.Time = int64(t)

		pts = append(pts, pt)
	}

	sort.Sort(points(pts))

	for _, pt := range pts {
		fmt.Printf("%d -> %#v\n", pt.Time, pt)
	}
	fmt.Printf("calculation took %s\n", duration)
}

func populateFile(name string, fs mapreduce.FileSystem) {
	if err := fs.CreateFile(name); err != nil {
		log.Fatalf("Failed to create file %s: %s", name, err)
	}

	writer, err := fs.WriteToFile(name)
	if err != nil {
		log.Fatalf("Failed to fetch writer for %s: %s", name, err)
	}

	for i := 0; i < 1000000; i++ {
		pt := Point{
			Time: time.Now().Truncate(time.Second).Add(1 * time.Duration(i) * time.Millisecond).UnixNano(),
			X:    float64(i) * 50,
			Y:    float64(i) * 20,
		}

		b, _ := json.Marshal(pt)
		if err := writer.Write(b); err != nil {
			log.Fatalf("Failed to write to %s: %s", name, err)
		}
	}
}

type points []Point

func (p points) Len() int {
	return len(p)
}

func (p points) Less(i, j int) bool {
	return p[i].Time < p[j].Time
}

func (p points) Swap(i, j int) {
	tmp := p[i]
	p[i] = p[j]
	p[j] = tmp
}
