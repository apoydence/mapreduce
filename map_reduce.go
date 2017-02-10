package mapreduce

import (
	"fmt"
	"math/rand"

	"golang.org/x/net/context"
)

type Algorithm struct {
	Mapper
	Reducer
}

type MapReduce struct {
	fs       FileSystem
	network  Network
	reducers map[string]Algorithm
}

func New(fs FileSystem, network Network, reducers map[string]Algorithm) MapReduce {
	return MapReduce{
		fs:       fs,
		network:  network,
		reducers: reducers,
	}
}

func (r MapReduce) Calculate(route, algName string, ctx context.Context) (finalResult map[string][]byte, err error) {
	files, err := r.fs.Files(route, ctx)
	if err != nil {
		return nil, err
	}

	m := make(map[string][][]byte)
	for fileName, ids := range files {
		// TODO: Balance load across nodes
		id := ids[rand.Intn(len(ids))]
		result, err := r.network.Execute(fileName, algName, id, ctx)
		if err != nil {
			return nil, err
		}

		for key, value := range result {
			m[key] = append(m[key], value)
		}
	}

	finalResult = make(map[string][]byte)
	reducer, ok := r.reducers[algName]
	if !ok {
		return nil, fmt.Errorf("Unknown algorithm: %s", algName)
	}

	for key, results := range m {
		// TODO: Circuit break?
		for len(results) > 1 {
			results, err = reducer.Reduce(results)
			if err != nil {
				return nil, err
			}
		}
		finalResult[key] = results[0]
	}

	return finalResult, nil
}
