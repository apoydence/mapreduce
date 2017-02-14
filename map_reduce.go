package mapreduce

import (
	"log"
	"math/rand"

	"golang.org/x/net/context"
)

type Algorithm struct {
	Mapper
	Reducer
}

type MapReduce struct {
	fs         FileSystem
	network    Network
	algFetcher AlgorithmFetcher
}

func New(fs FileSystem, network Network, algFetcher AlgorithmFetcher) MapReduce {
	return MapReduce{
		fs:         fs,
		network:    network,
		algFetcher: algFetcher,
	}
}

func (r MapReduce) Calculate(route, algName string, ctx context.Context, meta []byte) (finalResult map[string][]byte, err error) {
	files, err := r.fs.Files(route, ctx, meta)
	if err != nil {
		return nil, err
	}

	m := make(map[string][][]byte)
	for fileName, ids := range files {
		// TODO: Balance load across nodes
		id := ids[rand.Intn(len(ids))]
		log.Printf("Start calculation for file %s on %s with algorithm %s", fileName, id, algName)

		result, err := r.network.Execute(fileName, algName, id, ctx, meta)
		if err != nil {
			return nil, err
		}

		for key, value := range result {
			m[key] = append(m[key], value)
		}
	}

	finalResult = make(map[string][]byte)
	reducer, err := r.algFetcher.Alg(algName, meta)
	if err != nil {
		return nil, err
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
