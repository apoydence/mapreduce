// mapreduce is used to run calculations on data the is available across several remote nodes.
// Each node that has some data of interst will run calculations against it.
package mapreduce

import (
	"io/ioutil"
	"log"
	"math/rand"

	"golang.org/x/net/context"
)

// Algorithm stores a Mapper and Reducer.
type Algorithm struct {
	Mapper
	Reducer
}

// MapReduceOption is used to configure a new MapReduce.
type MapReduceOption func(*MapReduce)

// Log is used to write debug logs.
type Log interface {
	Printf(format string, v ...interface{})
}

// WithLogger is used to set the given logger.
func WithLogger(l Log) MapReduceOption {
	return func(r *MapReduce) {
		r.log = l
	}
}

// MapReduce is used to invoke a Map/Reduce algorithm across data on various remote nodes.
//
// It should be created with New().
type MapReduce struct {
	fs         FileSystem
	network    Network
	algFetcher AlgorithmFetcher
	log        Log
}

// New returns a new MapReduce.
func New(fs FileSystem, network Network, algFetcher AlgorithmFetcher, opts ...MapReduceOption) MapReduce {
	r := MapReduce{
		fs:         fs,
		network:    network,
		algFetcher: algFetcher,
		log:        log.New(ioutil.Discard, "", 0),
	}

	for _, o := range opts {
		o(&r)
	}

	return r
}

// Calculate runs the given algorithm for the files returned from FileSystem for the given route and meta information.
// It uses the Network to run the calculations across the remote nodes that report having the given data.
func (r MapReduce) Calculate(route, algName string, ctx context.Context, meta []byte) (finalResult map[string][]byte, err error) {
	files, err := r.fs.Files(route, ctx, meta)
	if err != nil {
		return nil, err
	}

	errs := make(chan error, len(files))
	results := make(chan map[string][]byte, len(files))

	for fileName, ids := range files {
		// TODO: Balance load across nodes
		id := ids[rand.Intn(len(ids))]
		r.log.Printf("Start calculation for file %s on %s with algorithm %s", fileName, id, algName)
		go func(fileName, id string) {
			result, err := r.network.Execute(fileName, algName, id, ctx, meta)
			if err != nil {
				errs <- err
				return
			}

			results <- result
		}(fileName, id)
	}

	m := make(map[string][][]byte)
	for i := 0; i < len(files); i++ {
		select {
		case err := <-errs:
			return nil, err
		case result := <-results:
			for key, value := range result {
				m[key] = append(m[key], value)
			}
		case <-ctx.Done():
			break
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
