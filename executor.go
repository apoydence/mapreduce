package mapreduce

import (
	"io"

	"golang.org/x/net/context"
)

// Executor is used to apply algorithms to data. It maps and then returns the
// reduced data from the given algorithm.
//
// An Executor has to be created with NewExecutor().
type Executor struct {
	algFetcher AlgorithmFetcher
	fs         FileSystem
}

// NewExecutor returns a new Executor.
func NewExecutor(algFetcher AlgorithmFetcher, fs FileSystem) *Executor {
	return &Executor{
		algFetcher: algFetcher,
		fs:         fs,
	}
}

// Execute maps local data from the file (fileName) via the mapper given from the algorithm (algName) and reduces it.
func (e *Executor) Execute(fileName, algName string, ctx context.Context, meta []byte) (result map[string][]byte, err error) {
	alg, err := e.algFetcher.Alg(algName, meta)
	if err != nil {
		return nil, err
	}

	reader, err := e.fs.Reader(fileName, ctx, meta)
	if err != nil {
		return nil, err
	}

	m, err := e.consumeFile(alg, reader)
	if err != nil {
		return nil, err
	}

	result = make(map[string][]byte)
	for key, values := range m {
		for len(values) > 1 {
			values, err = alg.Reduce(values)
			if err != nil {
				return nil, err
			}
		}

		if len(values) == 0 {
			result[key] = nil
			continue
		}

		result[key] = values[0]
	}

	return result, nil
}

// consumeFile maps data from the reader to the according keys.
func (e *Executor) consumeFile(alg Mapper, reader func() ([]byte, error)) (map[string][][]byte, error) {
	m := make(map[string][][]byte)
	for {
		data, err := reader()
		if err == io.EOF {
			return m, nil
		}

		if err != nil {
			return nil, err
		}

		key, data, err := alg.Map(data)
		if err != nil {
			return nil, err
		}

		if len(key) == 0 {
			continue
		}

		m[key] = append(m[key], data)
	}
}
