package mapreduce

import (
	"fmt"
	"io"

	"golang.org/x/net/context"
)

type Executor struct {
	algs map[string]Algorithm
	fs   FileSystem
}

func NewExecutor(algs map[string]Algorithm, fs FileSystem) *Executor {
	return &Executor{
		algs: algs,
		fs:   fs,
	}
}

func (e *Executor) Execute(fileName, algName string, ctx context.Context) (result map[string][]byte, err error) {
	alg, ok := e.algs[algName]
	if !ok {
		return nil, fmt.Errorf("unknown algorithm: %s", algName)
	}

	reader, err := e.fs.Reader(fileName, ctx)
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
