package mapreduce

import (
	"fmt"

	"golang.org/x/net/context"
)

type AlgorithmFetcher interface {
	Alg(name string, ctx context.Context) (alg Algorithm, err error)
}

type AlgFetcherMap map[string]Algorithm

func (f AlgFetcherMap) Alg(name string, ctx context.Context) (Algorithm, error) {
	alg, ok := f[name]
	if !ok {
		return Algorithm{}, fmt.Errorf("unkown algorithm: %s", name)
	}

	return alg, nil
}
