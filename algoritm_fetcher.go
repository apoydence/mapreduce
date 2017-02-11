package mapreduce

import "fmt"

type AlgorithmFetcher interface {
	Alg(name string) (alg Algorithm, err error)
}

type AlgFetcherMap map[string]Algorithm

func (f AlgFetcherMap) Alg(name string) (Algorithm, error) {
	alg, ok := f[name]
	if !ok {
		return Algorithm{}, fmt.Errorf("unkown algorithm: %s", name)
	}

	return alg, nil
}
