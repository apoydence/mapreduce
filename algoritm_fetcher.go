package mapreduce

import "fmt"

// AlgorithmFetcher is used to store and fetch algorithms based on
// name and meta information.
type AlgorithmFetcher interface {
	Alg(name string, meta []byte) (alg Algorithm, err error)
}

// AlgFetcherMap implements AlgorithmFetcher.
type AlgFetcherMap map[string]Algorithm

// Alg returns the requested algorithm.
func (f AlgFetcherMap) Alg(name string, meta []byte) (Algorithm, error) {
	alg, ok := f[name]
	if !ok {
		return Algorithm{}, fmt.Errorf("unkown algorithm: %s", name)
	}

	return alg, nil
}
