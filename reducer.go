package mapreduce

// Reducer reduces a slice data points into a smaller set.
type Reducer interface {
	// Reduce is called with marshalled data either from a mapper or
	// a reducer. It will be invoked until a slice of length 1
	// or a non-nil error is returned. Reduce is expected to know how
	// to marshal and unmarshal the given data.
	Reduce(value [][]byte) (reduced [][]byte, err error)
}

// ReduceFunc wraps a function into a Reducer.
type ReduceFunc func(value [][]byte) (reduced [][]byte, err error)

// Reduce implements the Reducer interface.
func (f ReduceFunc) Reduce(value [][]byte) (reduced [][]byte, err error) {
	return f(value)
}
