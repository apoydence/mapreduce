package mapreduce

type Reducer interface {
	Reduce(value [][]byte) (reduced [][]byte, err error)
}

type ReduceFunc func(value [][]byte) (reduced [][]byte, err error)

func (f ReduceFunc) Reduce(value [][]byte) (reduced [][]byte, err error) {
	return f(value)
}
