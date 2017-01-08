package mapreduce

type Reducer interface {
	Reduce(value [][]byte) (reduced [][]byte)
}

type ReduceFunc func(value [][]byte) (reduced [][]byte)

func (f ReduceFunc) Reduce(value [][]byte) (reduced [][]byte) {
	return f(value)
}
