package mapreduce

type Reducer interface {
	Reduce(value [][]byte) (reduced [][]byte)
}

type ReduceFunc func(value [][]byte) (reduced [][]byte)

func (f ReduceFunc) Reduce(value [][]byte) (reduced [][]byte) {
	return f(value)
}

type FinalReducer interface {
	FinalReduce(value [][]byte) (reduced [][]byte)
}

type FinalReduceFunc func(value [][]byte) (reduced [][]byte)

func (f FinalReduceFunc) FinalReduce(value [][]byte) (reduced [][]byte) {
	return f(value)
}
