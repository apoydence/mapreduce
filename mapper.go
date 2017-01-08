package mapreduce

type Mapper interface {
	Map(value []byte) (key []byte, ok bool)
}

type MapFunc func(value []byte) (key []byte, ok bool)

func (f MapFunc) Map(value []byte) (key []byte, ok bool) {
	return f(value)
}
