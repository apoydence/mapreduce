package mapreduce

type Mapper interface {
	Map(value []byte) (key string, output []byte, err error)
}

type MapFunc func(value []byte) (key string, output []byte, err error)

func (f MapFunc) Map(value []byte) (key string, output []byte, err error) {
	return f(value)
}
