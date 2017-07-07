package mapreduce

// Mapper maps data ([]byte) to key (string).
type Mapper interface {
	// Map maps data to keys. It filters out the value if the
	// returned key has a length of 0. A non-nil error will abort
	// the operation.
	Map(value []byte) (key string, output []byte, err error)
}

// MapFunc wraps a function into a Mapper.
type MapFunc func(value []byte) (key string, output []byte, err error)

// Map implements the Mapper interface.
func (f MapFunc) Map(value []byte) (key string, output []byte, err error) {
	return f(value)
}
