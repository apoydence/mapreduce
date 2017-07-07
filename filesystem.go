package mapreduce

import "golang.org/x/net/context"

// FileSystem is used to store data local to the node.
type FileSystem interface {
	// Files returns a set of files that matches the given route and meta information. A non-nil
	// error will exit the operation.
	Files(route string, ctx context.Context, meta []byte) (files map[string][]string, err error)

	// Reader returns a reader to read data from the given file.
	Reader(file string, ctx context.Context, meta []byte) (reader func() (data []byte, err error), err error)
}
