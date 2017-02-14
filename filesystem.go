package mapreduce

import "golang.org/x/net/context"

type FileSystem interface {
	Files(route string, ctx context.Context, meta []byte) (files map[string][]string, err error)
	Reader(file string, ctx context.Context, meta []byte) (reader func() (data []byte, err error), err error)
}
