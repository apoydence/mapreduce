package mapreduce

import "golang.org/x/net/context"

type FileSystem interface {
	Files(route string, ctx context.Context) (files map[string][]string, err error)
	Reader(file string, ctx context.Context) (reader func() (data []byte, err error), err error)
}
