package mapreduce

import "golang.org/x/net/context"

type Network interface {
	Execute(file, algName, nodeID string, ctx context.Context) (result map[string][]byte, err error)
}
