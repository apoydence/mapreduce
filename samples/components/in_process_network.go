package components

import (
	"golang.org/x/net/context"

	"github.com/apoydence/mapreduce"
)

type InProcessNetwork struct {
	e *mapreduce.Executor
}

func NewInProcessNetwork(e *mapreduce.Executor) *InProcessNetwork {
	return &InProcessNetwork{
		e: e,
	}
}

func (n *InProcessNetwork) Execute(file, algName, nodeID string, ctx context.Context, meta []byte) (result map[string][]byte, err error) {
	return n.e.Execute(file, algName, ctx, meta)
}
