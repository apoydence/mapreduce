package mapreduce

import "golang.org/x/net/context"

// Network is used to execute commands on remote node.
type Network interface {
	// Execute is invoked to run calculations for a file (file) on a remote node (nodeID) with the given algorithm
	// (algName). Any necessary information can be encoded into meta. The context (ctx) is used for lifecycle
	// management.
	Execute(file, algName, nodeID string, ctx context.Context, meta []byte) (result map[string][]byte, err error)
}
