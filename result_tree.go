package mapreduce

type Result struct {
	Key      []byte
	value    []byte
	children map[string]*Result
}

func newResult() *Result {
	return &Result{
		children: make(map[string]*Result),
	}
}

func (r Result) Leaf() (value []byte, isLeaf bool) {
	if len(r.children) > 0 {
		return nil, false
	}

	return r.value, true
}

func (r Result) ChildrenKeys() [][]byte {
	var keys [][]byte
	for k, _ := range r.children {
		keys = append(keys, []byte(k))
	}
	return keys
}

func (r *Result) Child(key []byte) (child *Result) {
	return r.children[string(key)]
}

func (r *Result) addChild(key []byte) (child *Result) {
	child = newResult()
	child.Key = key
	r.children[string(key)] = child
	return child
}
