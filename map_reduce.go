package mapreduce

import (
	"io"
	"log"
)

type FileReader interface {
	Read() ([]byte, error)
}

type FileSystem interface {
	Nodes(name string) (IDs []string, err error)
	Length(name string) (length uint64, err error)
	ReadFile(name string, start, end uint64) (FileReader, error)
}

type Network interface {
}

type Functions interface {
	Functions() []Function
}

type MapReduce struct {
	fs        FileSystem
	network   Network
	functions []Function
}

func New(fs FileSystem, network Network, funcs Functions) MapReduce {
	functions := funcs.Functions()
	return MapReduce{
		fs:        fs,
		network:   network,
		functions: functions,
	}
}

func (r MapReduce) Calculate(name string) (*Result, error) {
	length, err := r.fs.Length(name)
	if err != nil {
		return nil, err
	}

	nodes, err := r.fs.Nodes(name)
	if err != nil {
		return nil, err
	}

	segmentLen := length / uint64(len(nodes))

	rc := make(chan *Result, len(nodes))
	errs := make(chan error, len(nodes))
	var results []*Result
	for start := uint64(0); start < length; start += segmentLen {
		go func(start uint64) {
			result, err := r.calculate(name, start, start+segmentLen)
			if err != nil {
				errs <- err
			}

			rc <- result
		}(start)
	}

	for i := 0; i < len(nodes); i++ {
		select {
		case result := <-rc:
			results = append(results, result)
		case err := <-errs:
			return nil, err
		}
	}

	return r.combineResults(results), nil
}

func (r MapReduce) combineResults(results []*Result) *Result {
	finalResult := newResult()
	m := make(map[*Result][][]byte)
	for _, res := range results {
		r.traverseResult(res, finalResult, m)
	}

	finalReducer := r.finalReducer()
	for k, v := range m {
		for {
			if len(v) == 1 {
				k.value = v[0]
				break
			}
			v = finalReducer.FinalReduce(v)
		}
	}

	return finalResult
}

func (r MapReduce) finalReducer() FinalReducer {
	if len(r.functions) == 0 {
		log.Fatal("Empty function chain")
	}

	f := r.functions[len(r.functions)-1].finalReducer

	if f == nil {
		log.Fatal("Final function in chain is not a FinalReducer")
	}
	return f
}

func (r MapReduce) traverseResult(result, finalResult *Result, m map[*Result][][]byte) {
	if result == nil {
		return
	}

	for _, k := range result.ChildrenKeys() {
		child := finalResult.addChild(k)
		r.traverseResult(result.Child(k), child, m)
	}

	value, leaf := result.Leaf()
	if !leaf {
		return
	}

	m[finalResult] = append(m[finalResult], value)
}

func (r MapReduce) calculate(name string, start, end uint64) (*Result, error) {
	reader, err := r.fs.ReadFile(name, start, end)
	if err != nil {
		return nil, err
	}

	data, err := r.readAll(reader)
	if err != nil {
		return nil, err
	}

	results := newResult()
	r.invokeChain(data, results, r.functions)
	return results, nil
}

func (r MapReduce) invokeChain(data [][]byte, results *Result, functions []Function) {
	if len(functions) == 0 {
		return
	}

	f := functions[0]
	if f.reducer != nil {
		newData := f.reducer.Reduce(data)
		r.invokeChain(newData, results, functions[1:])
		return
	}

	if f.finalReducer != nil {
		for {
			data = f.finalReducer.FinalReduce(data)
			if len(data) > 1 {
				continue
			}
			results.value = data[0]

			return
		}
	}

	m := make(map[string][][]byte)
	for _, segment := range data {
		key, ok := f.mapper.Map(segment)
		if !ok {
			continue
		}

		m[string(key)] = append(m[string(key)], segment)
	}

	for k, v := range m {
		child := results.addChild([]byte(k))
		r.invokeChain(v, child, functions[1:])
	}
}

func (r MapReduce) readAll(reader FileReader) ([][]byte, error) {
	var results [][]byte
	for {
		data, err := reader.Read()
		if err == io.EOF {
			return results, nil
		}

		if err != nil {
			return nil, err
		}

		results = append(results, data)
	}
}
