package mapreduce

import "io"

type FileReader interface {
	Length() uint64
	Read() ([]byte, error)
}

type FileWriter interface {
	Write(data []byte) error
}

type FileSystem interface {
	CreateFile(name string) error
	ReadFile(name string) (FileReader, error)
	WriteToFile(name string) (FileWriter, error)
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
	reader, err := r.fs.ReadFile(name)
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
			newData := f.finalReducer.FinalReduce(data)
			if len(newData) > 1 {
				continue
			}
			results.value = newData[0]

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
