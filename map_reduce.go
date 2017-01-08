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

type MapReduce struct {
	fs        FileSystem
	network   Network
	functions []function
}

func New(fs FileSystem, network Network, chain ChainLink) MapReduce {
	functions := chain.functions()
	return MapReduce{
		fs:        fs,
		network:   network,
		functions: functions,
	}
}

func (r MapReduce) Calculate(name string) error {
	reader, err := r.fs.ReadFile(name)
	if err != nil {
		return err
	}

	data, err := r.readAll(reader)
	if err != nil {
		return err
	}

	r.invokeChain(data, r.functions)
	return nil
}

func (r MapReduce) invokeChain(data [][]byte, functions []function) {
	if len(functions) == 0 {
		return
	}

	f := functions[0]
	if f.reducer != nil {
		newData := f.reducer.Reduce(data)
		r.invokeChain(newData, functions[1:])
		return
	}

	m := make(map[string][][]byte)
	for _, segment := range data {
		key, ok := f.mapper.Map(segment)
		if !ok {
			continue
		}

		m[string(key)] = append(m[string(key)], segment)
	}

	for _, v := range m {
		r.invokeChain(v, functions[1:])
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
