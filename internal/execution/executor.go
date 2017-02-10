package execution

type Mapper interface {
	Map(value []byte) (key []byte, ok bool)
}

type Reducer interface {
	Reduce(value [][]byte) (reduced [][]byte)
}

type FinalReducer interface {
	FinalReduce(value [][]byte) (reduced [][]byte)
}

type Function struct {
	mapper       Mapper
	reducer      Reducer
	finalReducer FinalReducer
}

type Functions interface {
	Functions() []Function
}

type Executor struct {
	chains map[string]Functions
}

func NewExecutor(chains map[string]Functions) *Executor {
	return &Executor{
		chains: chains,
	}
}

func (e *Executor) Execute(chainName string) {
}
