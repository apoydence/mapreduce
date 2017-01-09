package mapreduce

import "log"

type ChainLink interface {
	Map(m Mapper) ChainLink
	Reduce(r Reducer) ChainLink
	FinalReduce(r FinalReducer) Functions
}

type funcChainBuilder struct {
	fs []Function
}

type Function struct {
	mapper       Mapper
	reducer      Reducer
	finalReducer FinalReducer
}

func Build(m Mapper) ChainLink {
	return &funcChainBuilder{
		fs: []Function{{mapper: m}},
	}
}

func (b *funcChainBuilder) Map(m Mapper) ChainLink {
	if m == nil {
		log.Panic("Mapper must not be nil")
	}

	b.fs = append(b.fs, Function{mapper: m})
	return b
}

func (b *funcChainBuilder) Reduce(r Reducer) ChainLink {
	if r == nil {
		log.Panic("Reducer must not be nil")
	}

	b.fs = append(b.fs, Function{reducer: r})
	return b
}

func (b *funcChainBuilder) FinalReduce(r FinalReducer) Functions {
	if r == nil {
		log.Panic("Reducer must not be nil")
	}

	b.fs = append(b.fs, Function{finalReducer: r})
	return b
}

func (b *funcChainBuilder) Functions() []Function {
	return b.fs
}
