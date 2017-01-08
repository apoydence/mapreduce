package mapreduce

import "log"

type ChainLink interface {
	Map(m Mapper) ChainLink
	Reduce(r Reducer) ChainLink
	functions() []function
}

type funcChainBuilder struct {
	fs []function
}

type function struct {
	mapper  Mapper
	reducer Reducer
}

func Build(m Mapper) ChainLink {
	return &funcChainBuilder{
		fs: []function{{mapper: m}},
	}
}

func (b *funcChainBuilder) Map(m Mapper) ChainLink {
	if m == nil {
		log.Panic("Mapper must not be nil")
	}

	b.fs = append(b.fs, function{mapper: m})
	return b
}

func (b *funcChainBuilder) Reduce(r Reducer) ChainLink {
	if r == nil {
		log.Panic("Reducer must not be nil")
	}

	b.fs = append(b.fs, function{reducer: r})
	return b
}

func (b *funcChainBuilder) functions() []function {
	return b.fs
}
