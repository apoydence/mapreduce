//go:generate hel

package mapreduce_test

import (
	"fmt"
	"io"
	"testing"

	"github.com/apoydence/mapreduce"
	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
)

type TMR struct {
	*testing.T
	mr mapreduce.MapReduce

	mockFileSystem *mockFileSystem
	mockNetwork    *mockNetwork

	mockFileReader *mockFileReader

	mapIndex    chan int
	reduceIndex chan int

	mapValues     chan []byte
	mapResultsKey chan []byte
	mapResultsOk  chan bool

	reduceValues chan [][]byte
	reduceResult chan [][]byte
}

func TestMapReduce(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TMR {
		mapIndex := make(chan int, 100)
		reduceIndex := make(chan int, 100)

		mapValues := make(chan []byte, 100)
		mapResultsKey := make(chan []byte, 100)
		mapResultsOk := make(chan bool, 100)

		reduceValue := make(chan [][]byte, 100)
		reduceResult := make(chan [][]byte, 100)

		mockFileSystem := newMockFileSystem()
		mockNetwork := newMockNetwork()
		mockFileReader := newMockFileReader()

		chain := mapreduce.Build(mapreduce.MapFunc(func(value []byte) (key []byte, ok bool) {
			mapIndex <- 0
			mapValues <- value
			return <-mapResultsKey, <-mapResultsOk
		})).Reduce(mapreduce.ReduceFunc(func(value [][]byte) (reduced [][]byte) {
			reduceIndex <- 1
			reduceValue <- value
			return <-reduceResult
		})).Map(mapreduce.MapFunc(func(value []byte) (key []byte, ok bool) {
			mapIndex <- 1
			mapValues <- value
			return <-mapResultsKey, <-mapResultsOk
		})).FinalReduce(mapreduce.FinalReduceFunc(func(value [][]byte) (reduced [][]byte) {
			reduceIndex <- 1
			reduceValue <- value
			return <-reduceResult
		}))

		mr := mapreduce.New(mockFileSystem, mockNetwork, chain)

		return TMR{
			T:              t,
			mr:             mr,
			mockFileSystem: mockFileSystem,
			mockNetwork:    mockNetwork,
			mockFileReader: mockFileReader,

			mapIndex:      mapIndex,
			mapValues:     mapValues,
			mapResultsKey: mapResultsKey,
			mapResultsOk:  mapResultsOk,

			reduceIndex:  reduceIndex,
			reduceValues: reduceValue,
			reduceResult: reduceResult,
		}
	})

	o.Group("when FileSystem does not return an error", func() {
		o.BeforeEach(func(t TMR) TMR {
			t.mockFileSystem.ReadFileOutput.Ret0 <- t.mockFileReader
			close(t.mockFileSystem.ReadFileOutput.Ret1)

			return t
		})

		o.Spec("it uses the correct name in the file system", func(t TMR) {
			close(t.mapResultsKey)
			close(t.mapResultsOk)
			close(t.reduceResult)
			close(t.mockFileReader.ReadOutput.Ret0)
			t.mockFileReader.ReadOutput.Ret1 <- io.EOF

			t.mr.Calculate("some-name")

			Expect(t, t.mockFileSystem.ReadFileInput.Name).To(ViaPolling(
				Chain(Receive(), Equal("some-name")),
			))
		})

		o.Group("when FileReader does not return an error", func() {
			o.Spec("it passes the data from the reader to the mapper", func(t TMR) {
				close(t.mapResultsKey)
				close(t.mapResultsOk)
				close(t.reduceResult)
				t.mockFileReader.ReadOutput.Ret0 <- []byte("some-data")
				t.mockFileReader.ReadOutput.Ret1 <- nil

				t.mockFileReader.ReadOutput.Ret0 <- nil
				t.mockFileReader.ReadOutput.Ret1 <- io.EOF

				t.mr.Calculate("some-name")

				Expect(t, t.mapValues).To(ViaPolling(
					Chain(Receive(), Equal([]byte("some-data"))),
				))
			})

			o.Spec("it writes each functions result to the next function", func(t TMR) {
				t.mapResultsOk <- true
				close(t.mapResultsKey)
				close(t.mapResultsOk)

				t.mockFileReader.ReadOutput.Ret0 <- []byte("some-data")
				t.mockFileReader.ReadOutput.Ret1 <- nil
				t.mockFileReader.ReadOutput.Ret0 <- nil
				t.mockFileReader.ReadOutput.Ret1 <- io.EOF
				t.reduceResult <- [][]byte{[]byte("some-reduced-data")}

				t.mr.Calculate("some-name")

				Expect(t, t.reduceValues).To(ViaPolling(
					Chain(Receive(), Equal([][]byte{[]byte("some-data")})),
				))

				Expect(t, t.mapValues).To(ViaPolling(
					Chain(Receive(), Equal([]byte("some-reduced-data"))),
				))
			})

			o.Spec("it does not write filtered out data", func(t TMR) {
				t.mapResultsOk <- false
				t.mapResultsOk <- true
				close(t.mapResultsKey)
				close(t.mapResultsOk)

				t.mockFileReader.ReadOutput.Ret0 <- []byte("some-filtered-data")
				t.mockFileReader.ReadOutput.Ret1 <- nil

				t.mockFileReader.ReadOutput.Ret0 <- []byte("some-data")
				t.mockFileReader.ReadOutput.Ret1 <- nil

				t.mockFileReader.ReadOutput.Ret0 <- nil
				t.mockFileReader.ReadOutput.Ret1 <- io.EOF

				t.reduceResult <- [][]byte{[]byte("some-reduced-data")}

				t.mr.Calculate("some-name")

				Expect(t, t.reduceValues).To(Always(
					Not(Chain(Receive(), Equal([]byte("some-filtered-data")))),
				))

				Expect(t, t.mapValues).To(ViaPolling(
					Chain(Receive(), Equal([]byte("some-reduced-data"))),
				))
			})

			o.Spec("it groups the data via key", func(t TMR) {
				t.mapResultsKey <- []byte("some-key-a")
				t.mapResultsKey <- []byte("some-key-b")
				t.mapResultsKey <- []byte("some-key-a")
				t.mapResultsOk <- true
				t.mapResultsOk <- true
				t.mapResultsOk <- true
				close(t.mapResultsKey)
				close(t.mapResultsOk)

				t.mockFileReader.ReadOutput.Ret0 <- []byte("some-data-0")
				t.mockFileReader.ReadOutput.Ret1 <- nil
				t.mockFileReader.ReadOutput.Ret0 <- []byte("some-data-1")
				t.mockFileReader.ReadOutput.Ret1 <- nil
				t.mockFileReader.ReadOutput.Ret0 <- []byte("some-data-2")
				t.mockFileReader.ReadOutput.Ret1 <- nil
				t.mockFileReader.ReadOutput.Ret0 <- nil
				t.mockFileReader.ReadOutput.Ret1 <- io.EOF
				t.reduceResult <- [][]byte{[]byte("some-reduced-data")}
				t.reduceResult <- [][]byte{[]byte("some-reduced-data")}
				t.reduceResult <- [][]byte{[]byte("some-reduced-data")}

				t.mr.Calculate("some-name")

				Expect(t, t.reduceValues).To(ViaPolling(
					Chain(Receive(), Equal([][]byte{
						[]byte("some-data-0"),
						[]byte("some-data-2"),
					})),
				))

				Expect(t, t.reduceValues).To(ViaPolling(
					Chain(Receive(), Equal([][]byte{
						[]byte("some-data-1"),
					})),
				))
			})

			o.Spec("it invokes FinalReduce until result is length 1", func(t TMR) {
				for i := 0; i < 4; i++ {
					t.mapResultsKey <- []byte("some-key-a")
					t.mapResultsOk <- true
				}
				t.mockFileReader.ReadOutput.Ret0 <- []byte("some-data-1")
				t.mockFileReader.ReadOutput.Ret1 <- nil
				t.mockFileReader.ReadOutput.Ret0 <- nil
				t.mockFileReader.ReadOutput.Ret1 <- io.EOF
				t.reduceResult <- [][]byte{{'a'}, {'b'}, {'c'}}
				t.reduceResult <- [][]byte{{'a'}, {'b'}}
				t.reduceResult <- [][]byte{{'a'}}

				t.mr.Calculate("some-name")

				Expect(t, t.reduceResult).To(ViaPolling(HaveLen(0)))
			})

			o.Spec("it returns a result tree", func(t TMR) {
				for i := 0; i < 4; i++ {
					t.mapResultsKey <- []byte("some-key-a")
					t.mapResultsOk <- true
				}
				t.mockFileReader.ReadOutput.Ret0 <- []byte("some-data-1")
				t.mockFileReader.ReadOutput.Ret1 <- nil
				t.mockFileReader.ReadOutput.Ret0 <- nil
				t.mockFileReader.ReadOutput.Ret1 <- io.EOF
				t.reduceResult <- [][]byte{{'a'}, {'b'}, {'c'}}
				t.reduceResult <- [][]byte{{'a'}, {'b'}}
				t.reduceResult <- [][]byte{{'a'}}

				results, err := t.mr.Calculate("some-name")
				Expect(t, err == nil).To(BeTrue())
				Expect(t, results == nil).To(BeFalse())

				_, isLeaf := results.Leaf()
				Expect(t, isLeaf).To(BeFalse())
				Expect(t, results.ChildrenKeys()).To(Contain(
					[]byte("some-key-a"),
				))
				child := results.Child([]byte("some-key-a"))
				Expect(t, child == nil).To(BeFalse())
				_, isLeaf = child.Leaf()
				Expect(t, isLeaf).To(BeFalse())

				child = child.Child([]byte("some-key-a"))
				Expect(t, child == nil).To(BeFalse())
				value, isLeaf := child.Leaf()
				Expect(t, isLeaf).To(BeTrue())
				Expect(t, string(value)).To(Equal("a"))
			})
		})
	})

	o.Group("when the FileSystem returns an error", func() {
		o.BeforeEach(func(t TMR) TMR {
			t.mockFileSystem.ReadFileOutput.Ret0 <- nil
			t.mockFileSystem.ReadFileOutput.Ret1 <- fmt.Errorf("some-error")
			return t
		})

		o.Spec("it returns an error", func(t TMR) {
			_, err := t.mr.Calculate("some-name")
			Expect(t, err == nil).To(BeFalse())
		})
	})
}
