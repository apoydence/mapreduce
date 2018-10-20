//go:generate hel

package mapreduce_test

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/poy/eachers/testhelpers"
	"github.com/poy/mapreduce"
	"github.com/poy/onpar"
	. "github.com/poy/onpar/expect"
	. "github.com/poy/onpar/matchers"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
	}

	os.Exit(m.Run())
}

type TMR struct {
	*testing.T

	mockFileSystem *mockFileSystem
	mockNetwork    *mockNetwork
	mockAlgorithm  *mockReducer
	mockAlgFetcher *mockAlgorithmFetcher

	mr mapreduce.MapReduce
}

func TestMapReduce(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TMR {
		mockFileSystem := newMockFileSystem()
		mockNetwork := newMockNetwork()
		mockReducer := newMockReducer()
		mockAlgFetcher := newMockAlgorithmFetcher()

		mockAlgFetcher.AlgOutput.Alg <- mapreduce.Algorithm{Reducer: mockReducer}
		close(mockAlgFetcher.AlgOutput.Err)

		var opts []mapreduce.MapReduceOption
		if testing.Verbose() {
			opts = append(opts, mapreduce.WithLogger(log.New(os.Stderr, "", log.LstdFlags)))
		}

		return TMR{
			T:              t,
			mockNetwork:    mockNetwork,
			mockFileSystem: mockFileSystem,
			mockAlgorithm:  mockReducer,
			mockAlgFetcher: mockAlgFetcher,
			mr:             mapreduce.New(mockFileSystem, mockNetwork, mockAlgFetcher, opts...),
		}
	})

	o.Group("when the FileSystem does not return an error", func() {
		o.BeforeEach(func(t TMR) TMR {
			testhelpers.AlwaysReturn(t.mockFileSystem.FilesOutput.Files, map[string][]string{
				"some-name-a": []string{"id-a", "id-b"},
				"some-name-b": []string{"id-b", "id-c"},
			})
			close(t.mockFileSystem.FilesOutput.Err)
			return t
		})

		o.Group("when the Network does not return an error", func() {
			o.BeforeEach(func(t TMR) TMR {
				close(t.mockNetwork.ExecuteOutput.Err)
				return t
			})

			o.Group("when all the results have unique keys", func() {
				o.BeforeEach(func(t TMR) TMR {
					go func() {
						for i := 0; i < 100; i++ {
							t.mockNetwork.ExecuteOutput.Result <- map[string][]byte{
								fmt.Sprintf("key-%d", i): []byte(fmt.Sprintf("some-value-%d", i)),
							}
						}
					}()
					return t
				})

				o.Spec("it does not return an error", func(t TMR) {
					_, err := t.mr.Calculate("some-file", "some-alg", context.Background(), nil)
					Expect(t, err == nil).To(BeTrue())
				})

				o.Spec("it uses the correct chain name", func(t TMR) {
					t.mr.Calculate("some-file", "some-alg", context.Background(), nil)

					s := toSlice(t.mockNetwork.ExecuteInput.AlgName, 2)
					Expect(t, s).To(Equal([]string{"some-alg", "some-alg"}))
				})

				o.Spec("it executes each file on a corresponding node", func(t TMR) {
					t.mr.Calculate("some-file", "some-alg", context.Background(), nil)

					m := toMap(t.mockNetwork.ExecuteInput.File, t.mockNetwork.ExecuteInput.NodeID, 2)
					Expect(t, m).To(HaveLen(2))

					id, ok := m["some-name-a"]
					Expect(t, ok).To(BeTrue())
					Expect(t, id).To(Or(Equal("id-a"), Equal("id-b")))

					id, ok = m["some-name-b"]
					Expect(t, ok).To(BeTrue())
					Expect(t, id).To(Or(Equal("id-b"), Equal("id-c")))
				})

				o.Spec("it returns the results", func(t TMR) {
					result, _ := t.mr.Calculate("some-file", "some-alg", context.Background(), nil)

					Expect(t, result).To(HaveLen(2))
					Expect(t, result["key-0"]).To(Equal([]byte("some-value-0")))
					Expect(t, result["key-1"]).To(Equal([]byte("some-value-1")))
				})

				o.Spec("it does not need the reducer", func(t TMR) {
					t.mr.Calculate("some-file", "some-alg", context.Background(), nil)

					Expect(t, t.mockAlgorithm.ReduceCalled).To(Always(HaveLen(0)))
				})
			})

			o.Group("when the results have the same key", func() {
				o.BeforeEach(func(t TMR) TMR {
					go func() {
						for i := 0; i < 100; i++ {
							t.mockNetwork.ExecuteOutput.Result <- map[string][]byte{
								"same-key": []byte("some-value"),
							}
						}
					}()

					go func() {
						results := [][]byte{
							[]byte("a"),
							[]byte("b"),
							[]byte("c"),
							[]byte("d"),
							[]byte("e"),
						}
						for {
							t.mockAlgorithm.ReduceOutput.Reduced <- results
							if len(results) == 0 {
								return
							}
							results = results[1:]
						}
					}()
					return t
				})

				o.Group("when the reducer does not return an error", func() {
					o.BeforeEach(func(t TMR) TMR {
						close(t.mockAlgorithm.ReduceOutput.Err)
						return t
					})

					o.Spec("it returns the combined results", func(t TMR) {
						result, _ := t.mr.Calculate("some-file", "some-alg", context.Background(), nil)
						_ = result
					})

					o.Spec("it combines the results with the reducer", func(t TMR) {
						t.mr.Calculate("some-file", "some-alg", context.Background(), nil)

						Expect(t, t.mockAlgorithm.ReduceInput.Value).To(Chain(
							Receive(), HaveLen(2),
						))
					})

					o.Spec("it combines until there is a single result for the key", func(t TMR) {
						t.mr.Calculate("some-file", "some-alg", context.Background(), nil)

						Expect(t, t.mockAlgorithm.ReduceCalled).To(HaveLen(5))
					})
				})

				o.Group("when the reducer returns an error", func() {
					o.BeforeEach(func(t TMR) TMR {
						t.mockAlgorithm.ReduceOutput.Err <- fmt.Errorf("some-error")
						return t
					})

					o.Spec("it returns an error", func(t TMR) {
						_, err := t.mr.Calculate("some-file", "some-alg", context.Background(), nil)
						Expect(t, err == nil).To(BeFalse())
					})
				})
			})
		})

		o.Group("when the Network returns an error", func() {
			o.BeforeEach(func(t TMR) TMR {
				testhelpers.AlwaysReturn(t.mockNetwork.ExecuteOutput.Err, fmt.Errorf("some-error"))
				close(t.mockNetwork.ExecuteOutput.Result)
				return t
			})

			// TODO: We should retry with different nodes
			o.Spec("it returns an error", func(t TMR) {
				_, err := t.mr.Calculate("some-file", "some-alg", context.Background(), nil)
				Expect(t, err == nil).To(BeFalse())
			})
		})
	})

	o.Group("when the FileSystem returns an error", func() {
		o.BeforeEach(func(t TMR) TMR {
			t.mockFileSystem.FilesOutput.Err <- fmt.Errorf("some-error")
			close(t.mockFileSystem.FilesOutput.Files)
			return t
		})

		o.Spec("it returns an error", func(t TMR) {
			_, err := t.mr.Calculate("some-file", "some-alg", context.Background(), nil)
			Expect(t, err == nil).To(BeFalse())
		})
	})

}

func toSlice(c <-chan string, count int) (result []string) {
	for i := 0; i < count; i++ {
		select {
		case x := <-c:
			result = append(result, x)
		case <-time.NewTimer(time.Second).C:
			panic(fmt.Sprintf("expected to receive (i=%d)", i))
		}
	}
	return result
}

func toMap(a, b <-chan string, count int) (result map[string]string) {
	result = make(map[string]string)
	for i := 0; i < count; i++ {
		var aa, bb string

		select {
		case aa = <-a:
		case <-time.NewTimer(time.Second).C:
			panic(fmt.Sprintf("expected to receive (i=%d)", i))
		}

		select {
		case bb = <-b:
		case <-time.NewTimer(time.Second).C:
			panic(fmt.Sprintf("expected to receive (i=%d)", i))
		}

		result[aa] = bb
	}
	return result

}
