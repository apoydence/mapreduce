package mapreduce_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/apoydence/eachers/testhelpers"
	"github.com/apoydence/mapreduce"
	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
)

type TE struct {
	*testing.T
	e              *mapreduce.Executor
	mockFileSystem *mockFileSystem
	mockReducer    *mockReducer
	mockMapper     *mockMapper
	algs           map[string]mapreduce.Algorithm
}

func TestExecutor(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TE {
		mockReducer := newMockReducer()
		mockMapper := newMockMapper()
		mockFileSystem := newMockFileSystem()

		algs := map[string]mapreduce.Algorithm{
			"a": mapreduce.Algorithm{Mapper: mockMapper, Reducer: mockReducer},
		}

		return TE{
			T:              t,
			mockMapper:     mockMapper,
			mockReducer:    mockReducer,
			mockFileSystem: mockFileSystem,
			e:              mapreduce.NewExecutor(algs, mockFileSystem),
		}
	})

	o.Group("when the filesystem does not return an error", func() {
		o.BeforeEach(func(t TE) TE {
			results := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
			t.mockFileSystem.ReaderOutput.Reader <- func() ([]byte, error) {
				if len(results) == 0 {
					return nil, io.EOF
				}
				defer func() { results = results[1:] }()
				return results[0], nil
			}
			close(t.mockFileSystem.ReaderOutput.Err)
			return t
		})

		o.Group("when the mapper does not return an error", func() {
			o.BeforeEach(func(t TE) TE {
				testhelpers.AlwaysReturn(t.mockMapper.MapOutput.Key, "key")
				testhelpers.AlwaysReturn(t.mockMapper.MapOutput.Output, []byte("a"))
				close(t.mockMapper.MapOutput.Err)
				return t
			})

			o.Group("when the reducer does not return an error", func() {
				o.BeforeEach(func(t TE) TE {
					testhelpers.AlwaysReturn(t.mockReducer.ReduceOutput.Reduced, [][]byte{[]byte("a")})
					close(t.mockReducer.ReduceOutput.Err)
					return t
				})
				o.Spec("it uses the correct file", func(t TE) {
					t.e.Execute("file", "a", context.Background())
					Expect(t, t.mockFileSystem.ReaderInput.File).To(Chain(
						Receive(), Equal("file"),
					))
				})

				o.Spec("it uses mapper for each value in file", func(t TE) {
					t.e.Execute("file", "a", context.Background())
					s := toSliceBytes(t.mockMapper.MapInput.Value, 3)
					Expect(t, s).To(Equal([][]byte{
						[]byte("a"),
						[]byte("b"),
						[]byte("c"),
					}))
				})

				o.Spec("it uses the reducer for each key", func(t TE) {
					t.e.Execute("file", "a", context.Background())
					s := toSliceDoubleBytes(t.mockReducer.ReduceInput.Value, 1)
					Expect(t, s).To(Contain([][]byte{
						[]byte("a"),
						[]byte("a"),
						[]byte("a"),
					}))
				})

				o.Spec("it returns a result for each key", func(t TE) {
					result, err := t.e.Execute("file", "a", context.Background())
					Expect(t, err == nil).To(BeTrue())
					Expect(t, result).To(HaveLen(1))

					value, ok := result["key"]
					Expect(t, ok).To(BeTrue())
					Expect(t, value).To(Equal([]byte("a")))
				})
			})

			o.Group("when the reducer returns an error", func() {
				o.BeforeEach(func(t TE) TE {
					close(t.mockReducer.ReduceOutput.Reduced)
					t.mockReducer.ReduceOutput.Err <- fmt.Errorf("some-error")
					return t
				})

				o.Spec("it returns an error", func(t TE) {
					_, err := t.e.Execute("file", "a", context.Background())
					Expect(t, err == nil).To(BeFalse())
				})
			})
		})

		o.Group("when the mapper returns an error", func() {
			o.BeforeEach(func(t TE) TE {
				close(t.mockMapper.MapOutput.Output)
				close(t.mockMapper.MapOutput.Key)
				t.mockMapper.MapOutput.Err <- fmt.Errorf("some-error")
				return t
			})

			o.Spec("it returns an error", func(t TE) {
				_, err := t.e.Execute("file", "a", context.Background())
				Expect(t, err == nil).To(BeFalse())
			})
		})
	})

	o.Group("when the filesystem returns an error", func() {
		o.BeforeEach(func(t TE) TE {
			close(t.mockFileSystem.ReaderOutput.Reader)
			t.mockFileSystem.ReaderOutput.Err <- fmt.Errorf("some-error")
			return t
		})

		o.Spec("it returns an error", func(t TE) {
			_, err := t.e.Execute("file", "a", context.Background())
			Expect(t, err == nil).To(BeFalse())
		})
	})
}

func toSliceBytes(c <-chan []byte, count int) (results [][]byte) {
	for i := 0; i < count; i++ {
		select {
		case x := <-c:
			results = append(results, x)
		case <-time.NewTimer(time.Second).C:
			panic(fmt.Sprintf("expected to receive (i=%d)", i))
		}
	}

	return results
}

func toSliceDoubleBytes(c <-chan [][]byte, count int) (results [][][]byte) {
	for i := 0; i < count; i++ {
		select {
		case x := <-c:
			results = append(results, x)
		case <-time.NewTimer(time.Second).C:
			panic(fmt.Sprintf("expected to receive (i=%d)", i))
		}
	}

	return results
}
