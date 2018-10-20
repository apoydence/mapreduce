package execution_test

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/poy/onpar"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
	}

	os.Exit(m.Run())
}

type TE struct {
	*testing.T
}

func TestExecutor(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TE {
		return TE{
			T: t,
		}
	})

	o.Spec("", func(t TE) {
	})
}
