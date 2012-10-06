package countmin

import (
	// "github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
)

func TestBasicSketch(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	log.SetOutput(os.Stdout)

	s := NewCountMinSketch(20, 2500)
	iterations := 50000
	for i := 1; i < iterations; i += 1 {
		s.AddString(strconv.Itoa(i), int64(i))
	}

	for i := 1; i < iterations; i += 1 {
		v := s.QueryString(strconv.Itoa(i))
		if v != int64(i) {
			log.Printf("%d - %d", i, v)
		}
	}
}
