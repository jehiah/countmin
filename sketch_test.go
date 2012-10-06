package countmin

import (
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
)

func TestBasicSketch(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	log.SetOutput(os.Stdout)

	s := NewCountMinSketch(7, 2000)
	iterations := 5500
	var diverged int
	for i := 1; i < iterations; i += 1 {
		v := uint32(i % 50)
		vv := s.AddString(strconv.Itoa(i), v)
		if vv > v {
			diverged += 1
		}
	}

	var miss int
	for i := 1; i < iterations; i += 1 {
		vv := uint32(i % 50)
		v := s.QueryString(strconv.Itoa(i))
		assert.Equal(t, v >= v, true)
		if v != vv {
			miss += 1
		}
	}
	log.Printf("missed %d of %d (%d diverged during adds)", miss, iterations, diverged)
}
