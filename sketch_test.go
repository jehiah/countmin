package countmin

import (
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
		key := strconv.Itoa(i)
		vv := s.AddString(key, v)
		if vv > v {
			diverged += 1
		}
	}

	var miss int
	for i := 1; i < iterations; i += 1 {
		expected := uint32(i % 50)
		key := strconv.Itoa(i)
		got := s.QueryString(key)
		if got != expected {
			t.Logf("missed got %d expected %d", got, expected)
			miss += 1
		}
	}
	t.Logf("missed %d of %d (%d diverged during adds)", miss, iterations, diverged)
}
