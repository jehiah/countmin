/*
Count-Min Sketch, an approximate counting data structure for summarizing data streams

for more information see http://github.com/jehiah/countmin
*/
package countmin

import (
	"encoding/binary"
	"hash/crc32"
	"hash/fnv"
)

type Sketch interface {
	Add([]byte, uint32) uint32
	Query([]byte) uint32
	AddString(string, uint32) uint32
	QueryString(string) uint32
}

type countMinSketch struct {
	Hashes  int
	Columns uint32
	Data    [][]uint32
}

// Create a new Sketch. Settings for hashes and columns affect performance
// of Adding and Querying items, but also accuracy.
func NewCountMinSketch(hashes int, columns int) Sketch {
	s := countMinSketch{
		Hashes:  hashes,
		Columns: uint32(columns),
		Data:    make([][]uint32, hashes),
	}
	for i, _ := range s.Data {
		s.Data[i] = make([]uint32, columns)
	}
	return &s
}

func (s *countMinSketch) AddString(key string, count uint32) uint32 {
	return s.Add([]byte(key), count)
}
func (s *countMinSketch) QueryString(key string) uint32 {
	return s.Query([]byte(key))
}

func (s *countMinSketch) Add(key []byte, count uint32) uint32 {
	// TODO: this is a bad implementation because we hash all twice in worst case.
	newValue := s.Query(key) + count
	h := fnv.New64a()
	h.Write(key)
	var b []byte
	for i := 0; i < s.Hashes; i += 1 {
		binary.Write(h, binary.LittleEndian, uint32(i))
		index := crc32.ChecksumIEEE(h.Sum(b)) % s.Columns
		if s.Data[i][index] <= newValue {
			s.Data[i][index] = newValue
		}
	}
	return newValue
}

func (s *countMinSketch) Query(key []byte) uint32 {
	h := fnv.New64a()
	h.Write(key)
	var min uint32
	var b []byte
	for i := 0; i < s.Hashes; i += 1 {
		binary.Write(h, binary.LittleEndian, uint32(i))
		index := crc32.ChecksumIEEE(h.Sum(b)) % s.Columns
		v := s.Data[i][index]
		if i == 0 || v < min {
			min = v
		}
	}
	return min
}
