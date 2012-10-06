package countmin

import (
	"hash/crc32"
	"hash/fnv"
	"encoding/binary"
)

type Sketch interface {
	Add([]byte, int64)
	Query([]byte) int64
	AddString(string, int64)
	QueryString(string) int64
}

type CountMinSketch struct {
	Hashes  int
	Columns uint32
	Data    [][]int64
}

func NewCountMinSketch(hashes int, columns int) Sketch {
	s := CountMinSketch{
		Hashes:  hashes,
		Columns: uint32(columns),
		Data:    make([][]int64, hashes),
	}
	for i, _ := range s.Data {
		s.Data[i] = make([]int64, columns)
	}
	return &s
}

func (s *CountMinSketch) AddString(key string, count int64) {
	s.Add([]byte(key), count)
}
func (s *CountMinSketch) QueryString(key string) int64 {
	return s.Query([]byte(key))
}

func (s *CountMinSketch) Add(key []byte, count int64) {
	h := fnv.New64a()
	h.Write(key)
	var b []byte
	for i := 0; i < s.Hashes; i += 1 {
		binary.Write(h, binary.LittleEndian, uint32(i))
		index := crc32.ChecksumIEEE(h.Sum(b)) % s.Columns
		s.Data[i][index] += count
	}
}

func (s *CountMinSketch) Query(key []byte) int64 {
	h := fnv.New64a()
	h.Write(key)
	var min int64
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
