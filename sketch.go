package countmin

import (
	"hash/crc32"
	"hash/fnv"
	// "log"
	"encoding/binary"
)

type Sketch interface {
	Add([]byte, uint32) uint32
	Query([]byte) uint32
	AddString(string, uint32) uint32
	QueryString(string) uint32
}

type CountMinSketch struct {
	Hashes  int
	Columns uint32
	Data    [][]uint32
}

func NewCountMinSketch(hashes int, columns int) Sketch {
	s := CountMinSketch{
		Hashes:  hashes,
		Columns: uint32(columns),
		Data:    make([][]uint32, hashes),
	}
	for i, _ := range s.Data {
		s.Data[i] = make([]uint32, columns)
	}
	return &s
}

func (s *CountMinSketch) AddString(key string, count uint32) uint32 {
	return s.Add([]byte(key), count)
}
func (s *CountMinSketch) QueryString(key string) uint32 {
	return s.Query([]byte(key))
}

func (s *CountMinSketch) Add(key []byte, count uint32) uint32 {
	// this is a bad implementation because we hash all twice in worst case.
	newValue := s.Query(key) + count
	h := fnv.New64a()
	h.Write(key)
	var b []byte
	for i := 0; i < s.Hashes; i += 1 {
		binary.Write(h, binary.LittleEndian, uint32(i))
		index := crc32.ChecksumIEEE(h.Sum(b)) % s.Columns
		if s.Data[i][index] <= newValue {
			s.Data[i][index] = newValue
			// log.Printf("%s - [%d][%d] = %d", key, i, index, s.Data[i][index])
		}
	}
	return newValue
}

func (s *CountMinSketch) Query(key []byte) uint32 {
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
