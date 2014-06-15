package store

import (
	"github.com/PreetamJinka/listmap"

	"bytes"
	"encoding/binary"
	"encoding/json"
	"path/filepath"
	"time"
)

type CounterStats struct {
	Interface  string
	InOctets   float32
	InPackets  float32
	OutOctets  float32
	OutPackets float32
}

type Store struct {
	dataDir string
	dbs     map[string]*listmap.Listmap
}

func NewStore(dataDir string) *Store {
	return &Store{
		dataDir: dataDir,
		dbs:     make(map[string]*listmap.Listmap),
	}
}

func (s *Store) StoreCounters(inbound chan CounterStats) {
	for c := range inbound {
		lmap, present := s.dbs[c.Interface]
		if !present {
			lmap = listmap.OpenListmap(filepath.Join(s.dataDir, c.Interface))
			if lmap == nil {
				lmap = listmap.NewListmap(filepath.Join(s.dataDir, c.Interface))
			}

			s.dbs[c.Interface] = lmap
		}

		insertIntoMap(lmap, c)
	}
}

func (s *Store) FetchCounters(iface string, metrics []string, start, end uint64) []byte {
	result := map[string][]interface{}{}

	for _, metric := range metrics {
		result[metric] = []interface{}{}
	}

	lmap, present := s.dbs[iface]
	if !present {
		return nil
	}

	for c := lmap.NewCursor(); c != nil; c = c.Next() {
		var (
			ts  uint64
			val float32
		)
		binary.Read(bytes.NewReader(c.Key()), binary.BigEndian, &ts)
		binary.Read(bytes.NewReader(c.Value()), binary.LittleEndian, &val)

		if start != 0 && ts < start {
			continue
		}

		if end != 0 && ts > end {
			break
		}

		metric := string(c.Key()[8:])
		if _, present = result[metric]; present {
			result[metric] = append(result[string(c.Key()[8:])], []interface{}{ts, val})
		}

	}

	buf, err := json.Marshal(&result)
	if err != nil {
		return nil
	}

	return buf
}

func insertIntoMap(lmap *listmap.Listmap, stats CounterStats) {
	now := uint64(time.Now().UnixNano() / 1000)

	buffer := bytes.NewBuffer(nil)
	binary.Write(buffer, binary.BigEndian, &now)
	ts := buffer.Bytes()

	buffer = bytes.NewBuffer(nil)
	binary.Write(buffer, binary.LittleEndian, &stats.InOctets)
	lmap.Set(append(ts, []byte("oi")...), buffer.Bytes())

	buffer = bytes.NewBuffer(nil)
	binary.Write(buffer, binary.LittleEndian, &stats.OutOctets)
	lmap.Set(append(ts, []byte("oo")...), buffer.Bytes())

	buffer = bytes.NewBuffer(nil)
	binary.Write(buffer, binary.LittleEndian, &stats.InPackets)
	lmap.Set(append(ts, []byte("pi")...), buffer.Bytes())

	buffer = bytes.NewBuffer(nil)
	binary.Write(buffer, binary.LittleEndian, &stats.OutPackets)
	lmap.Set(append(ts, []byte("po")...), buffer.Bytes())
}
