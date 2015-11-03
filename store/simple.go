package store

import "sync"
import "errors"
//import "fmt"
import "github.com/jrydberg/kingkong/query"
import "github.com/ryanuber/go-glob"

const TIMESERIES_WIDTH = 2 * 60 * 60;

type SimpleStore struct {
  sync.Mutex
  series map[string]*timeSeries
  width uint32
}

func NewSimple() *SimpleStore {
  s := new(SimpleStore)
  s.series = make(map[string]*timeSeries)
  s.width = TIMESERIES_WIDTH
  return s
}

func (s *SimpleStore) bytesConsumed() int {
  size := 0
  for _, series := range s.series {
    size += series.size()
  }
  return size
}

func (s *SimpleStore) Put(name string, timestamp uint32, value float64) {
  s.Lock()
  series, ok := s.series[name]
  if !ok {
    series = NewTimeSeries(s.width)
    s.series[name] = series
  }
  s.Unlock()

  series.Put(timestamp, value)
}

func (s *SimpleStore) Discover(wildcard string) ([]string, error) {
  s.Lock()
  defer s.Unlock()

  var result []string = []string{}

  for name, _ := range s.series {
    match := glob.Glob(wildcard, name)
    if match {
      result = append(result, name)
    }
  }

  return result, nil
}

func (s *SimpleStore) getTimeSeries(name string) *timeSeries {
  return s.series[name]
}

var NO_SUCH_SERIES = errors.New("No such series")

func (s *SimpleStore) Fetch(start, end uint64, name string) ([]query.SeriesChunk, error) {
  s.Lock()
  timeSeries, ok := s.series[name]
  s.Unlock()
  if !ok {
    return nil, NO_SUCH_SERIES
  }

  return timeSeries.gatherBlocks(uint32(start), uint32(end)), nil
}
