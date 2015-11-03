package query

import "fmt"
import "sort"
import "github.com/cznic/sortutil"
import "github.com/dgryski/go-tsz"


type Point struct {
  Timestamp uint64
  Value float64
}

type Output struct {
  Points []Point
}

type SeriesChunk struct {
  Timestamp uint64
  Data []byte
}

type QueryBackend interface {
  Discover(wildcard string) ([]string, error)
  Fetch(start, end uint64, series string) ([]SeriesChunk, error)
}

type QueryEngine struct {
  backend QueryBackend
}

func New(backend QueryBackend) *QueryEngine {
  engine := new(QueryEngine)
  engine.backend = backend
  return engine
}

func (qe *QueryEngine) query(start, end int64, wildcard string, samples int) {

}

type bucket []SeriesChunk

func average(chunks bucket) (output []Point) {
  iters := make([]*tsz.Iter, len(chunks))
  var timestamp uint32 = 0
  var err error

  for i, chunk := range chunks {
    iters[i], err = tsz.NewIterator(chunk.Data)
    if err != nil {
      return
    }
    if iters[i].Next() == false {
      iters[i] = nil
    } else {
      t, _ := iters[i].Values()
      if timestamp == 0|| t < timestamp {
        timestamp = t
      }
    }
  }

  for ;; {
    c := 0
    n := 0
    total := float64(0.0)
    next_timestamp := ^uint32(0)

    for i, iter := range iters {
      if iter == nil {
        continue
      }

      t, value := iter.Values()
      if t == timestamp {
        total += value
        n += 1
        next := iter.Next()

        if next {
          t, _ = iter.Values()
          if t < next_timestamp {
            next_timestamp = t
          }
        } else {
          iters[i] = nil
        }
      }

      c += 1
    }

    if c == 0 {
      break
    }

    value := float64(total) / float64(n)
    output = append(output, Point{Timestamp: uint64(timestamp), Value: value})

    timestamp = next_timestamp
  }

  return
}

func (qe *QueryEngine) Query(start, end uint64, wildcard string, samples int) (output Output, err error) {
  names, _ := qe.backend.Discover(wildcard)
  fmt.Printf("got %v\n", names)

  buckets := make(map[uint64]bucket)
  for _, name := range names {
    series, err := qe.backend.Fetch(start, end, name)
    if err != nil {
      continue
    }
    for _, serie := range series {
      buckets[serie.Timestamp] = append(buckets[serie.Timestamp], serie)
    }
  }

  fmt.Printf("BUCKETS: %v\n", buckets)

  // apply function

  // To store the keys in slice in sorted order
  var keys []uint64
  for k := range buckets {
    keys = append(keys, k)
  }
  sort.Sort(sortutil.Uint64Slice(keys))

  for _, timestamp := range keys {
    points := average(buckets[timestamp])
    output.Points = append(output.Points, points...)
  }

  // FIXME: filter out between start end end

  return
  //return qe.query(start, end, wildcard, samples)
}
