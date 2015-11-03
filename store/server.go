package store

import "fmt"
import "io"
import "io/ioutil"
import "github.com/dgryski/go-tsz"
import "net/http"
import "encoding/json"
import "errors"

type StoreServer struct {
  store *SimpleStore
}

func NewStoreServer() *StoreServer {
  server := new(StoreServer)
  server.store = NewSimple()
  return server
}

type Point struct {
  Timestamp uint32
  Value float64
}

func (p Point) MarshalJSON() ([]byte, error) {
  return []byte(fmt.Sprintf("[%d, %f]", p.Timestamp, p.Value)), nil
}

func (p Point) UnmarshalJSON(b []byte) error {
  var v[]float64

  err := json.Unmarshal(b, &v)
  if err != nil {
    return err
  }
  if len(v) != 2 {
    return errors.New("NO")
  }
  p.Timestamp = uint32(v[0])
  p.Value = v[1]
  return nil
}

func (server *StoreServer) Put(name string, timestamp uint32, value float64) {
  server.store.Put(name, timestamp, value)
}

func (server *StoreServer) BytesConsumed() int {
  return server.store.bytesConsumed()
}

func (server *StoreServer) Query(w http.ResponseWriter, r *http.Request) {
  body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
  if err != nil {
    panic(err)
  }
  if err := r.Body.Close(); err != nil {
    panic(err)
  }

  var requestBody struct {
    Timeseries []string `json:"time_series"`
    Start uint32 `json:"start"`
    End uint32 `json:"end"`
  }

  err = json.Unmarshal(body, &requestBody);
  if err != nil {
    w.Header().Set("Content-Type", "application/json; charset=UTF-8")
    w.WriteHeader(422) // unprocessable entity
    return
  }

  fmt.Printf("QUERY: %v\n", requestBody)

  var responseBody struct {
    Timeseries map[string][]Point `json:"time_series"`
  }
  responseBody.Timeseries = make(map[string][]Point)

  for _, name := range requestBody.Timeseries {
    points := make([]Point, 0)
    server.query(name, requestBody.Start, requestBody.End, func(timestamp uint32, value float64) {
      points = append(points, Point{timestamp, value})
    })
    responseBody.Timeseries[name] = points
  }

  w.Header().Set("Content-Type", "application/json; charset=UTF-8")
  w.WriteHeader(200)
  err = json.NewEncoder(w).Encode(responseBody)
  if err != nil {
    panic(err)
  }
}

func (server *StoreServer) query(name string, start, end uint32, fp func(t uint32, v float64)) {
  ts := server.store.getTimeSeries(name)
  if ts == nil {
    return
  }
  ts.iterate(start, end, func(iter *tsz.Iter) error {
      for ;; {
        has_data := iter.Next()
        if !has_data {
          return nil
        }
        timestamp, value := iter.Values()
        if timestamp < start || timestamp > end {
          continue
        }
        fp(timestamp, value)
      }
    })
}
