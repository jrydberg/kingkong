package main

import (
    "fmt"
    //"log"
    //"net/http"
    "strings"
    "os"
    "bufio"
    "sort"
    "encoding/json"
    "github.com/jrydberg/kingkong/store"
    "hash/adler32"
    //"github.com/gorilla/mux"
)

type entry struct {
  Key string `json:"key"`
  Time uint64 `json:"time"`
  Value float64 `json:"value"`
  Attributes map[string]string `json:"attributes"`
}

func (e entry) timeSeriesName() string {
  keys := make([]string, 0)
  for key, value := range e.Attributes {
    keys = append(keys, fmt.Sprintf("%s=%s", key, value))
  }
  sort.Strings(keys)
  keys = append([]string{e.Key}, keys...)
  return strings.Join(keys, ".")
}

const NUM_SHARDS = 64

func main() {
  servers := make([]*store.StoreServer, NUM_SHARDS)
  i := 0
  for i = 0; i < NUM_SHARDS; i++ {
    servers[i] = store.NewStoreServer()
  }
  //server := store.NewStoreServer()

  var e entry
  var firstTime uint64

  scanner := bufio.NewScanner(os.Stdin)
  c := 0
  n := 0
  for scanner.Scan() {
    err := json.Unmarshal([]byte(scanner.Text()), &e)
    if err != nil {
      n += 1
      continue
    }
    if firstTime == 0 {
        firstTime = e.Time
    }
    name := e.timeSeriesName()
    shard := adler32.Checksum([]byte(name)) % NUM_SHARDS
    servers[shard].Put(e.timeSeriesName(), uint32(e.Time / 1000), e.Value)
    c += 1
  }

  fmt.Printf("first_time: %d  last_time: %d  delta: %d\n", firstTime / 1000, e.Time / 1000,
            (e.Time - firstTime) / 1000)

  total_bytes, total_series := uint64(0), 0

  for i = 0; i < NUM_SHARDS; i++ {
    bytes_consumed := servers[i].BytesConsumed()
    num_time_series := servers[i].NumTimeSeries()
    fmt.Printf("%02d: bytes=%d #series=%d\n", i, bytes_consumed, num_time_series);
    total_bytes += uint64(bytes_consumed)
    total_series += num_time_series
  }

  fmt.Printf("total: bytes: %d  series: %d   count: %d\n", total_bytes, total_series, c)
  //router := mux.NewRouter().StrictSlash(true)
  //router.HandleFunc("/query", server.Query)
  //log.Fatal(http.ListenAndServe(":8080", router))
}
