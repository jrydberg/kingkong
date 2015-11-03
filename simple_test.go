package main

import "testing"
//import "github.com/jrydberg/kingkong/query"
import "github.com/jrydberg/kingkong/store"
import "gopkg.in/jmcvetta/napping.v3"
import "fmt"
//import "net/http"
import "net/http/httptest"
import "github.com/gorilla/mux"


func TestWalkingSkeleton(t *testing.T) {
  server := store.NewStoreServer()
  server.Put("test.foo.cpu", 60, 10)
  server.Put("test.bar.cpu", 60, 20)
  server.Put("test.bar.cpu", 70, 20)
  server.Put("test.baz.cpu", 60, 20)

  router := mux.NewRouter().StrictSlash(true)
  router.HandleFunc("/", server.Query)

  ts := httptest.NewServer(router)
  defer ts.Close()

  var requestBody struct {
    TimeSeries []string `json:"time_series"`
    Start uint32 `json:"start"`
    End uint32 `json:"end"`
  }

  requestBody.TimeSeries = []string{"test.foo.cpu", "test.bar.cpu"}
  requestBody.Start = 0
  requestBody.End = 10000

  var responseBody struct {
    TimeSeries map[string][]store.Point `json:"time_series"`
  }

  session := napping.Session{}
  session.Log = true

  res, _ := session.Post(ts.URL, &requestBody, &responseBody, nil)

  //fmt.Printf("%s\n", res.ResponseBody.Bytes())

  fmt.Printf("WHAT: %v\n", res)
  fmt.Printf("yes: %v\n", responseBody)
}
