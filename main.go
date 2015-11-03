package main

import (
    //"fmt"
    "log"
    "net/http"
    "github.com/jrydberg/kingkong/store"
    "github.com/gorilla/mux"
)

func main() {
  server := store.NewStoreServer()
  router := mux.NewRouter().StrictSlash(true)
  router.HandleFunc("/query", server.Query)
  log.Fatal(http.ListenAndServe(":8080", router))
}
