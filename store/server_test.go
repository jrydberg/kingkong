package store

import "testing"
//import "github.com/jrydberg/kingkong/query"
import "fmt"
//import "net/http"
import "math/rand"

func BenchmarkCompressionTime(b *testing.B) {
  server := NewStoreServer()
  w := 2*1440
  for i := 0; i < b.N; i++ {
    name := fmt.Sprintf("test.foo.cpu.%d", i)
    i := 0
    for ; i < w; i++ {
      j := 0
      if rand.Int31n(10) >= 8 {
        j = 1
      }
      if rand.Int31n(10) >= 9 {
        j = 2
      }
      server.Put(name, uint32(60 * i + j), float64(1000 + rand.Int31n(10)))
    }
  }
}

func BenchmarkDecompressionTime(b *testing.B) {
  server := NewStoreServer()

  w := 2*1440

  i := 0
  for ; i < w; i++ {
    j := 0
    if rand.Int31n(10) >= 8 {
      j = 1
    }
    if rand.Int31n(10) >= 9 {
      j = 2
    }
    server.Put("test.foo.cpu", uint32(60 * i + j), float64(1000 + rand.Int31n(10)))
  }

  b.ResetTimer()
  for i := 0; i < b.N; i++ {
    server.query("test.foo.cpu", 0, uint32(60 * 1000000 * 2), func(t uint32, v float64) {
    })
  }
}
