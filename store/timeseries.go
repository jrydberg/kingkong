package store

import "sync"
//import "fmt"
import "github.com/dgryski/go-tsz"
import "github.com/jrydberg/kingkong/query"


type closedBlock struct {
  Timestamp uint32
  Data []byte
}

type timeSeries struct {
  sync.Mutex
  openBlock *tsz.Series
  closedBlocks []closedBlock
  startOpenBlock uint32
  width uint32
}

func NewTimeSeries(width uint32) *timeSeries {
  return &timeSeries{
    openBlock: nil,
    closedBlocks: []closedBlock{},
    width: width,
    startOpenBlock: 0,
  }
}

func (ts *timeSeries) Put(timestamp uint32, value float64) {
  ts.Lock()
  defer ts.Unlock()
  if ts.closeAndOpenBlock(timestamp) {
    return
  }
  ts.openBlock.Push(timestamp, value)
}

func (ts *timeSeries) size() int {
  size := 0
  for _, block := range ts.closedBlocks {
    size += len(block.Data)
  }
  if ts.openBlock != nil {
    size += len(ts.openBlock.BytesSoFar())
  }
  return size
}

func (ts *timeSeries) close() {
  ts.openBlock.Finish()
  newClosedBlock := closedBlock{
    Timestamp: ts.startOpenBlock,
    Data: ts.openBlock.Bytes(),
  }
  ts.closedBlocks = append(ts.closedBlocks, newClosedBlock)
  ts.openBlock = nil
}

func (ts *timeSeries) closeAndOpenBlock(timestamp uint32) bool {
  if (ts.openBlock != nil) {
    if (timestamp < ts.startOpenBlock) {
      // fmt.Printf("timestamp less then startOpenBlock\n")
      return true
    }

    close := (timestamp > (ts.startOpenBlock + ts.width));
    if close {
      // fmt.Printf("closing: %d (b/c %d)\n", ts.startOpenBlock, timestamp)
      ts.close()
    }
  }

  if ts.openBlock == nil {
    ts.startOpenBlock = (timestamp - (timestamp % ts.width));
    ts.openBlock = tsz.New(ts.startOpenBlock)
    // fmt.Printf("open new: %d\n", ts.startOpenBlock);
  }

  return false
}

func (ts *timeSeries) gatherBlocks(start, end uint32) (blocks []query.SeriesChunk) {
  ts.Lock()
  defer ts.Unlock()

  start = start - (start % ts.width)

  // FIXME: we can optimize here to see if we need to look through the
  // closed blocks at all. also binsearch, or search in reverse or something.

  if len(ts.closedBlocks) > 0 {
    l := len(ts.closedBlocks)
    i := 0

    for ; i < l; i++ {
      if start >= ts.closedBlocks[i].Timestamp {
        break
      }
    }

    for ; i < l; i++ {
      if ts.closedBlocks[i].Timestamp > end {
        break
      }

      block := query.SeriesChunk{
        Timestamp: uint64(ts.closedBlocks[i].Timestamp),
        Data: ts.closedBlocks[i].Data,
      }
      blocks = append(blocks, block)
    }
  }

  if ts.openBlock != nil {
    if end < ts.startOpenBlock {
      // not query the open block
      return
    }

    if start > (ts.startOpenBlock + ts.width) {
      // not query the open block
      return
    }

    block := query.SeriesChunk{
      Timestamp: uint64(ts.startOpenBlock),
      Data: ts.openBlock.BytesSoFar(),
    }
    blocks = append(blocks, block)
  }

  return
}

func (ts *timeSeries) iterate(start, end uint32, fp func(iter *tsz.Iter) error) error {
  ts.Lock()
  defer ts.Unlock()

  start = start - (start % ts.width)

  // FIXME: we can optimize here to see if we need to look through the
  // closed blocks at all. also binsearch, or search in reverse or something.

  if len(ts.closedBlocks) > 0 {
    l := len(ts.closedBlocks)
    i := 0

    for ; i < l; i++ {
      if start >= ts.closedBlocks[i].Timestamp {
        break
      }
    }

    for ; i < l; i++ {
      if ts.closedBlocks[i].Timestamp > end {
        break
      }

      iter, err := tsz.NewIterator(ts.closedBlocks[i].Data)
      if err != nil {
        return err
      }
      err = fp(iter)
      if err != nil {
        return err
      }
    }
  }

  if ts.openBlock != nil {
    if end < ts.startOpenBlock {
      // not query the open block
      return nil
    }

    if start > (ts.startOpenBlock + ts.width) {
      // not query the open block
      return nil
    }

    err := fp(ts.openBlock.Iter())
    return err
  }

  return nil
}
