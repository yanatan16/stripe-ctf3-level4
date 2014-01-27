// Package recorder provides an object to record events and associated output and listen for these events
package recorder

import (
  "fmt"
  "stripe-ctf.com/sqlcluster/log"
)

type Keyer interface {
  Key() string
}

type Listen struct {
  Key string
  Timeout int
  C chan Keyer
}

type Recorder struct {
  incoming chan Keyer
  recent []Keyer
  listeners chan *Listen
}

func New(nrecent int) *Recorder {
  r := &Recorder{
    incoming: make(chan Keyer, 5),
    recent: make([]Keyer, nrecent),
    listeners: make(chan *Listen),
  }
  go r.listen()
  return r
}

func (r *Recorder) Push(k Keyer) {
  log.Debugf("Recorder: Pushing %s", k.Key())
  r.incoming <- k
}

func (r *Recorder) Listen(key string, timeout int) (Keyer, error) {
  log.Debugf("Recorder: Listenening for %s", key)
  l := &Listen{key, timeout, make(chan Keyer, 0)}
  r.listeners <- l
  if k, ok := <- l.C; ok {
    log.Debugf("Recorder: Got result for %s: %v", key, k)
    return k, nil
  } else {
    return nil, fmt.Errorf("No Record found after %d records passed.", timeout)
  }
}

func (r *Recorder) listen() {
  lists := make([]*Listen, 0)

ListenLoop:
  for {
    select {
    case l := <- r.listeners:
      for _, k := range r.recent {
        if k != nil && l.Key == k.Key() {
          l.C <- k
          continue ListenLoop
        }
      }
      lists = append(lists, l)
    case k := <- r.incoming:
      r.recent = append(r.recent, k)
      for i := len(lists) - 1; i >= 0; i-- {
        l := lists[i]
        if l.Key == k.Key() {
          l.C <- k
          lists = append(lists[:i], lists[i+1:]...)
        } else if l.Timeout <= 1 {
          close(l.C)
          lists = append(lists[:i], lists[i+1:]...)
        } else {
          l.Timeout -= 1
        }
      }
    }
  }
}