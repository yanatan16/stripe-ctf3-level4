// Package recorder provides an object to record events and associated output and listen for these events
package recorder

import (
  "testing"
  "time"
)

type TestKeyer string
func (tk TestKeyer) Key() string {
  return string(tk)
}

func TestBasic(t *testing.T) {
  r := New(0)
  result := make(chan bool, 0)
  go func () {
    k, err := r.Listen("hello", 0)
    if err != nil {
      t.Error(err)
    } else if string(k.(TestKeyer)) != "hello" {
      t.Error("bad key " + string(k.(TestKeyer)))
    } else {
      result <- true
    }
  }()
  r.Push(TestKeyer("hello"))

  select {
  case <- result:
  case <- time.After(10 * time.Millisecond):
    t.Error("Timeout!?")
  }
}

func TestRecent(t *testing.T) {
  r := New(1)
  r.Push(TestKeyer("world"))

  result := make(chan bool, 0)
  go func () {
    k, err := r.Listen("world", 0)
    if err != nil {
      t.Error(err)
    } else if string(k.(TestKeyer)) != "world" {
      t.Error("bad key " + string(k.(TestKeyer)))
    } else {
      result <- true
    }
  }()

  select {
  case <- result:
  case <- time.After(10 * time.Millisecond):
    t.Error("Timeout!?")
  }
}

func TestTimeout(t *testing.T) {
  r := New(1)

  result := make(chan bool, 0)
  go func () {
    _, err := r.Listen("world", 2)
    if err != nil {
      result <- true
    } else {
      t.Error("shoulda been an error!")
    }
  }()
  <- time.After(5*time.Millisecond)
  r.Push(TestKeyer("akf"))
  r.Push(TestKeyer("fka"))
  r.Push(TestKeyer("ghi"))

  select {
  case <- result:
  case <- time.After(10 * time.Millisecond):
    t.Error("Timeout!?")
  }
}