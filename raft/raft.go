package raft

// Lets use raft!

import (
  "github.com/goraft/raft"
  "stripe-ctf.com/sqlcluster/raft/command"
  "stripe-ctf.com/sqlcluster/sql"
  "stripe-ctf.com/sqlcluster/transport"
  "stripe-ctf.com/sqlcluster/util"
  "stripe-ctf.com/sqlcluster/log"
  "fmt"
  "errors"
  "io"
)

type Server struct {
  raft.Server
  connectionString string
  client *transport.Client
}

func New(name, path, connstr string, db *sql.SQL, mux raft.HTTPMuxer, client *transport.Client) (*Server, error) {
  log.Println("Initializing Raft Server:", name, path, connstr)

  if log.Verbose() {
    raft.SetLogLevel(raft.Trace)
  }

  transporter := raft.NewHTTPTransporter("/raft")
  transporter.Transport.Dial = client.Transport().Dial
  // transporter.DisableKeepAlives = true
  // transporter.Transport.DisableKeepAlives = true

  raftServer, err := raft.NewServer(name, path, transporter, nil, db, connstr)
  if err != nil {
    return nil, err
  }

  transporter.Install(raftServer, mux)

  if err := raftServer.Start(); err != nil {
    return nil, err
  }

  s := &Server{
    Server: raftServer,
    connectionString: connstr,
    client: client,
  }

  s.AddListeners()

  return s, nil
}

func (s *Server) Init(leader string) error {
  if !s.IsLogEmpty() {
    log.Println("Recovered from log!")
    return nil
  }

  if leader != "" {
    log.Println("Attempting to join leader:", leader)

    return s.Join(leader)
  } else {
    log.Println("Initializing new cluster")

    jc := &raft.DefaultJoinCommand{
      Name: s.Name(),
      ConnectionString: s.connectionString,
    }

    _, err := s.Server.Do(jc)
    log.Println("Joined myself... lol")
    return err
  }
}

func (s *Server) Query(query string) (*sql.Output, error) {
  wc := command.NewWriteCommand(query)

  log.Debugf("[%s] Executing write command: %v", s.State(), wc)

  out, err := s.Server.Do(wc)
  if err != nil {
    return nil, err
  }

  if output, ok := out.(*sql.Output); ok {
    return output, nil
  } else {
    return nil, errors.New(fmt.Sprintf("Output could not be casted to sql.output! %v", output))
  }
}

func (s *Server) Join(leader string) error {
  jc := &raft.DefaultJoinCommand{
    Name: s.Name(),
    ConnectionString: s.connectionString,
  }

  b := util.JSONEncode(jc)

  cs, err := transport.Encode(leader)
  if err != nil {
    return err
  }

  if _, err = s.client.SafePost(cs, "/join", b); err != nil {
    return err
  }

  return nil
}

func (s *Server) HandleJoin(body io.Reader) error {
  jc := &raft.DefaultJoinCommand{}

  if err := util.JSONDecode(body, jc); err != nil {
    return err
  }

  log.Println("Got join command from:", jc.Name, jc.ConnectionString)

  if _, err := s.Server.Do(jc); err != nil {
    return err
  }

  return nil
}

func (s *Server) Id() string {
  return s.Name() + ":" + s.State()
}

func (s *Server) AddListeners() {
  s.AddEventListener(raft.AddPeerEventType, raft.EventListener(func (e raft.Event) {
    log.Printf("[%s] Peer joined: %s\n", s.Name(), e.Value())
  }))

  s.AddEventListener(raft.RemovePeerEventType, raft.EventListener(func (e raft.Event) {
    log.Printf("[%s] Peer left: %s\n", s.Name(), e.Value())
  }))

  s.AddEventListener(raft.LeaderChangeEventType, raft.EventListener(func (e raft.Event) {
    log.Printf("[%s] Leader changed: %s\n", s.Name(), e.Value())
  }))

  s.AddEventListener(raft.StateChangeEventType, raft.EventListener(func (e raft.Event) {
    log.Printf("[%s] State changed: %s\n", s.Name(), e.Value())
  }))
}