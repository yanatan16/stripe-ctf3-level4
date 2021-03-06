package server

import (
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"stripe-ctf.com/sqlcluster/log"
	"stripe-ctf.com/sqlcluster/sql"
	"stripe-ctf.com/sqlcluster/transport"
	"stripe-ctf.com/sqlcluster/util"
	"stripe-ctf.com/sqlcluster/raft"
	"stripe-ctf.com/sqlcluster/raft/command"
	"regexp"
	"os"
	"math/rand"
)

type Server struct {
	name       string
	path string
	logpath       string
	listen     string
	router     *mux.Router
	httpServer *http.Server
	sql        *sql.SQL
	client     *transport.Client
	writeQuery  *regexp.Regexp
	consensus  *raft.Server
}

// Creates a new server.
func New(path, listen string) (*Server, error) {
	cs, err := transport.Encode(listen)
	if err != nil {
		return nil, err
	}

	sqlPath := filepath.Join(path, "storage.sql")
	util.EnsureAbsent(sqlPath)

	rq, err := regexp.Compile("(CREATE|INSERT|UPDATE)")
	if err != nil {
		return nil, err
	}

	logpath := filepath.Join(path, "raft")
	util.EnsureAbsent(logpath)
	if err := os.MkdirAll(logpath, 0744); err != nil {
		return nil, err
	}

	s := &Server{
		path:    path,
		logpath: logpath,
		listen:  listen,
		sql:     sql.NewSQL(sqlPath),
		router:  mux.NewRouter(),
		client:  transport.NewClient(),
		writeQuery: rq,
	}

	s.consensus, err = raft.New(path, logpath, cs, s.sql, s, s.client)

	return s, err
}

// Starts the server.
func (s *Server) ListenAndServe(primary string) error {
	var err error
	// Initialize and start HTTP server.
	s.httpServer = &http.Server{
		Handler: s.router,
	}

	if err = s.consensus.Init(primary); err != nil {
		return err
	}

	s.router.HandleFunc("/sql", s.sqlHandler).Methods("POST")
	s.router.HandleFunc("/fwd", s.fwdHandler).Methods("POST")
	s.router.HandleFunc("/join", s.joinHandler).Methods("POST")

	// Start Unix transport
	l, err := transport.Listen(s.listen)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("[%s] Ready to server requests!\n", s.consensus.Id())
	return s.httpServer.Serve(l)
}

// This is a hack around Gorilla mux not providing the correct net/http
// HandleFunc() interface.
func (s *Server) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
        s.router.HandleFunc(pattern, handler)
}

// Client operations

// Server handlers
func (s *Server) joinHandler(w http.ResponseWriter, req *http.Request) {
	if err := s.consensus.HandleJoin(req.Body); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(nil)
}

func (s *Server) forwardRequest(sqry string) (*sql.Output, error) {
	// http.Error(w, "Cannot serve requests from nonleader", http.StatusBadRequest)
	// return

	leader, err := s.consensus.LeaderCS()
	if err != nil {
		return nil, err
	}
	log.Println("Forwarding request to leader", leader)

	wc := &command.WriteCommand{
		Key: fmt.Sprintf("%s:%x", s.consensus.Name(), rand.Int() % 100000),
		Query: sqry,
	}
  b := util.JSONEncode(wc)

  if _, err := s.client.SafePost(leader, "/fwd", b); err != nil {
  	if _, ok := err.(*transport.RequestError); ok {
	  	return nil, err
	  }
	}

	keyer, err := s.sql.Recorder.Listen(wc.Key, 200)
	if err != nil {
		return nil, errors.New("Could not contact master!")
	} else {
		wr := keyer.(*command.WriteRecord)
		return wr.Output, wr.Error
	}
}

// This is the only user-facing function, and accordingly the body is
// a raw string rather than JSON.
func (s *Server) sqlHandler(w http.ResponseWriter, req *http.Request) {
	query, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Printf("Couldn't read body: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sqry := string(query)
	log.Debugf("[%s] Received query: %#v", s.consensus.Name(), sqry)

	var output *sql.Output
	state := s.consensus.State()
	if state == "follower" {
		output, err = s.forwardRequest(sqry)
	} else if state != "leader" {
		http.Error(w, "Not follower and not leader; cannot accept requests now.", http.StatusInternalServerError)
		return
	} else {
		output, err = s.consensus.Query(sqry)
	}

	resp, err := s.formatExec(sqry, output, err)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Debugf("[%s] Returning response to %#v: %#v", s.consensus.State(), string(query), string(resp))
	w.Write(resp)
}

func (s *Server) fwdHandler(w http.ResponseWriter, req *http.Request) {
	state := s.consensus.State()
	if state != "leader" {
		http.Error(w, "Not the leader; cannot accept forwarded requests now.", http.StatusInternalServerError)
		return
	}

	wc := &command.WriteCommand{}

  if err := util.JSONDecode(req.Body, wc); err != nil {
  	http.Error(w, err.Error(), http.StatusInternalServerError)
    return
  }

	_, err := s.consensus.Do(wc)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Write(nil)
}

func (s *Server) isWrite(query []byte) bool {
	return s.writeQuery.Find(query) != nil
}

func (s *Server) formatExec(query string, output *sql.Output, err error) ([]byte, error) {
	if err != nil {
		var msg string
		if output != nil && len(output.Stderr) > 0 {
			template := `Error executing %#v (%s)

SQLite error: %s`
			msg = fmt.Sprintf(template, query, err.Error(), util.FmtOutput(output.Stderr))
		} else {
			msg = err.Error()
		}

		return nil, errors.New(msg)
	}

	formatted := fmt.Sprintf("SequenceNumber: %d\n%s",
		output.SequenceNumber, output.Stdout)
	return []byte(formatted), nil
}
