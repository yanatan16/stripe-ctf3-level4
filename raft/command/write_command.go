package command

import (
        "github.com/goraft/raft"
        "stripe-ctf.com/sqlcluster/sql"
        "stripe-ctf.com/sqlcluster/log"
)

func init() {
        raft.RegisterCommand(&WriteCommand{})
}

// This command writes a value to a key.
type WriteCommand struct {
        Key string `json:"key"`
        Query   string `json:"query"`
}

// Creates a new write command.
func NewWriteCommand(key, query string) *WriteCommand {
        return &WriteCommand{
                Key: key,
                Query: query,
        }
}

// The name of the command in the log.
func (c *WriteCommand) CommandName() string {
        return "write"
}

// Writes a value to a key.
func (c *WriteCommand) Apply(ctx raft.Context) (interface{}, error) {
        if log.Verbose() {
                log.Debugf("[%s] Applying WriteCommand %v\n", ctx.Server().State(), c)
        }
        db := ctx.Server().Context().(*sql.SQL)
        output, err := db.Execute(c.Key, ctx.Server().State(), c.Query)

        wr := &WriteRecord{c.Key, output, err}
        db.Recorder.Push(wr)

        return output, err
}

type WriteRecord struct {
        KeyS string
        Output *sql.Output
        Error error
}
func (wr *WriteRecord) Key() string {
        return wr.KeyS
}