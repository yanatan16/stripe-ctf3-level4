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
        Query   string `json:"query"`
}

// Creates a new write command.
func NewWriteCommand(query string) *WriteCommand {
        return &WriteCommand{
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
        return db.Execute(ctx.Server().State(), c.Query)
}