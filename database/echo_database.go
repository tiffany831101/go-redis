package database

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

type EchoDatabase struct {
}

func NewEchoDatabase() *EchoDatabase {
	return &EchoDatabase{}
}

func (e EchoDatabase) Exec(client resp.Connection, args [][]byte) resp.Reply {
	return reply.MakeMultiBulkReply(args)
}

func (e EchoDatabase) Close() {
	panic("not implemented") // TODO: Implement
}

func (e EchoDatabase) AfterClientClose(c resp.Connection) {
	panic("not implemented") // TODO: Implement
}
