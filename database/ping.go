package database

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

func Ping(db *DB, args [][]byte) resp.Reply {
	return reply.MakePongReply()
}

// write in anywhere then it will init when the package inits, eg: database package
// special kw
func init() {

	// PING
	RegisterCommand("ping", Ping, 1)

}
