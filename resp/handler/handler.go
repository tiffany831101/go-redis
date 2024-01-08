package handler

import (
	"context"
	"fmt"
	"go-redis/cluster"
	"go-redis/config"
	"go-redis/database"
	databaseface "go-redis/interface/database"
	"go-redis/lib/logger"
	"go-redis/lib/sync/atomic"
	"go-redis/resp/connection"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
	"io"
	"net"
	"strings"
	"sync"
)

var (
	unknownErrReplyBytes = []byte("-Err unknown \r\n")
)

type RespHandler struct {
	activeConn sync.Map
	db         databaseface.Database
	closing    atomic.Boolean
}

func MakeHandler() *RespHandler {

	var db databaseface.Database
	// cluster mode
	if config.Properties.Self != "" && len(config.Properties.Peers) > 0 {

		db = cluster.MakeClusterDatabase()
	} else {
		// standalone mode
		db = database.NewStandaloneDataBase()

	}
	return &RespHandler{
		db: db,
	}
}

// close one client
func (r *RespHandler) closeClient(client *connection.Connection) {
	_ = client.Close()
	r.db.AfterClientClose(client)
	r.activeConn.Delete(client)
}

func (r *RespHandler) Handle(ctx context.Context, conn net.Conn) {

	if r.closing.Get() {
		_ = conn.Close()
	}

	client := connection.NewConn(conn)
	r.activeConn.Store(client, struct{}{})

	ch := parser.ParseStream(conn)

	for payload := range ch {

		// error
		if payload.Err != nil {
			// close the connection
			if payload.Err == io.EOF || payload.Err == io.ErrUnexpectedEOF || strings.Contains(payload.Err.Error(), "use of closed network connection") {

				r.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())

				return
			}

			// protocal error

			errReply := reply.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())

			if err != nil {
				r.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}

		// exec

		if payload.Data == nil {
			continue
		}

		reply, ok := payload.Data.(*reply.MultiBulkReply)
		fmt.Println("ok", ok)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		result := r.db.Exec(client, reply.Args)

		if result != nil {
			_ = client.Write(result.ToBytes())
		} else {
			client.Write(unknownErrReplyBytes)
		}
	}

}

// close the whole redis
func (r *RespHandler) Close() error {

	logger.Info("handler shutting down")

	r.closing.Set(true)
	r.activeConn.Range(
		func(key interface{}, value interface{}) bool {
			client := key.(*connection.Connection)
			_ = client.Close()
			return true
		},
	)

	r.db.Close()

	return nil
}
