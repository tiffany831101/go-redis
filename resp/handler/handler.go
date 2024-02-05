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

	// make a new connection

	/*
		{
			conn: net.Conn
			wait:
			mutex: lock for the waitgroup
			selecteddb
		}
	*/
	client := connection.NewConn(conn)
	// store an empty struct

	// this map need to be synchronization
	// only to check if the client is in the map, not to store any actual data
	r.activeConn.Store(client, struct{}{})

	// parse the request and put the result into the channel
	ch := parser.ParseStream(conn)

	// get the payload from the channel
	for payload := range ch {

		// if there is an error from the payload
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

		// execute the command in the db now
		result := r.db.Exec(client, reply.Args)

		if result != nil {
			// write the result back to the client
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
