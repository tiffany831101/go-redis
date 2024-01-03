package aof

import (
	"fmt"
	"go-redis/config"
	"go-redis/interface/database"
	"go-redis/lib/logger"
	"go-redis/lib/utils"
	"go-redis/resp/connection"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
	"io"
	"os"
	"strconv"
)

type CmdLine = [][]byte

const aofBufferSize = 1 << 16

type payload struct {
	cmdLine CmdLine
	dbIndex int
}

type AofHandler struct {
	database    database.Database
	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string
	currentDB   int
}

// newAofHandler
func NewAofHandler(database database.Database) (*AofHandler, error) {
	handler := &AofHandler{}
	handler.aofFilename = config.Properties.AppendFilename

	fmt.Println(handler.aofFilename)
	handler.database = database

	// loadaof
	handler.LoadAof()

	aofile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)

	if err != nil {
		return nil, err
	}

	handler.aofFile = aofile

	// channel: buffer
	handler.aofChan = make(chan *payload, aofBufferSize)

	go func() {
		handler.handleAof()
	}()
	return handler, nil
}

func (handler *AofHandler) AddAof(dbIndex int, cmd CmdLine) {

	if config.Properties.AppendOnly && handler.aofChan != nil {

		handler.aofChan <- &payload{
			cmdLine: cmd,
			dbIndex: dbIndex,
		}
	}
}

// handleAof payload

func (handler *AofHandler) handleAof() {
	// TODO
	handler.currentDB = 0
	for p := range handler.aofChan {
		if p.dbIndex != handler.currentDB {
			data := reply.MakeMultiBulkReply(utils.ToCmdLine("select", strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Error(err)
				continue
			}

			// change the handler currentdb to the p index
			handler.currentDB = p.dbIndex
		}

		data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
		_, err := handler.aofFile.Write(data)

		if err != nil {
			logger.Error(err)
		}

	}
}

// LoadAof
func (handler *AofHandler) LoadAof() {

	// only readonly
	file, err := os.Open(handler.aofFilename)
	if err != nil {
		logger.Error(err)
		return
	}

	defer file.Close()
	ch := parser.ParseStream(file)

	fakeConn := &connection.Connection{}
	for p := range ch {
		if p.Err != nil {
			// EOF
			if p.Err == io.EOF {
				break
			}
			// this line of command has error
			logger.Error(p.Err)
			continue
		}

		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}

		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("need Multi bulk")
			continue
		}

		rep := handler.database.Exec(fakeConn, r.Args)
		if reply.IsErrReply(rep) {
			logger.Error(rep)
		}

	}
}
