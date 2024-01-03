package database

import (
	"fmt"
	"go-redis/aof"
	"go-redis/config"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"strconv"
	"strings"
)

type Database struct {
	dbSet      []*DB
	aofHandler *aof.AofHandler
}

func NewDataBase() *Database {
	database := &Database{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}

	database.dbSet = make([]*DB, config.Properties.Databases)

	// init 16 dbs
	for i := range database.dbSet {
		db := MakeDB()
		db.index = i
		database.dbSet[i] = db
	}

	fmt.Println(config.Properties.AppendOnly)

	fmt.Println("check: ", config.Properties.AppendOnly == true)

	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAofHandler(database)
		if err != nil {
			panic(err)
		}

		database.aofHandler = aofHandler

		for _, db := range database.dbSet {

			// closure issue
			sdb := db
			sdb.addAof = func(line CmdLine) {
				database.aofHandler.AddAof(sdb.index, line)
			}
		}
	}

	return database
}

func (database *Database) Exec(client resp.Connection, args [][]byte) resp.Reply {

	defer func() {

		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()

	cmdName := strings.ToLower(string(args[0]))
	if cmdName == "select" {
		if len(args) != 2 {
			return reply.MakeArgNumErrReply("select")
		}

		return execSelect(client, database, args[1:])
	}

	dbIndex := client.GetDBIndex()
	db := database.dbSet[dbIndex]
	return db.Exec(client, args)

}

func (database *Database) Close() {
}

func (database *Database) AfterClientClose(c resp.Connection) {
}

// select 2
func execSelect(c resp.Connection, database *Database, args [][]byte) resp.Reply {

	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.MakeErrReply("Err invalid DB index")
	}

	if dbIndex >= len(database.dbSet) {
		reply.MakeErrReply("ERR DB index is out of range")
	}

	c.SelectDB(dbIndex)

	return reply.MakeOKReply()
}
