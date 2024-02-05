package database

import (
	"go-redis/datastruct/dict"
	"go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
	"strings"
)

// each db is a sync.map and index 16 dbs
type DB struct {
	index int
	//
	stringData dict.Dict
	addAof     func(CmdLine)
}

type ExecFunc func(db *DB, args [][]byte) resp.Reply

type CmdLine = [][]byte

// creat db
func MakeDB() *DB {
	db := &DB{
		stringData: dict.MakeSyncDict(),
		addAof:     func(line CmdLine) {},
	}
	return db
}

func (db *DB) Exec(c resp.Connection, cmdLine CmdLine) resp.Reply {
	// PING SET SETNX

	cmdName := strings.ToLower(string(cmdLine[0]))

	cmd, ok := cmdTable[cmdName]

	if !ok {
		return reply.MakeErrReply("Err Unknown command " + cmdName)
	}

	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}

	fun := cmd.executor
	// do not need the keyword of the method
	return fun(db, cmdLine[1:])

}

func validateArity(arity int, cmdArgs [][]byte) bool {

	argNum := len(cmdArgs)

	if arity >= 0 {
		return argNum == arity
	}

	// EXISTS k1 k2...kn not the fixed length of the args

	return argNum >= -arity
}

func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	raw, ok := db.stringData.Get(key)

	if !ok {
		return nil, false
	}

	entity, _ := raw.(*database.DataEntity)

	return entity, true
}

func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	return db.stringData.Put(key, entity)
}

func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.stringData.PutIfAbsent(key, entity)
}

func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.stringData.PutIfExists(key, entity)
}

func (db *DB) Remove(key string) {
	db.stringData.Remove(key)
}

func (db *DB) Removes(keys ...string) (deleted int) {

	deleted = 0
	for _, key := range keys {

		_, exists := db.stringData.Get(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}

	return deleted

}

func (db *DB) Flush() {
	db.stringData.Clear()
}
