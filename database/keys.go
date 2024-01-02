package database

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"

	"go-redis/lib/wildcard"
)

// DEL
func execDel(db *DB, args [][]byte) resp.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}

	deleted := db.Removes(keys...)
	return reply.MakeIntReply(int64(deleted))
}

//ExISTS

func execExists(db *DB, args [][]byte) resp.Reply {

	result := int64(0)

	for _, arg := range args {
		key := string(arg)
		_, exists := db.GetEntity(key)
		if exists {
			result++
		}
	}

	return reply.MakeIntReply(result)
}

//FLUSHDB

func execFlushDB(db *DB, args [][]byte) resp.Reply {

	db.Flush()
	return reply.MakeOKReply()
}

// TYPE: check the key's type
// TYPE k1
func execType(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeStatusReply("none")
	}

	switch entity.Data.(type) {
	case []byte:
		return reply.MakeStatusReply("string")
	}

	// TODO can do other data types...
	return &reply.UnknowErrReply{}
}

// RENAME: change the keyname
// RENAME k1 k2
func execRename(db *DB, args [][]byte) resp.Reply {

	src := string(args[0])
	dest := string(args[1])

	entity, exists := db.GetEntity(src)

	if !exists {
		return reply.MakeErrReply("no such key")
	}

	db.PutEntity(dest, entity)
	db.Remove(src)

	return reply.MakeOKReply()
}

//RENAMENX

func execRenamenx(db *DB, args [][]byte) resp.Reply {

	src := string(args[0])
	dest := string(args[1])

	_, ok := db.GetEntity(dest)

	if ok {
		// nothing to do
		return reply.MakeIntReply(0)
	}

	entity, exists := db.GetEntity(src)

	if !exists {
		return reply.MakeErrReply("no such key")
	}

	db.PutEntity(dest, entity)
	db.Remove(src)

	return reply.MakeIntReply(1)
}

// KEYS *
func execKeys(db *DB, args [][]byte) resp.Reply {
	pattern, _ := wildcard.CompilePattern(string(args[0]))
	result := make([][]byte, 0)

	db.data.ForEach(func(key string, val interface{}) bool {
		if pattern.IsMatch(key) {
			result = append(result, []byte(key))
		}

		return true
	})
	return reply.MakeMultiBulkReply(result)

}

func init() {
	// negative number is not a fixed len of the args
	RegisterCommand("DEL", execDel, -2)
	RegisterCommand("EXISIS", execExists, -2)
	RegisterCommand("flushdb", execFlushDB, -1)
	RegisterCommand("Type", execType, 2)
	RegisterCommand("RENAME", execRename, 3)
	RegisterCommand("RENAMENX", execRenamenx, 3)
	RegisterCommand("KEYS", execKeys, 2)

}
