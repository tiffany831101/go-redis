package dict

//

type Consumer func(key string, val interface{}) bool
type Dict interface {
	// use empty interface: each type can be saved into redis
	// get the value using the key
	Get(key string) (val interface{}, exists bool)
	Len() int
	// how many has being put into redis
	Put(key string, val interface{}) (result int)

	// if does not exists then put it into the redis
	PutIfAbsent(key string, val interface{}) (result int)

	PutIfExists(key string, val interface{}) (result int)

	Remove(key string) (result int)
	ForEach(consumer Consumer)

	// list all the keys
	Keys() []string

	RandomKeys(limit int) []string
	RandomDistinctKeys(limit int) []string
	// clear the dictionary
	Clear()
}
