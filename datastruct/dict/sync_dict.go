package dict

import (
	"sync"
)

type SyncDict struct {
	m sync.Map
}

func MakeSyncDict() *SyncDict {
	return &SyncDict{}
}

// use empty interface: each type can be saved into redis
// get the value using the key
func (dict *SyncDict) Get(key string) (val interface{}, exists bool) {
	val, ok := dict.m.Load(key)
	return val, ok
}

func (dict *SyncDict) Len() int {

	length := 0
	dict.m.Range(func(key, value interface{}) bool {
		length++
		return true
	})

	return length
}

func (dict *SyncDict) Put(key string, val interface{}) (result int) {

	_, existed := dict.m.Load(key)
	dict.m.Store(key, val)
	// only edit no add new
	if existed {
		return 0
	}

	return 1

}

// if does not exists then put it into the redis
func (dict *SyncDict) PutIfAbsent(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key)

	if existed {
		return 0
	}

	dict.m.Store(key, val)
	return 1

}

func (dict *SyncDict) PutIfExists(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key)
	if existed {
		dict.m.Store(key, val)

		return 1

	}
	return 0
}

func (dict *SyncDict) Remove(key string) (result int) {

	_, existed := dict.m.Load(key)

	dict.m.Delete(key)
	if existed {
		return 1
	}

	return 0
}

func (dict *SyncDict) ForEach(consumer Consumer) {

	dict.m.Range(func(key, value interface{}) bool {
		consumer(key.(string), value)
		return true
	})

}

// list all the keys
func (dict *SyncDict) Keys() []string {
	result := make([]string, dict.Len())

	i := 0
	dict.m.Range(func(key, value interface{}) bool {
		result[i] = key.(string)
		i++

		// only true can iterate to the next one
		return true
	})
	return result
}

func (dict *SyncDict) RandomKeys(limit int) []string {

	result := make([]string, dict.Len())
	for i := 0; i < limit; i++ {

		// random pick one key
		dict.m.Range(func(key, value interface{}) bool {
			result[i] = key.(string)
			return false
		})
	}

	return result
}

func (dict *SyncDict) RandomDistinctKeys(limit int) []string {

	result := make([]string, dict.Len())

	i := 0
	dict.m.Range(func(key, value interface{}) bool {
		result[i] = key.(string)
		i++
		if i == limit {
			return false
		}

		return true

	})

	return result
}

// clear the dictionary
func (dict *SyncDict) Clear() {
	// the old one wll be gc by the system...more efficient than do by yourself
	*dict = *MakeSyncDict()
}

// call when need a syncdic
