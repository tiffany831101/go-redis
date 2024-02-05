package list

import (
	"sync"
)

// is the dict.Dict type which is the interface
// this is the actual implement
type SyncList struct {
	// read does not need to sync, but when doing write no one can write now
	m sync.Map
}

func MakeSyncList() *SyncList {
	return &SyncList{}
}

func (list *SyncList) LRange(key string, start int, end int) (result []interface{}, ok bool) {
	value, _ := list.m.Load(key)

	// but have to check for the range of the start and end
	if slice, ok := value.([]interface{}); !ok {
		return []interface{}{}, ok
	} else {
		return slice[start:end], ok
	}

}

func (list *SyncList) LIndex(key string, index int) (result interface{}, ok bool) {

	value, _ := list.m.Load(key)

	slice, _ := value.([]interface{})

	if index >= len(slice) {
		return nil, false
	}

	return slice[index], true
}

func (list *SyncList) LPush(key string, val interface{}) (result int, ok bool) {
	value, ok := list.m.Load(key)

	if !ok {
		return 0, ok
	}

	slice, ok := value.([]interface{})

	if !ok {
		return 0, ok
	}

	l := []interface{}{val}

	slice = append(l, slice...)

	list.m.Store(key, slice)

	return len(slice), true
}

func (list *SyncList) LPop(key string) (result interface{}, ok bool) {

	value, ok := list.m.Load(key)

	if !ok {
		return nil, ok
	}

	slice, ok := value.([]interface{})

	if !ok {
		return nil, ok
	}

	if len(slice) <= 0 {
		return nil, false
	}

	firstElement := slice[0]
	slice = slice[1:]
	list.m.Store(key, slice)

	return firstElement, true

}

func (list *SyncList) RPush(key string, val interface{}) (result interface{}, ok bool) {

	value, ok := list.m.Load(key)
	if !ok {
		return nil, ok
	}

	slice, ok := value.([]interface{})

	if !ok {
		return nil, ok
	}

	slice = append(slice, val)
	list.m.Store(key, slice)

	return slice, ok
}

func (list *SyncList) RPop(key string) (result interface{}, ok bool) {
	value, ok := list.m.Load(key)
	if !ok {
		return nil, ok
	}

	slice, ok := value.([]interface{})

	if !ok {
		return nil, ok
	}

	lastElement := slice[len(slice)-1]

	list.m.Store(key, slice[:len(slice)-1])
	return lastElement, true

}

func (list *SyncList) Clear() {
	*list = *MakeSyncList()
}
