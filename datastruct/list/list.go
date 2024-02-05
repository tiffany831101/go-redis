package list

//

type Consumer func(key string, val interface{}) bool
type List interface {
	LRange(key string, start int, end int) (result []interface{}, ok bool)

	LIndex(key string, index int) (result interface{}, ok bool)

	LPush(key string, val interface{}) (result int, ok bool)

	LPop(key string) (result interface{}, ok bool)

	RPush(key string, val interface{}) (result interface{}, ok bool)
	RPop(key string) (result interface{}, ok bool)

	Clear()
}
