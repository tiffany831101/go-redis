package resp

type Connection interface {
	Write([]byte) error

	// using which db
	GetDBIndex() int

	SelectDB(int)
}
