package reply

type PongReply struct {
}

var pongbytes = []byte("+PONG\r\n")

func (r PongReply) ToBytes() []byte {
	return pongbytes
}

func MakePongReply() *PongReply {
	return &PongReply{}
}

type OKReply struct {
}

var okbytes = []byte("+OK\r\n")

func (r *OKReply) ToBytes() []byte {
	return okbytes
}

var theOKReply = new(OKReply)

func MakeOKReply() *OKReply {
	return theOKReply
}

type NullBulkReply struct {
}

var nullBulkBytes = []byte("$-1\r\n")

func (n NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

var emptyMultiBulkBytes = []byte("*0\r\n")

type EmptyMultiBulkReply struct{}

func (e EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

var noBytes = []byte("")

type NoReply struct {
}

func (n NoReply) ToBytes() []byte {
	return noBytes
}
