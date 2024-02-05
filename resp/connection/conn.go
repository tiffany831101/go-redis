package connection

import (
	"go-redis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

// each client has one connection..
type Connection struct {
	// current connection
	conn         net.Conn
	waitingReply wait.Wait

	// lock for the waiting Reply
	mu sync.Mutex
	// the selected db of redis
	selectedDB int
}

// return a new connection
func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
	}
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Connection) Close() error {
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}

// write the result back to the connection
func (c *Connection) Write(bytes []byte) error {
	if len(bytes) == 0 {
		return nil
	}

	c.mu.Lock()

	c.waitingReply.Add(1)

	defer func() {
		c.waitingReply.Done()
		c.mu.Unlock()
	}()

	_, err := c.conn.Write(bytes)
	return err
}

// using which db
func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

func (c *Connection) SelectDB(dbNum int) {
	c.selectedDB = dbNum
}
