package tcp

import (
	"context"
	"net"
)

// abstract how the tcp connection is handle and close
type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}
