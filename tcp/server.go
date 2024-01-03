package tcp

import (
	"context"
	"go-redis/interface/tcp"
	"go-redis/lib/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// tcp config
type Config struct {
	Address string
}

func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {

	// close the connection use signal
	closeChan := make(chan struct{})

	// send os signal os level send to the thread
	sigChan := make(chan os.Signal)

	// notify when the system send the signal
	// runtime will monitor if the system gets the syscall signal send to sigChan
	signal.Notify(sigChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)

	go func() {

		sig := <-sigChan

		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			// send an empty struct to close the listener
			closeChan <- struct{}{}
		}

	}()

	listener, err := net.Listen("tcp", cfg.Address)

	if err != nil {
		return err
	}

	logger.Info("start listen")

	ListenAndServe(listener, handler, closeChan)

	return nil
}

func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {

	go func() {

		// empty struct can be a signal
		<-closeChan

		logger.Info("shutting down")
		// listen for socket
		_ = listener.Close()

		_ = handler.Close()
	}()

	defer func() {
		_ = listener.Close()
		_ = handler.Close()
	}()

	ctx := context.Background()
	// accept new request

	var waitDone sync.WaitGroup
	for true {

		// blocking call for waiting for the client for the connection
		conn, err := listener.Accept()

		if err != nil {
			break
		}

		logger.Info("Accept link")

		waitDone.Add(1)
		go func() {

			// will run this when the handler.Handle has done all of its job
			defer func() {
				waitDone.Done()
			}()
			handler.Handle(ctx, conn)

		}()

	}

	// wait for all the request has done the job
	waitDone.Wait()
}
