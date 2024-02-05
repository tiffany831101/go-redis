package parser

import (
	"bufio"
	"errors"
	"fmt"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

type Payload struct {
	Data resp.Reply
	Err  error
}

type readState struct {
	readingMultiLine  bool
	expectedArgsCount int
	msgType           byte
	args              [][]byte
	bulkLen           int64
}

func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)

	// use goroutine to parse the stream..for each client
	go parse0(reader, ch)
	return ch
}

func parse0(reader io.Reader, ch chan<- *Payload) {

	defer func() {

		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()

	bufReader := bufio.NewReader(reader)
	// fmt.Println(bufReader)
	var state readState
	var err error
	var msg []byte

	// the loop will keep until
	for true {
		var ioErr bool
		msg, ioErr, err = readLine(bufReader, &state)
		fmt.Println("msg; ", msg)

		if err != nil {
			if ioErr {
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}

			ch <- &Payload{
				Err: err,
			}

			state = readState{}
			continue
		}

		// only a single line
		if !state.readingMultiLine {

			// * is a array => get the elememt

			// each msg only represents one line, when user press enter this is the new msg
			if msg[0] == '*' {
				err := parseMultiBulkHeader(msg, &state)

				fmt.Println(err)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}

					state = readState{}
					continue
				}

				// this is an empty bulk reply
				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: &reply.EmptyMultiBulkReply{},
					}

					state = readState{}
					continue
				}
			} else if msg[0] == '$' {
				err := parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}

					// reset the state
					state = readState{}
					continue
				}

				if state.bulkLen == -1 {
					ch <- &Payload{
						Data: &reply.NullBulkReply{},
					}

					// reset the state
					state = readState{}
					continue
				}

			} else {
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}

				state = readState{}
				continue
			}
		} else {
			// start with * to represent multiple lines
			// if has multilines => which is the *3
			// so it keeps putting the arguments into the arg slice

			err := readBody(msg, &state)
			// if there is an error
			if err != nil {
				ch <- &Payload{
					Err: errors.New("protocol error: " + string(msg)),
				}

				// reset the state
				state = readState{}
				continue
			}

			// has enough
			if state.finished() {
				var result resp.Reply

				if state.msgType == '*' {
					result = reply.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = reply.MakeBulkReply(state.args[0])
				}

				ch <- &Payload{
					Data: result,
					Err:  err,
				}

				state = readState{}
			}
		}
	}
}

// readline
// it just read the line => and send the message back it does not write any data
// it just read the line and return the read msg
// if the line is read
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {

	var msg []byte
	var err error

	if state.bulkLen == 0 {
		msg, err = bufReader.ReadBytes('\n')

		if err != nil {
			return nil, true, err
		}

		// fmt.Println(msg[len(msg)-1] == '\r')

		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}

	} else {
		msg = make([]byte, state.bulkLen+2)
		_, err := io.ReadFull(bufReader, msg)

		if err != nil {
			return nil, true, err
		}

		if len(msg) == 0 || msg[len(msg)-2] != '\r' || msg[len(msg)-1] != '\n' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}

		state.bulkLen = 0

	}

	return msg, false, nil

}

// parsemultibulkheader
// for the one that start with the "*" => needs to use the multibulkreader to parse
func parseMultiBulkHeader(msg []byte, state *readState) error {

	var err error
	var expectedLine uint64

	// expectedLine is the line for *3 like this will be 3 lines
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 64)

	if err != nil {

		fmt.Println("Error parsing expectedLine:", err)

		return errors.New("protocol error: " + string(msg))
	}

	// no line
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {

		// msgType = "*"
		state.msgType = msg[0]

		// has multiple lines
		state.readingMultiLine = true

		// the number after * which is the expected Arg counts
		state.expectedArgsCount = int(expectedLine)

		// args is a 2-dimentional splice, and
		state.args = make([][]byte, 0, expectedLine)
		return nil

	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if state.bulkLen == -1 {
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

// +OK -err
func parseSingleLineReply(msg []byte) (resp.Reply, error) {

	str := strings.TrimSuffix(string(msg), "\r\n")

	var result resp.Reply

	switch msg[0] {
	case '+':
		result = reply.MakeStatusReply(str[1:])
	case '-':
		result = reply.MakeErrReply(str[1:])
	case ':':
		val, err := strconv.ParseInt(str[1:], 10, 64)

		if err != nil {
			return nil, errors.New("protocol error: " + string(msg))

		}
		result = reply.MakeIntReply(val)
	}

	return result, nil
}

func readBody(msg []byte, state *readState) error {
	// hello/r/n so the last two does not want => readbody means to get the
	// since line is the byte slice, so it dipose the last 2 elements
	// $2/r/n so it only retrives $2
	// hello/r/n it only retrieve the hello
	line := msg[0 : len(msg)-2]
	var err error

	// so check if it is telling the next words length or the value
	if line[0] == '$' {
		// bulklen is the length of the word
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)

		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}

		if state.bulkLen <= 0 {
			// append an empty byte slice to the arg
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}

	} else {
		// if it is like hello => then put this in the args
		// args will be like [[set], [key], [value]] => but the key and value will be the byte slice type
		state.args = append(state.args, line)

	}
	return nil
}
