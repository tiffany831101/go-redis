package cluster

import (
	"context"
	"go-redis/config"
	database2 "go-redis/database"
	"go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/lib/consistenthash"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"strings"

	pool "github.com/jolestar/go-commons-pool"
)

type ClusterDatabase struct {
	self       string
	nodes      []string
	peerPicker *consistenthash.NodeMap
	// use a map to maintain a connection pool
	peerConnection map[string]*pool.ObjectPool
	db             database.Database
}

func MakeClusterDatabase() *ClusterDatabase {
	cluster := &ClusterDatabase{
		self:           config.Properties.Self,
		db:             database2.NewStandaloneDataBase(),
		peerPicker:     consistenthash.NewNodeMap(nil),
		peerConnection: make(map[string]*pool.ObjectPool),
	}

	nodes := make([]string, 0, len(config.Properties.Peers)+1)

	for _, peer := range config.Properties.Peers {
		nodes = append(nodes, peer)
	}

	nodes = append(nodes, config.Properties.Self)
	cluster.peerPicker.AddNode(nodes...)

	ctx := context.Background()

	// will maintian the connection for us
	for _, peer := range config.Properties.Peers {
		cluster.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			Peer: peer,
		})
	}

	cluster.nodes = nodes
	return cluster

}

type CmdFunc func(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply

var router = makeRouter()

func (cluster *ClusterDatabase) Exec(client resp.Connection, args [][]byte) (result resp.Reply) {

	defer func() {

		if err := recover(); err != nil {
			logger.Error(err)

			result = reply.UnknowErrReply{}

		}
	}()

	cmdName := strings.ToLower(string(args[0]))

	cmdFunc, ok := router[cmdName]
	if !ok {

		reply.MakeErrReply("not supported cmd")
	}

	result = cmdFunc(cluster, client, args)
	return
}

func (c *ClusterDatabase) Close() {
	c.db.Close()
}

func (cluster *ClusterDatabase) AfterClientClose(c resp.Connection) {
	cluster.db.AfterClientClose(c)
}
