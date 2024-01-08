package consistenthash

import (
	"hash/crc32"
	"sort"
)

type HashFunc func(data []byte) uint32

type NodeMap struct {
	hashFunc HashFunc
	// sort the nodehash
	nodeHashs   []int // hash for the node
	nodeHashMap map[int]string
}

func NewNodeMap(fn HashFunc) *NodeMap {

	m := &NodeMap{
		hashFunc:    fn,
		nodeHashMap: make(map[int]string),
	}

	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}

	return m
}

// check if the nodemap is empty
func (m *NodeMap) IsEmpty() bool {
	return len(m.nodeHashs) == 0
}

func (m *NodeMap) AddNode(keys ...string) {

	for _, key := range keys {
		if key == "" {
			continue
		}
		hash := int(m.hashFunc([]byte(key)))
		m.nodeHashs = append(m.nodeHashs, hash)
		m.nodeHashMap[hash] = key

	}

	// use quick sort
	sort.Ints(m.nodeHashs)
}

func (m *NodeMap) PickNode(key string) string {

	if m.IsEmpty() {
		return ""
	}

	hash := int(m.hashFunc([]byte(key)))
	idx := sort.Search(len(m.nodeHashs), func(i int) bool {
		return m.nodeHashs[i] >= hash
	})

	// a cycle
	if idx == len(m.nodeHashs) {
		idx = 0
	}

	return m.nodeHashMap[m.nodeHashs[idx]]
}
