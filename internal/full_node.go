package internal

import (
	"hash"

	"github.com/MetaDataLab/go-MerklePatriciaTree/api"
	"github.com/MetaDataLab/go-MerklePatriciaTree/pb"
	"google.golang.org/protobuf/proto"
)

type FullNode struct {
	OriginalKey []byte
	Children    [257]Node
	Cache       []byte
	Status      NodeStatus
}

func (n *FullNode) CachedHash() []byte { return n.Cache }

func (fn *FullNode) Serialize(hasher hash.Hash) ([]byte, error) {
	persistFullNode := pb.PersistFullNode{}
	persistFullNode.Children = make([][]byte, 257)
	for i := 0; i < len(fn.Children); i++ {
		if fn.Children[i] != nil {
			persistFullNode.Children[i] = fn.Children[i].Hash(hasher)
		}
	}
	data, _ := proto.Marshal(&pb.PersistNode{
		Content: &pb.PersistNode_Full{Full: &persistFullNode},
	})
	hash, err := Hash(hasher, data)
	if err != nil {
		return nil, err
	}
	fn.Cache = hash[:]
	fn.Status = CLEAN
	return data, nil
}

func (fn *FullNode) Hash(cs hash.Hash) []byte {
	if fn.Status == DIRTY {
		fn.Serialize(cs)
	}
	return fn.Cache
}

func (fn *FullNode) Save(kv api.KvStorageTransaction, cs hash.Hash) error {
	if fn.Status == DELETED {
		return kv.Delete(fn.OriginalKey)
	}
	data, err := fn.Serialize(cs)
	if err != nil {
		return err
	}
	return kv.Put(fn.Cache, data)
}

func (fn *FullNode) OnlyChild() (bool, Node) {
	var hasOneChild bool
	var onlyChild Node
	for _, child := range fn.Children {
		if child != nil {
			if hasOneChild {
				return false, nil
			}
			hasOneChild = true
			onlyChild = child
		}
	}
	return hasOneChild, onlyChild
}
