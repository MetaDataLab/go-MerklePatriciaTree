package internal

import (
	"hash"

	"github.com/MetaDataLab/go-MerklePatriciaTree/pb"
	"google.golang.org/protobuf/proto"
)

type FullNode struct {
	Children [257]Node
	Cache    []byte
	Status   NodeStatus
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

func (fn *FullNode) Save(kv KvStorage, cs hash.Hash) error {
	data, err := fn.Serialize(cs)
	if err != nil {
		return err
	}
	return kv.Put(fn.Cache, data)
}
