package internal

import (
	"hash"

	"github.com/MetaDataLab/go-MerklePatriciaTree/pb"
	"google.golang.org/protobuf/proto"
)

type ShortNode struct {
	Key   []byte
	Value Node
	Cache []byte
	Dirty bool
}

func (n *ShortNode) CachedHash() []byte { return n.Cache }

func (sn *ShortNode) Serialize(hasher hash.Hash) ([]byte, error) {
	persistShortNode := pb.PersistShortNode{}
	persistShortNode.Key = sn.Key
	persistShortNode.Value = sn.Value.Hash(hasher)
	data, _ := proto.Marshal(&pb.PersistNode{
		Content: &pb.PersistNode_Short{Short: &persistShortNode},
	})
	hash, err := Hash(hasher, data)
	if err != nil {
		return nil, err
	}
	sn.Cache = hash[:]
	sn.Dirty = false
	return data, nil
}

func (sn *ShortNode) Hash(cs hash.Hash) []byte {
	if sn.Dirty {
		sn.Serialize(cs)
	}
	return sn.Cache
}

func (sn *ShortNode) Save(kv KvStorage, cs hash.Hash) error {
	data, err := sn.Serialize(cs)
	if err != nil {
		return err
	}
	return kv.Put(sn.Cache, data)
}
