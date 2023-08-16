package internal

import (
	"hash"

	"github.com/MetaDataLab/go-MerklePatriciaTree/pb"
	"google.golang.org/protobuf/proto"
)

type ValueNode struct {
	Value []byte
	Cache []byte
	Dirty bool
}

func (n *ValueNode) CachedHash() []byte { return n.Cache }

func (vn *ValueNode) Serialize(hasher hash.Hash) ([]byte, error) {
	persistValueNode := pb.PersistNode_Value{}
	persistValueNode.Value = vn.Value
	persistNode := pb.PersistNode{
		Content: &persistValueNode,
	}
	data, _ := proto.Marshal(&persistNode)
	hash, err := Hash(hasher, data)
	if err != nil {
		return nil, err
	}
	vn.Cache = hash[:]
	vn.Dirty = false
	return data, nil
}

func (vn *ValueNode) Hash(cs hash.Hash) []byte {
	if vn.Dirty {
		vn.Serialize(cs)
	}
	return vn.Cache
}

func (vn *ValueNode) Save(kv KvStorage, cs hash.Hash) error {
	data, err := vn.Serialize(cs)
	if err != nil {
		return err
	}
	return kv.Put(vn.Cache, data)
}
