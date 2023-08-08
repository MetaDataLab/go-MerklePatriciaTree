package internal

import (
	"errors"
	fmt "fmt"
	"hash"

	"google.golang.org/protobuf/proto"

	"github.com/MetaDataLab/go-MerklePatriciaTree/pb"
)

type (
	KvStorage interface {
		Put(key, val []byte) error
		Get(key []byte) ([]byte, error)
		Delete(key []byte) error
	}
	Node interface {
		Hash(hash.Hash) []byte
		CachedHash() []byte
		Serialize(hash.Hash) ([]byte, error)
		Save(KvStorage, hash.Hash) error
	}
	FullNode struct {
		Children [257]Node
		Cache    []byte
		Dirty    bool
	}
	ShortNode struct {
		Key   []byte
		Value Node
		Cache []byte
		Dirty bool
	}
	HashNode  []byte
	ValueNode struct {
		Value []byte
		Cache []byte
		Dirty bool
	}
)

func (n *FullNode) CachedHash() []byte  { return n.Cache }
func (n *ShortNode) CachedHash() []byte { return n.Cache }
func (n *ValueNode) CachedHash() []byte { return n.Cache }
func (n *HashNode) CachedHash() []byte  { return []byte(*n) }

func DeserializeNode(hasher hash.Hash, data []byte) (Node, error) {
	persistNode := &pb.PersistNode{}
	err := proto.Unmarshal(data, persistNode)
	if err != nil {
		return nil, fmt.Errorf("[Node] cannot deserialize persist node: %s", err.Error())
	}
	switch v := persistNode.Content.(type) {
	case *pb.PersistNode_Full:
		fullNode := FullNode{}
		for i := 0; i < len(fullNode.Children); i++ {
			if len(v.Full.Children[i]) != 0 {
				child := HashNode(v.Full.Children[i])
				fullNode.Children[i] = &child
				if len([]byte(child)) == 0 {
					return nil, errors.New("[Node] nil full node child")
				}
			}
		}
		hash, err := Hash(hasher, data)
		if err != nil {
			return nil, err
		}
		fullNode.Cache = hash[:]
		return &fullNode, nil
	case *pb.PersistNode_Short:
		shortNode := ShortNode{}
		shortNode.Key = v.Short.Key
		if len(v.Short.Value) == 0 {
			return nil, errors.New("[Node] nil short node value")
		}
		child := HashNode(v.Short.Value)
		shortNode.Value = &child
		hash, err := Hash(hasher, data)
		if err != nil {
			return nil, err
		}
		shortNode.Cache = hash[:]
		return &shortNode, nil
	case *pb.PersistNode_Value:
		hash, err := Hash(hasher, data)
		if err != nil {
			return nil, err
		}
		ret := ValueNode{v.Value, hash[:], false}
		return &ret, nil
	}
	return nil, errors.New("[Node] Unknown node type")
}

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
	fn.Dirty = false
	return data, nil
}

func (fn *FullNode) Hash(cs hash.Hash) []byte {
	if fn.Dirty {
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

func (hn *HashNode) Hash(hash.Hash) []byte                 { return []byte(*hn) }
func (hn *HashNode) Serialize(hash.Hash) ([]byte, error)   { return nil, nil }
func (hn *HashNode) Save(kv KvStorage, cs hash.Hash) error { return nil }
