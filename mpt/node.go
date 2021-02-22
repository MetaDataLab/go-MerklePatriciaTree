package mpt

import (
	"errors"
	fmt "fmt"

	"github.com/golang/protobuf/proto"

	"github.com/tokentransfer/go-MerklePatriciaTree/pb"

	libcrypto "github.com/tokentransfer/interfaces/crypto"
	libstore "github.com/tokentransfer/interfaces/store"
)

type (
	Node interface {
		Hash(libcrypto.CryptoService) []byte
		CachedHash() []byte
		Serialize(libcrypto.CryptoService) ([]byte, error)
		Save(libstore.KvService, libcrypto.CryptoService) error
	}
	FullNode struct {
		Children [257]Node
		cache    []byte
		dirty    bool
	}
	ShortNode struct {
		Key   []byte
		Value Node
		cache []byte
		dirty bool
	}
	HashNode  []byte
	ValueNode struct {
		Value []byte
		cache []byte
		dirty bool
	}
)

func (n *FullNode) CachedHash() []byte  { return n.cache }
func (n *ShortNode) CachedHash() []byte { return n.cache }
func (n *ValueNode) CachedHash() []byte { return n.cache }
func (n *HashNode) CachedHash() []byte  { return []byte(*n) }

func DeserializeNode(cs libcrypto.CryptoService, data []byte) (Node, error) {
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
		hash, err := cs.Hash(data)
		if err != nil {
			return nil, err
		}
		fullNode.cache = hash[:]
		return &fullNode, nil
	case *pb.PersistNode_Short:
		shortNode := ShortNode{}
		shortNode.Key = v.Short.Key
		if len(v.Short.Value) == 0 {
			return nil, errors.New("[Node] nil short node value")
		}
		child := HashNode(v.Short.Value)
		shortNode.Value = &child
		hash, err := cs.Hash(data)
		if err != nil {
			return nil, err
		}
		shortNode.cache = hash[:]
		return &shortNode, nil
	case *pb.PersistNode_Value:
		hash, err := cs.Hash(data)
		if err != nil {
			return nil, err
		}
		ret := ValueNode{v.Value, hash[:], false}
		return &ret, nil
	}
	return nil, errors.New("[Node] Unknown node type")
}

func (vn *ValueNode) Serialize(cs libcrypto.CryptoService) ([]byte, error) {
	persistValueNode := pb.PersistNode_Value{}
	persistValueNode.Value = vn.Value
	persistNode := pb.PersistNode{
		Content: &persistValueNode,
	}
	data, _ := proto.Marshal(&persistNode)
	hash, err := cs.Hash(data)
	if err != nil {
		return nil, err
	}
	vn.cache = hash[:]
	vn.dirty = false
	return data, nil
}

func (vn *ValueNode) Hash(cs libcrypto.CryptoService) []byte {
	if vn.dirty {
		vn.Serialize(cs)
	}
	return vn.cache
}

func (vn *ValueNode) Save(kv libstore.KvService, cs libcrypto.CryptoService) error {
	data, err := vn.Serialize(cs)
	if err != nil {
		return err
	}
	return kv.PutData(vn.cache, data)
}

func (fn *FullNode) Serialize(cs libcrypto.CryptoService) ([]byte, error) {
	persistFullNode := pb.PersistFullNode{}
	persistFullNode.Children = make([][]byte, 257)
	for i := 0; i < len(fn.Children); i++ {
		if fn.Children[i] != nil {
			persistFullNode.Children[i] = fn.Children[i].Hash(cs)
		}
	}
	data, _ := proto.Marshal(&pb.PersistNode{
		Content: &pb.PersistNode_Full{Full: &persistFullNode},
	})
	hash, err := cs.Hash(data)
	if err != nil {
		return nil, err
	}
	fn.cache = hash[:]
	fn.dirty = false
	return data, nil
}

func (fn *FullNode) Hash(cs libcrypto.CryptoService) []byte {
	if fn.dirty {
		fn.Serialize(cs)
	}
	return fn.cache
}

func (fn *FullNode) Save(kv libstore.KvService, cs libcrypto.CryptoService) error {
	data, err := fn.Serialize(cs)
	if err != nil {
		return err
	}
	return kv.PutData(fn.cache, data)
}

func (sn *ShortNode) Serialize(cs libcrypto.CryptoService) ([]byte, error) {
	persistShortNode := pb.PersistShortNode{}
	persistShortNode.Key = sn.Key
	persistShortNode.Value = sn.Value.Hash(cs)
	data, _ := proto.Marshal(&pb.PersistNode{
		Content: &pb.PersistNode_Short{Short: &persistShortNode},
	})
	hash, err := cs.Hash(data)
	if err != nil {
		return nil, err
	}
	sn.cache = hash[:]
	sn.dirty = false
	return data, nil
}

func (sn *ShortNode) Hash(cs libcrypto.CryptoService) []byte {
	if sn.dirty {
		sn.Serialize(cs)
	}
	return sn.cache
}

func (sn *ShortNode) Save(kv libstore.KvService, cs libcrypto.CryptoService) error {
	data, err := sn.Serialize(cs)
	if err != nil {
		return err
	}
	return kv.PutData(sn.cache, data)
}

func (hn *HashNode) Hash(libcrypto.CryptoService) []byte                          { return []byte(*hn) }
func (hn *HashNode) Serialize(libcrypto.CryptoService) ([]byte, error)            { return nil, nil }
func (hn *HashNode) Save(kv libstore.KvService, cs libcrypto.CryptoService) error { return nil }
