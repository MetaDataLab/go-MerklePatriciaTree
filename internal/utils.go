package internal

import (
	"errors"
	fmt "fmt"
	"hash"

	"github.com/MetaDataLab/go-MerklePatriciaTree/pb"
	"google.golang.org/protobuf/proto"
)

func Hash(hasher hash.Hash, data []byte) ([]byte, error) {
	hasher.Reset()
	_, err := hasher.Write(data)
	if err != nil {
		return nil, err
	}
	return hasher.Sum(nil), nil
}

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
		ret := ValueNode{v.Value, hash[:], CLEAN}
		return &ret, nil
	}
	return nil, errors.New("[Node] Unknown node type")
}
