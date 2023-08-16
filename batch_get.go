package mpt

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/MetaDataLab/go-MerklePatriciaTree/internal"
)

func (b *Batch) Get(key []byte) ([]byte, error) {
	node, expandedNode, err := b.get(b.root, key, 0)
	if expandedNode != nil {
		b.root = expandedNode
	}
	if err != nil {
		return nil, err
	} else if v, ok := node.(*internal.ValueNode); ok {
		return []byte(v.Value), nil
	} else {
		return nil, KeyNotFound
	}
}

func (b *Batch) get(node internal.Node, key []byte, prefixLen int) (internal.Node, internal.Node, error) {
	if node == nil {
		return nil, node, KeyNotFound
	}
	switch n := node.(type) {
	case *internal.FullNode:
		if prefixLen > len(key) {
			return nil, node, KeyNotFound
		}
		if prefixLen == len(key) {
			valueNode, newNode, err := b.get(n.Children[256], key, prefixLen)
			n.Children[256] = newNode
			return valueNode, node, err
		}

		valueNode, newNode, err := b.get(n.Children[key[prefixLen]], key, prefixLen+1)
		n.Children[key[prefixLen]] = newNode
		return valueNode, node, err
	case *internal.ShortNode:
		if len(key)-prefixLen < len(n.Key) || !bytes.Equal(n.Key, key[prefixLen:prefixLen+len(n.Key)]) {
			return nil, node, KeyNotFound
		}
		valueNode, newNode, err := b.get(n.Value, key, prefixLen+len(n.Key))
		n.Value = newNode
		return valueNode, node, err
	case *internal.HashNode:
		data, err := b.kv.Get([]byte(*n))
		if err != nil {
			return nil, node, err
		}
		loadedNode, err := internal.DeserializeNode(b.hFac(), data)
		if err != nil {
			return nil, node, fmt.Errorf("[Trie Batch] Cannot load node: %s", err.Error())
		}
		if !bytes.Equal([]byte(*n), loadedNode.Hash(b.hFac())) {
			return nil, node, fmt.Errorf("[Trie Batch] Cannot load node: hash does not match")
		}
		valueNode, loadedNode, err := b.get(loadedNode, key, prefixLen)
		return valueNode, loadedNode, err
	case *internal.ValueNode:
		if prefixLen == len(key) {
			return node, node, nil
		}

		return nil, node, KeyNotFound
	}
	return nil, node, errors.New("[Tire] Unknown node type")
}
