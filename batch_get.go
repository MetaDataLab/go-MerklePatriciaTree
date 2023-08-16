package mpt

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/MetaDataLab/go-MerklePatriciaTree/internal"
)

func (t *Batch) Get(key []byte) ([]byte, error) {
	node, expandedNode, err := t.get(t.root, key, 0)
	if expandedNode != nil {
		t.root = expandedNode
	}
	if err != nil {
		return nil, err
	} else if v, ok := node.(*internal.ValueNode); ok {
		return []byte(v.Value), nil
	} else {
		return nil, fmt.Errorf("[Trie Batch] key not found: %s", hex.EncodeToString(key))
	}
}

func (t *Batch) get(node internal.Node, key []byte, prefixLen int) (internal.Node, internal.Node, error) {
	if node == nil {
		return nil, node, fmt.Errorf("[Trie Batch] key not found: %s", hex.EncodeToString(key))
	}
	switch n := node.(type) {
	case *internal.FullNode:
		if prefixLen > len(key) {
			return nil, node, fmt.Errorf("[Trie Batch] key not found: %s", hex.EncodeToString(key))
		}
		if prefixLen == len(key) {
			valueNode, newNode, err := t.get(n.Children[256], key, prefixLen)
			n.Children[256] = newNode
			return valueNode, node, err
		}

		valueNode, newNode, err := t.get(n.Children[key[prefixLen]], key, prefixLen+1)
		n.Children[key[prefixLen]] = newNode
		return valueNode, node, err
	case *internal.ShortNode:
		if len(key)-prefixLen < len(n.Key) || !bytes.Equal(n.Key, key[prefixLen:prefixLen+len(n.Key)]) {
			return nil, node, fmt.Errorf("[Trie Batch] key not found: %s", hex.EncodeToString(key))
		}
		valueNode, newNode, err := t.get(n.Value, key, prefixLen+len(n.Key))
		n.Value = newNode
		return valueNode, node, err
	case *internal.HashNode:
		data, err := t.kv.Get([]byte(*n))
		if err != nil {
			return nil, node, err
		}
		loadedNode, err := internal.DeserializeNode(t.hFac(), data)
		if err != nil {
			return nil, node, fmt.Errorf("[Trie Batch] Cannot load node: %s", err.Error())
		}
		if !bytes.Equal([]byte(*n), loadedNode.Hash(t.hFac())) {
			return nil, node, fmt.Errorf("[Trie Batch] Cannot load node: hash does not match")
		}
		valueNode, loadedNode, err := t.get(loadedNode, key, prefixLen)
		return valueNode, loadedNode, err
	case *internal.ValueNode:
		if prefixLen == len(key) {
			return node, node, nil
		}

		return nil, node, fmt.Errorf("[Trie Batch] key not found: %s", hex.EncodeToString(key))
	}
	return nil, node, errors.New("[Tire] Unknown node type")
}
