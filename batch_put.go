package mpt

import (
	"errors"
	"fmt"

	"github.com/MetaDataLab/go-MerklePatriciaTree/internal"
)

func (b *Batch) Put(key, value []byte) error {
	valueNode := internal.ValueNode{
		Value:  value,
		Cache:  nil,
		Status: internal.DIRTY,
	}
	expandedNode, err := b.put(b.root, key, &valueNode, 0)
	if expandedNode != nil {
		b.root = expandedNode
	}
	return err
}

func (b *Batch) put(node internal.Node, key []byte, value internal.Node, prefixLen int) (internal.Node, error) {
	if node == nil {
		if prefixLen > len(key) {
			return node, errors.New("[Trie Batch] Cannot insert")
		} else if prefixLen == len(key) {
			return value, nil
		} else {
			shortNode := internal.ShortNode{
				Key:    key[prefixLen:],
				Value:  value,
				Status: internal.DIRTY,
			}
			return &shortNode, nil
		}
	}
	switch n := node.(type) {
	case *internal.FullNode:
		n.Status = internal.DIRTY
		if prefixLen > len(key) {
			return node, fmt.Errorf("[Trie Batch] Cannot insert")
		} else if prefixLen == len(key) {
			n.Children[256] = value
			return n, nil
		}
		// prefixLen < len(key)
		newNode, err := b.put(n.Children[key[prefixLen]], key, value, prefixLen+1)
		if err != nil {
			return node, err
		}
		n.Children[key[prefixLen]] = newNode
		return n, err
	case *internal.ShortNode:
		n.Status = internal.DIRTY
		if prefixLen > len(key) {
			return node, fmt.Errorf("[Trie Batch] Cannot insert")
		}
		commonLen := commonPrefix(n.Key, key[prefixLen:])
		if commonLen == len(n.Key) {
			newNode, err := b.put(n.Value, key, value, prefixLen+len(n.Key))
			if err != nil {
				return node, err
			}
			n.Value = newNode
			return n, nil
		}
		prefixLen += commonLen
		fullNode := &internal.FullNode{Status: internal.DIRTY}
		newNode, err := b.put(fullNode, key, value, prefixLen)
		if err != nil {
			return node, err
		}
		newNode, err = b.put(newNode, n.Key, n.Value, commonLen)
		if err != nil {
			return node, err
		}
		if commonLen > 0 {
			shortNode := internal.ShortNode{Status: internal.DIRTY}
			shortNode.Key = n.Key[:commonLen]
			shortNode.Value = newNode
			return &shortNode, nil
		}
		return newNode, nil
	case *internal.ValueNode:
		n.Status = internal.DIRTY
		if prefixLen == len(key) {
			return value, nil
		} else if prefixLen < len(key) {
			fullNode := &internal.FullNode{Status: internal.DIRTY}
			newNode, err := b.put(fullNode, key, value, prefixLen)
			if err != nil {
				return node, fmt.Errorf("[Trie Batch] Cannot insert")
			}
			newNode, err = b.put(newNode, key[:prefixLen], node, prefixLen)
			if err != nil {
				return node, fmt.Errorf("[Trie Batch] Cannot insert")
			}
			return newNode, nil
		} else {
			return node, fmt.Errorf("[Trie Batch] Cannot insert")
		}
	case *internal.HashNode:
		if prefixLen > len(key) {
			return node, fmt.Errorf("[Trie Batch] Cannot insert")
		}
		data, err := b.kv.Get([]byte(*n))
		if err != nil {
			return node, err
		}
		newNode, err := internal.DeserializeNode(b.hFac(), data)
		if err != nil {
			return node, err
		}
		newNode, err = b.put(newNode, key, value, prefixLen)
		if err != nil {
			return node, err
		}
		return newNode, nil
	}
	return node, fmt.Errorf("[Trie Batch] Cannot insert")
}
