package mpt

import (
	"errors"
	"fmt"

	"github.com/MetaDataLab/go-MerklePatriciaTree/internal"
)

func (t *Trie) Put(key, value []byte) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	valueNode := internal.ValueNode{
		Value: value,
		Cache: nil,
		Dirty: true,
	}
	expandedNode, err := t.put(t.root, key, &valueNode, 0)
	if expandedNode != nil {
		t.root = expandedNode
	}
	return err
}

func (t *Trie) put(node internal.Node, key []byte, value internal.Node, prefixLen int) (internal.Node, error) {
	if node == nil {
		if prefixLen > len(key) {
			return node, errors.New("[Trie] Cannot insert")
		} else if prefixLen == len(key) {
			return value, nil
		} else {
			shortNode := internal.ShortNode{
				Key:   key[prefixLen:],
				Value: value,
				Dirty: true,
			}
			return &shortNode, nil
		}
	}
	switch n := node.(type) {
	case *internal.FullNode:
		n.Dirty = true
		if prefixLen > len(key) {
			return node, fmt.Errorf("[Trie] Cannot insert")
		} else if prefixLen == len(key) {
			n.Children[256] = value
			return n, nil
		}
		// prefixLen < len(key)
		newNode, err := t.put(n.Children[key[prefixLen]], key, value, prefixLen+1)
		if err != nil {
			return node, err
		}
		n.Children[key[prefixLen]] = newNode
		return n, err
	case *internal.ShortNode:
		n.Dirty = true
		if prefixLen > len(key) {
			return node, fmt.Errorf("[Trie] Cannot insert")
		}
		commonLen := commonPrefix(n.Key, key[prefixLen:])
		if commonLen == len(n.Key) {
			newNode, err := t.put(n.Value, key, value, prefixLen+len(n.Key))
			if err != nil {
				return node, err
			}
			n.Value = newNode
			return n, nil
		}
		prefixLen += commonLen
		fullNode := &internal.FullNode{Dirty: true}
		newNode, err := t.put(fullNode, key, value, prefixLen)
		if err != nil {
			return node, err
		}
		newNode, err = t.put(newNode, n.Key, n.Value, commonLen)
		if err != nil {
			return node, err
		}
		if commonLen > 0 {
			shortNode := internal.ShortNode{Dirty: true}
			shortNode.Key = n.Key[:commonLen]
			shortNode.Value = newNode
			return &shortNode, nil
		}
		return newNode, nil
	case *internal.ValueNode:
		n.Dirty = true
		if prefixLen == len(key) {
			return value, nil
		} else if prefixLen < len(key) {
			fullNode := &internal.FullNode{Dirty: true}
			newNode, err := t.put(fullNode, key, value, prefixLen)
			if err != nil {
				return node, fmt.Errorf("[Trie] Cannot insert")
			}
			newNode, err = t.put(newNode, key[:prefixLen], node, prefixLen)
			if err != nil {
				return node, fmt.Errorf("[Trie] Cannot insert")
			}
			return newNode, nil
		} else {
			return node, fmt.Errorf("[Trie] Cannot insert")
		}
	case *internal.HashNode:
		if prefixLen > len(key) {
			return node, fmt.Errorf("[Trie] Cannot insert")
		}
		data, err := t.kv.Get([]byte(*n))
		if err != nil {
			return node, err
		}
		newNode, err := internal.DeserializeNode(t.hFac(), data)
		if err != nil {
			return node, err
		}
		newNode, err = t.put(newNode, key, value, prefixLen)
		if err != nil {
			return node, err
		}
		return newNode, nil
	}
	return node, fmt.Errorf("[Trie] Cannot insert")
}
