package mpt

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/MetaDataLab/go-MerklePatriciaTree/internal"
)

func (b *Batch) Delete(key []byte) error {
	n, err := b.delete(b.root, key, 0)
	if err != nil {
		return err
	}
	b.root = n
	return nil
}

func (b *Batch) delete(node internal.Node, key []byte, prefixLen int) (internal.Node, error) {
	if node == nil {
		return nil, KeyNotFound
	}
	switch n := node.(type) {
	case *internal.FullNode:
		if prefixLen > len(key) {
			return nil, KeyNotFound
		}
		if prefixLen == len(key) {
			newNode, err := b.delete(n.Children[256], key, prefixLen)
			if err != nil {
				return nil, err
			}
			n.Children[256] = newNode
		} else {
			newNode, err := b.delete(n.Children[key[prefixLen]], key, prefixLen+1)
			if err != nil {
				return nil, err
			}
			n.Children[key[prefixLen]] = newNode
		}

		// only one child remains in this full node
		// promote it and delete the current one
		if hasOneChild, child := n.OnlyChild(); hasOneChild {
			b.toDel = appendEx(b.toDel, n.OriginalKey)

			// if the child is short node, replace the current node with it
			if sn, ok := child.(*internal.ShortNode); ok {
				return sn, nil
			}

			// otherwise replace current node with a new short node
			return &internal.ShortNode{
				Key:    key[prefixLen:],
				Value:  child,
				Status: internal.DIRTY,
			}, nil
		}
		return n, nil
	case *internal.ShortNode:
		if len(key)-prefixLen < len(n.Key) || !bytes.Equal(n.Key, key[prefixLen:prefixLen+len(n.Key)]) {
			return nil, KeyNotFound
		}
		newNode, err := b.delete(n.Value, key, prefixLen+len(n.Key))
		if err != nil {
			return nil, err
		}

		// this short node's value is empty
		// so the short node itself also needs to be deleted
		if newNode == nil {
			b.toDel = appendEx(b.toDel, n.OriginalKey)
			return nil, nil
		}

		// the child node turns into a short node
		// promote the child node, and remove the current one
		if sn, ok := newNode.(*internal.ShortNode); ok {
			b.toDel = appendEx(b.toDel, n.OriginalKey)
			return sn, nil
		}

		n.Value = newNode
		return node, nil
	case *internal.HashNode:
		data, err := b.kv.Get([]byte(*n))
		if err != nil {
			return node, err
		}
		loadedNode, err := internal.DeserializeNode(b.hFac(), data)
		if err != nil {
			return node, fmt.Errorf("[Trie Batch] Cannot load node: %s", err.Error())
		}
		if !bytes.Equal([]byte(*n), loadedNode.Hash(b.hFac())) {
			return node, fmt.Errorf("[Trie Batch] Cannot load node: hash does not match")
		}
		return b.delete(loadedNode, key, prefixLen)
	case *internal.ValueNode:
		if prefixLen == len(key) {
			b.toDel = appendEx(b.toDel, n.OriginalKey)
			return nil, nil
		}
		return nil, KeyNotFound
	}
	return node, errors.New("[Tire] Unknown node type")
}

func appendEx(array [][]byte, ele []byte) [][]byte {
	if ele != nil {
		return append(array, ele)
	}
	return array
}
