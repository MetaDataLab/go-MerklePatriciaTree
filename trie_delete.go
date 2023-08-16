package mpt

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/MetaDataLab/go-MerklePatriciaTree/internal"
)

func (t *Trie) Delete(key []byte) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	n, err := t.delete(t.root, key, 0)
	if err != nil {
		return err
	}
	t.root = n
	return nil
}

func (t *Trie) delete(node internal.Node, key []byte, prefixLen int) (internal.Node, error) {
	if node == nil {
		return node, fmt.Errorf("[Trie] key not found: %s", hex.EncodeToString(key))
	}
	switch n := node.(type) {
	case *internal.FullNode:
		if prefixLen > len(key) {
			return node, fmt.Errorf("[Trie] key not found: %s", hex.EncodeToString(key))
		}
		if prefixLen == len(key) {
			newNode, err := t.delete(n.Children[256], key, prefixLen)
			n.Children[256] = nil
			return newNode, err
		}

		newNode, err := t.delete(n.Children[key[prefixLen]], key, prefixLen+1)
		n.Children[key[prefixLen]] = newNode
		return newNode, err
	case *internal.ShortNode:
		if len(key)-prefixLen < len(n.Key) || !bytes.Equal(n.Key, key[prefixLen:prefixLen+len(n.Key)]) {
			return node, fmt.Errorf("[Trie] key not found: %s", hex.EncodeToString(key))
		}
		newNode, err := t.delete(n.Value, key, prefixLen+len(n.Key))
		n.Value = newNode
		return node, err
	case *internal.HashNode:
		data, err := t.kv.Get([]byte(*n))
		if err != nil {
			return node, err
		}
		loadedNode, err := internal.DeserializeNode(t.hFac(), data)
		if err != nil {
			return node, fmt.Errorf("[Trie] Cannot load node: %s", err.Error())
		}
		if !bytes.Equal([]byte(*n), loadedNode.Hash(t.hFac())) {
			return node, fmt.Errorf("[Trie] Cannot load node: hash does not match")
		}
		loadedNode, err = t.delete(loadedNode, key, prefixLen)
		return loadedNode, err
	case *internal.ValueNode:
		if prefixLen == len(key) {
			return node, nil
		}

		return node, fmt.Errorf("[Trie] key not found: %s", hex.EncodeToString(key))
	}
	return node, errors.New("[Tire] Unknown node type")
}
