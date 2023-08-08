package mpt

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/MetaDataLab/go-MerklePatriciaTree/internal"
	"github.com/MetaDataLab/go-MerklePatriciaTree/pb"
)

type Trie struct {
	oldRootHash []byte
	root        internal.Node
	kv          internal.KvStorage
	cs          hash.Hash
	lock        *sync.RWMutex
	rootKey     []byte
}

func New(hasher hash.Hash, kv internal.KvStorage, rootKey []byte) *Trie {
	var oldRoot []byte = nil
	var root internal.Node = nil
	rootHash, _ := kv.Get(rootKey)
	if len(rootHash) > 0 {
		r := internal.HashNode(rootHash)
		root = &r
	}
	if root != nil {
		root.Serialize(hasher) // update cached hash
		oldRoot = root.CachedHash()
	}
	return &Trie{
		oldRootHash: oldRoot,
		root:        root,
		kv:          kv,
		cs:          hasher,
		lock:        &sync.RWMutex{},
		rootKey:     rootKey,
	}
}

func (t *Trie) Get(key []byte) ([]byte, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	node, expandedNode, err := t.get(t.root, key, 0)
	if expandedNode != nil {
		t.root = expandedNode
	}
	if err != nil {
		return nil, err
	} else if v, ok := node.(*internal.ValueNode); ok {
		return []byte(v.Value), nil
	} else {
		return nil, fmt.Errorf("[Trie] key not found: %s", hex.EncodeToString(key))
	}
}

func (t *Trie) get(node internal.Node, key []byte, prefixLen int) (internal.Node, internal.Node, error) {
	if node == nil {
		return nil, node, fmt.Errorf("[Trie] key not found: %s", hex.EncodeToString(key))
	}
	switch n := node.(type) {
	case *internal.FullNode:
		if prefixLen > len(key) {
			return nil, node, fmt.Errorf("[Trie] key not found: %s", hex.EncodeToString(key))
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
			return nil, node, fmt.Errorf("[Trie] key not found: %s", hex.EncodeToString(key))
		}
		valueNode, newNode, err := t.get(n.Value, key, prefixLen+len(n.Key))
		n.Value = newNode
		return valueNode, node, err
	case *internal.HashNode:
		data, err := t.kv.Get([]byte(*n))
		if err != nil {
			return nil, node, err
		}
		loadedNode, err := internal.DeserializeNode(t.cs, data)
		if err != nil {
			return nil, node, fmt.Errorf("[Trie] Cannot load node: %s", err.Error())
		}
		if !bytes.Equal([]byte(*n), loadedNode.Hash(t.cs)) {
			return nil, node, fmt.Errorf("[Trie] Cannot load node: hash does not match")
		}
		valueNode, loadedNode, err := t.get(loadedNode, key, prefixLen)
		return valueNode, loadedNode, err
	case *internal.ValueNode:
		if prefixLen == len(key) {
			return node, node, nil
		}

		return nil, node, fmt.Errorf("[Trie] key not found: %s", hex.EncodeToString(key))
	}
	return nil, node, errors.New("[Tire] Unknown node type")
}

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
		newNode, err := internal.DeserializeNode(t.cs, data)
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

func commonPrefix(a, b []byte) int {
	minLen := len(a)
	if len(b) < len(a) {
		minLen = len(b)
	}
	ret := 0
	for i := 0; i < minLen; i++ {
		if a[i] == b[i] {
			ret++
		} else {
			break
		}
	}
	return ret
}

func (t *Trie) Commit() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.root == nil {
		err := t.kv.Delete(t.rootKey)
		if err != nil {
			return err
		}
		return nil
	}
	t.commit(t.root)
	h := t.root.CachedHash()
	t.oldRootHash = h

	hn := internal.HashNode(h)
	err := t.kv.Put(t.rootKey, hn)
	if err != nil {
		return err
	}
	return nil
}

func (t *Trie) commit(node internal.Node) {
	switch n := node.(type) {
	case *internal.FullNode:
		for i := 0; i < len(n.Children); i++ {
			t.commit(n.Children[i])
		}
		n.Save(t.kv, t.cs)
	case *internal.ShortNode:
		t.commit(n.Value)
		n.Save(t.kv, t.cs)
	case *internal.ValueNode:
		n.Save(t.kv, t.cs)
	}
}

func (t *Trie) Abort() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.oldRootHash == nil {
		t.root = nil
	} else {
		hashNode := internal.HashNode(t.oldRootHash)
		t.root = &hashNode
	}
	return nil
}

func (t *Trie) RootHash() []byte {
	if t.root == nil {
		return t.rootKey
	}
	return t.root.Hash(t.cs)
}

func (t *Trie) Serialize() ([]byte, error) {
	t.lock.Lock()
	t.lock.Unlock()
	persistTrie := &pb.PersistTrie{}
	newNode, err := t.persist(t.root, persistTrie)
	if err != nil {
		return nil, err
	}
	t.root = newNode
	data, err := proto.Marshal(persistTrie)
	return data, err
}

func (t *Trie) persist(node internal.Node, persistTrie *pb.PersistTrie) (internal.Node, error) {
	if node != nil {
		if n, ok := node.(*internal.HashNode); ok {
			data, err := t.kv.Get([]byte(*n))
			if err != nil {
				return node, err
			}
			newNode, err := internal.DeserializeNode(t.cs, data)
			if err != nil {
				return node, err
			}
			node = newNode
		}
		data, err := node.Serialize(t.cs)
		if err != nil {
			return nil, err
		}
		persistKV := pb.PersistKV{
			Key:   node.Hash(t.cs),
			Value: data,
		}
		persistTrie.Pairs = append(persistTrie.Pairs, &persistKV)
	}
	switch n := node.(type) {
	case *internal.FullNode:
		for i := 0; i < len(n.Children); i++ {
			t.persist(n.Children[i], persistTrie)
		}
	case *internal.ShortNode:
		t.persist(n.Value, persistTrie)
	}
	return node, nil
}

func (t *Trie) Deserialize(data []byte) error {
	persistTrie := pb.PersistTrie{}
	err := proto.Unmarshal(data, &persistTrie)
	if err != nil {
		return err
	}
	for i := 0; i < len(persistTrie.Pairs); i++ {
		err := t.kv.Put(persistTrie.Pairs[i].Key, persistTrie.Pairs[i].Value)
		if err != nil {
			return err
		}
	}
	if len(persistTrie.Pairs) == 0 {
		t.root = nil
	} else {
		rootNode := internal.HashNode(persistTrie.Pairs[0].Key)
		t.root = &rootNode
	}
	return nil
}
