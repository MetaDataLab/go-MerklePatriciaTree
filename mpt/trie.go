package mpt

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/golang/protobuf/proto"

	"github.com/tokentransfer/go-MerklePatriciaTree/pb"

	libcrypto "github.com/tokentransfer/interfaces/crypto"
	libstore "github.com/tokentransfer/interfaces/store"
)

var once = &sync.Once{}
var zero []byte

type Trie struct {
	oldRoot []byte
	root    Node
	kv      libstore.KvService
	cs      libcrypto.CryptoService
	lock    *sync.RWMutex
}

func New(cs libcrypto.CryptoService, kv libstore.KvService) *Trie {
	once.Do(func() {
		zero = libcrypto.ZeroHash(cs)
	})

	var oldRoot []byte = nil
	var root Node = nil
	rootHash, _ := kv.GetData(zero)
	if len(rootHash) > 0 {
		r := HashNode(rootHash)
		root = &r
	}
	if root != nil {
		root.Serialize(cs) // update cached hash
		oldRoot = root.CachedHash()
	}
	return &Trie{
		oldRoot: oldRoot,
		root:    root,
		kv:      kv,
		cs:      cs,
		lock:    &sync.RWMutex{},
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
	} else if v, ok := node.(*ValueNode); ok {
		return []byte(v.Value), nil
	} else {
		return nil, fmt.Errorf("[Trie] key not found: %s", hex.EncodeToString(key))
	}
}

func (t *Trie) get(node Node, key []byte, prefixLen int) (Node, Node, error) {
	if node == nil {
		return nil, node, fmt.Errorf("[Trie] key not found: %s", hex.EncodeToString(key))
	}
	switch n := node.(type) {
	case *FullNode:
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
	case *ShortNode:
		if len(key)-prefixLen < len(n.Key) || !bytes.Equal(n.Key, key[prefixLen:prefixLen+len(n.Key)]) {
			return nil, node, fmt.Errorf("[Trie] key not found: %s", hex.EncodeToString(key))
		}
		valueNode, newNode, err := t.get(n.Value, key, prefixLen+len(n.Key))
		n.Value = newNode
		return valueNode, node, err
	case *HashNode:
		data, err := t.kv.GetData([]byte(*n))
		if err != nil {
			return nil, node, err
		}
		loadedNode, err := DeserializeNode(t.cs, data)
		if err != nil {
			return nil, node, fmt.Errorf("[Trie] Cannot load node: %s", err.Error())
		}
		if !bytes.Equal([]byte(*n), loadedNode.Hash(t.cs)) {
			return nil, node, fmt.Errorf("[Trie] Cannot load node: hash does not match")
		}
		valueNode, loadedNode, err := t.get(loadedNode, key, prefixLen)
		return valueNode, loadedNode, err
	case *ValueNode:
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
	valueNode := ValueNode{value, nil, true}
	expandedNode, err := t.put(t.root, key, &valueNode, 0)
	if expandedNode != nil {
		t.root = expandedNode
	}
	return err
}

func (t *Trie) put(node Node, key []byte, value Node, prefixLen int) (Node, error) {
	if node == nil {
		if prefixLen > len(key) {
			return node, errors.New("[Trie] Cannot insert")
		} else if prefixLen == len(key) {
			return value, nil
		} else {
			shortNode := ShortNode{
				Key:   key[prefixLen:],
				Value: value,
				dirty: true,
			}
			return &shortNode, nil
		}
	}
	switch n := node.(type) {
	case *FullNode:
		n.dirty = true
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
	case *ShortNode:
		n.dirty = true
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
		fullNode := &FullNode{dirty: true}
		newNode, err := t.put(fullNode, key, value, prefixLen)
		if err != nil {
			return node, err
		}
		newNode, err = t.put(newNode, n.Key, n.Value, commonLen)
		if err != nil {
			return node, err
		}
		if commonLen > 0 {
			shortNode := ShortNode{dirty: true}
			shortNode.Key = n.Key[:commonLen]
			shortNode.Value = newNode
			return &shortNode, nil
		}
		return newNode, nil
	case *ValueNode:
		n.dirty = true
		if prefixLen == len(key) {
			return value, nil
		} else if prefixLen < len(key) {
			fullNode := &FullNode{dirty: true}
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
	case *HashNode:
		if prefixLen > len(key) {
			return node, fmt.Errorf("[Trie] Cannot insert")
		}
		data, err := t.kv.GetData([]byte(*n))
		if err != nil {
			return node, err
		}
		newNode, err := DeserializeNode(t.cs, data)
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
		err := t.kv.RemoveData(zero)
		if err != nil {
			return err
		}
		return nil
	}
	t.commit(t.root)
	h := t.root.CachedHash()
	t.oldRoot = h

	hn := HashNode(h)
	err := t.kv.PutData(zero, hn)
	if err != nil {
		return err
	}
	return nil
}

func (t *Trie) commit(node Node) {
	switch n := node.(type) {
	case *FullNode:
		for i := 0; i < len(n.Children); i++ {
			t.commit(n.Children[i])
		}
		n.Save(t.kv, t.cs)
	case *ShortNode:
		t.commit(n.Value)
		n.Save(t.kv, t.cs)
	case *ValueNode:
		n.Save(t.kv, t.cs)
	}
}

func (t *Trie) Abort() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.oldRoot == nil {
		t.root = nil
	} else {
		hashNode := HashNode(t.oldRoot)
		t.root = &hashNode
	}
	return nil
}

func (t *Trie) RootHash() []byte {
	if t.root == nil {
		return zero
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

func (t *Trie) persist(node Node, persistTrie *pb.PersistTrie) (Node, error) {
	if node != nil {
		if n, ok := node.(*HashNode); ok {
			data, err := t.kv.GetData([]byte(*n))
			if err != nil {
				return node, err
			}
			newNode, err := DeserializeNode(t.cs, data)
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
	case *FullNode:
		for i := 0; i < len(n.Children); i++ {
			t.persist(n.Children[i], persistTrie)
		}
	case *ShortNode:
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
		err := t.kv.PutData(persistTrie.Pairs[i].Key, persistTrie.Pairs[i].Value)
		if err != nil {
			return err
		}
	}
	if len(persistTrie.Pairs) == 0 {
		t.root = nil
	} else {
		rootNode := HashNode(persistTrie.Pairs[0].Key)
		t.root = &rootNode
	}
	return nil
}
