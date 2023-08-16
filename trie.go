package mpt

import (
	"errors"
	"hash"
	"sync"

	"github.com/MetaDataLab/go-MerklePatriciaTree/internal"
	"github.com/MetaDataLab/go-MerklePatriciaTree/pb"
)

var KeyNotFound = errors.New("key not found")

type HasherFactory func() hash.Hash

type Trie struct {
	kv      internal.KvStorage
	hFac    HasherFactory
	rootKey []byte

	sync.RWMutex
}

func New(hf HasherFactory, kv internal.KvStorage, rootKey []byte) *Trie {
	return &Trie{
		kv:      kv,
		hFac:    hf,
		rootKey: rootKey,
	}
}

func (t *Trie) Batch() *Batch {
	return &Batch{
		root: t.loadRoot(),
		Trie: t,
	}
}

func (t *Trie) Delete(key []byte) error {
	batch := t.Batch()
	err := batch.Delete(key)
	if err != nil {
		return err
	}
	return batch.Commit()
}

func (t *Trie) Get(key []byte) ([]byte, error) {
	batch := t.Batch()
	data, err := batch.Get(key)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (t *Trie) Put(key, value []byte) error {
	batch := t.Batch()
	err := batch.Put(key, value)
	if err != nil {
		return err
	}
	return batch.Commit()
}

func (t *Trie) loadRoot() internal.Node {
	var root internal.Node = nil
	rootHash, _ := t.kv.Get(t.rootKey)
	if len(rootHash) > 0 {
		r := internal.HashNode(rootHash)
		root = &r
	}
	if root != nil {
		root.Serialize(t.hFac())
	}
	return root
}

func (t *Trie) persist(node internal.Node, persistTrie *pb.PersistTrie) (internal.Node, error) {
	if node != nil {
		if n, ok := node.(*internal.HashNode); ok {
			data, err := t.kv.Get([]byte(*n))
			if err != nil {
				return node, err
			}
			newNode, err := internal.DeserializeNode(t.hFac(), data)
			if err != nil {
				return node, err
			}
			node = newNode
		}
		data, err := node.Serialize(t.hFac())
		if err != nil {
			return nil, err
		}
		persistKV := pb.PersistKV{
			Key:   node.Hash(t.hFac()),
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
