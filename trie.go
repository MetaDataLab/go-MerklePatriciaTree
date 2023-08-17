package mpt

import (
	"errors"
	"hash"

	"github.com/MetaDataLab/go-MerklePatriciaTree/api"
	"github.com/MetaDataLab/go-MerklePatriciaTree/internal"
	"github.com/MetaDataLab/go-MerklePatriciaTree/pb"
)

var KeyNotFound = errors.New("key not found")

type HasherFactory func() hash.Hash

type Trie struct {
	kv      api.TransactionalKvStorage
	hFac    HasherFactory
	rootKey []byte
}

func New(hf HasherFactory, kv api.TransactionalKvStorage, rootKey []byte) *Trie {
	return &Trie{
		kv:      kv,
		hFac:    hf,
		rootKey: rootKey,
	}
}

func (t *Trie) Batch(txn api.KvStorageTransaction) (*Batch, error) {
	var err error
	if txn == nil {
		txn, err = t.kv.Transaction()
		if err != nil {
			return nil, err
		}
	}
	root, err := t.loadRoot(txn)
	if err != nil {
		return nil, err
	}
	return &Batch{
		root:    root,
		rootKey: t.rootKey,
		hFac:    t.hFac,
		kv:      txn,
	}, nil
}

func (t *Trie) Delete(key []byte) error {
	batch, err := t.Batch(nil)
	if err != nil {
		return err
	}
	err = batch.Delete(key)
	if err != nil {
		return err
	}
	return batch.Commit()
}

func (t *Trie) Get(key []byte) ([]byte, error) {
	batch, err := t.Batch(nil)
	if err != nil {
		return nil, err
	}
	data, err := batch.Get(key)
	if err != nil {
		return nil, err
	}
	return data, batch.Abort()
}

func (t *Trie) Put(key, value []byte) error {
	batch, err := t.Batch(nil)
	if err != nil {
		return err
	}
	err = batch.Put(key, value)
	if err != nil {
		return err
	}
	return batch.Commit()
}

func (t *Trie) RootHash() ([]byte, error) {
	txn, err := t.kv.Transaction()
	if err != nil {
		return nil, err
	}
	rootHash, err := txn.Get(t.rootKey)
	defer txn.Abort()
	return rootHash, err
}

func (t *Trie) loadRoot(txn api.KvStorageTransaction) (internal.Node, error) {
	var root internal.Node = nil
	rootHash, err := txn.Get(t.rootKey)
	if err != nil {
		if err.Error() != KeyNotFound.Error() {
			return nil, err
		}
	}
	if len(rootHash) > 0 {
		r := internal.HashNode(rootHash)
		root = &r
	}
	if root != nil {
		root.Serialize(t.hFac())
	}
	return root, nil
}

func (t *Trie) persist(node internal.Node, persistTrie *pb.PersistTrie) (internal.Node, error) {
	if node != nil {
		if n, ok := node.(*internal.HashNode); ok {
			txn, err := t.kv.Transaction()
			if err != nil {
				return nil, err
			}
			data, err := txn.Get([]byte(*n))
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
