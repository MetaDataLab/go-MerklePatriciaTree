package mpt

import (
	"hash"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/MetaDataLab/go-MerklePatriciaTree/internal"
	"github.com/MetaDataLab/go-MerklePatriciaTree/pb"
)

type HasherFactory func() hash.Hash

type Trie struct {
	oldRootHash []byte
	root        internal.Node
	kv          internal.KvStorage
	hFac        HasherFactory
	lock        *sync.RWMutex
	rootKey     []byte
}

func New(hf HasherFactory, kv internal.KvStorage, rootKey []byte) *Trie {
	var oldRoot []byte = nil
	var root internal.Node = nil
	rootHash, _ := kv.Get(rootKey)
	if len(rootHash) > 0 {
		r := internal.HashNode(rootHash)
		root = &r
	}
	if root != nil {
		root.Serialize(hf()) // update cached hash
		oldRoot = root.CachedHash()
	}
	return &Trie{
		oldRootHash: oldRoot,
		root:        root,
		kv:          kv,
		hFac:        hf,
		lock:        &sync.RWMutex{},
		rootKey:     rootKey,
	}
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
			if n.Children[i] == nil {
				continue
			}
			t.commit(n.Children[i])
		}
		n.Save(t.kv, t.hFac())
	case *internal.ShortNode:
		t.commit(n.Value)
		n.Save(t.kv, t.hFac())
	case *internal.ValueNode:
		n.Save(t.kv, t.hFac())
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
	return t.root.Hash(t.hFac())
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
