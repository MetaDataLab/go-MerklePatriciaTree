package mpt

import (
	"github.com/MetaDataLab/go-MerklePatriciaTree/internal"
)

type Batch struct {
	root internal.Node
	*Trie
	toDel [][]byte
}

// the batch should not be used after committed
func (t *Batch) Commit() error {
	if t.root == nil {
		err := t.kv.Delete(t.rootKey)
		if err != nil {
			return err
		}
		return nil
	}
	for _, key := range t.toDel {
		t.kv.Delete(key)
	}
	t.commit(t.root)
	h := t.root.CachedHash()
	hn := internal.HashNode(h)
	err := t.kv.Put(t.rootKey, hn)
	if err != nil {
		return err
	}

	return nil
}

func (t *Batch) commit(node internal.Node) {
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
