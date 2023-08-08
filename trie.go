package trie

import (
	"hash"

	"github.com/MetaDataLab/go-MerklePatriciaTree/mpt"
)

func New(hasher hash.Hash, kv mpt.KvStorage, rootKey []byte) *mpt.Trie {
	return mpt.New(hasher, kv, rootKey)
}
