package internal

import (
	"hash"

	"github.com/MetaDataLab/go-MerklePatriciaTree/api"
)

type HashNode []byte

func (n *HashNode) CachedHash() []byte                                    { return []byte(*n) }
func (hn *HashNode) Hash(hash.Hash) []byte                                { return []byte(*hn) }
func (hn *HashNode) Serialize(hash.Hash) ([]byte, error)                  { return nil, nil }
func (hn *HashNode) Save(kv api.KvStorageTransaction, cs hash.Hash) error { return nil }
