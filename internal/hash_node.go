package internal

import "hash"

type HashNode []byte

func (n *HashNode) CachedHash() []byte                     { return []byte(*n) }
func (hn *HashNode) Hash(hash.Hash) []byte                 { return []byte(*hn) }
func (hn *HashNode) Serialize(hash.Hash) ([]byte, error)   { return nil, nil }
func (hn *HashNode) Save(kv KvStorage, cs hash.Hash) error { return nil }
