package internal

import (
	"hash"
)

type (
	KvStorage interface {
		Put(key, val []byte) error
		Get(key []byte) ([]byte, error)
		Delete(key []byte) error
	}
	Node interface {
		Hash(hash.Hash) []byte
		CachedHash() []byte
		Serialize(hash.Hash) ([]byte, error)
		Save(KvStorage, hash.Hash) error
	}
)
