package internal

import (
	"fmt"
	"hash"
)

type (
	KvStorageOperation interface {
		Put(key, val []byte) error
		Get(key []byte) ([]byte, error)
		Delete(key []byte) error
	}
	KvStorageTransaction interface {
		KvStorageOperation
		Abort() error
		Commit() error
	}
	TransactionalKvStorage interface {
		Transaction() (KvStorageTransaction, error)
	}
	Node interface {
		Hash(hash.Hash) []byte
		CachedHash() []byte
		Serialize(hash.Hash) ([]byte, error)
		Save(KvStorageTransaction, hash.Hash) error
	}
	NodeStatus uint8
)

const (
	CLEAN NodeStatus = iota
	DIRTY
	DELETED
)

func (s NodeStatus) String() string {
	switch s {
	case CLEAN:
		return "CLEAN"
	case DELETED:
		return "DELETED"
	case DIRTY:
		return "DIRTY"
	default:
		return fmt.Sprintf("UNKNOWN NODE STATUS: %d", s)
	}
}
