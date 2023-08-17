package api

import "errors"

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
)

var NotFound = errors.New("key not found")
