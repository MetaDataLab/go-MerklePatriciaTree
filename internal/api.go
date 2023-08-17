package internal

import (
	"fmt"
	"hash"

	"github.com/MetaDataLab/go-MerklePatriciaTree/api"
)

type (
	Node interface {
		Hash(hash.Hash) []byte
		CachedHash() []byte
		Serialize(hash.Hash) ([]byte, error)
		Save(api.KvStorageTransaction, hash.Hash) error
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
