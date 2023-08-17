package mpt

import (
	"bytes"
	"crypto"
	"errors"
	"testing"

	"github.com/MetaDataLab/go-MerklePatriciaTree/internal"
)

type MapKv struct {
	kv map[string][]byte
}

func (m *MapKv) Transaction() (internal.KvStorageTransaction, error) {
	return &MapKvTransaction{
		mapkv: m,
	}, nil
}

type MapKvTransaction struct {
	mapkv *MapKv
}

func (m *MapKvTransaction) Put(key, val []byte) error {
	m.mapkv.kv[string(key)] = val
	return nil
}
func (m *MapKvTransaction) Get(key []byte) ([]byte, error) {
	return m.mapkv.kv[string(key)], nil
}
func (m *MapKvTransaction) Delete(key []byte) error {
	delete(m.mapkv.kv, string(key))
	return nil
}

func (m *MapKvTransaction) Abort() error {
	return nil
}
func (m *MapKvTransaction) Commit() error {
	return nil
}

var testCases = map[string][]byte{
	"test1_key": []byte("test1_value"),
	"test2_key": []byte("test2_value"),
	"test3_key": []byte("test3_value"),
}

func TestTriePutGetUpdate(t *testing.T) {
	kv := &MapKv{
		kv: map[string][]byte{},
	}

	var testingTrie = New(
		crypto.SHA256.New,
		kv,
		[]byte("test_root"),
	)
	var err error
	txn, _ := testingTrie.Batch(nil)
	for k, v := range testCases {
		err = txn.Put([]byte(k), v)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = txn.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// test read on the same trie
	for k, v := range testCases {
		val, err := testingTrie.Get([]byte(k))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(val, v) != 0 {
			err = errors.New("value not equal")
			t.Fatal(err)
		}
	}

	// test read after failover
	testingTrie2 := New(
		crypto.SHA256.New,
		kv,
		[]byte("test_root"),
	)

	for k, v := range testCases {
		val, err := testingTrie2.Get([]byte(k))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(val, v) != 0 {
			err = errors.New("value not equal")
			t.Fatal(err)
		}
	}

	// test update
	testingTrie3 := New(
		crypto.SHA256.New,
		kv,
		[]byte("test_root"),
	)
	testCases["test1_key"] = []byte("test1_value2")
	txn, _ = testingTrie3.Batch(nil)
	for k, v := range testCases {
		err = txn.Put([]byte(k), v)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = txn.Commit()
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range testCases {
		val, err := testingTrie3.Get([]byte(k))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(val, v) != 0 {
			err = errors.New("value not equal")
			t.Fatal(err)
		}
	}
}

func TestTriePutDelete(t *testing.T) {
	kv := &MapKv{
		kv: map[string][]byte{},
	}

	var testingTrie = New(
		crypto.SHA256.New,
		kv,
		[]byte("test_root"),
	)
	var err error
	txn, _ := testingTrie.Batch(nil)
	for k, v := range testCases {
		err = txn.Put([]byte(k), v)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = txn.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// test read after failover
	testingTrie2 := New(
		crypto.SHA256.New,
		kv,
		[]byte("test_root"),
	)

	err = testingTrie2.Delete([]byte("test1_key"))
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range testCases {
		val, err := testingTrie2.Get([]byte(k))
		if k == "test1_key" {
			if err != KeyNotFound {
				err = errors.New("delete failed")
				t.Fatal(err)
			} else {
				continue
			}
		}

		if bytes.Compare(val, v) != 0 {
			err = errors.New("value not equal")
			t.Fatal(err)
		}
	}
}
