package mpt

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"reflect"
	"testing"

	"github.com/tokentransfer/chain/crypto"
	"github.com/tokentransfer/chain/store"

	libstore "github.com/tokentransfer/interfaces/store"
)

func NewMemKVStore() libstore.KvService {
	kv := store.MemoryService{}
	kv.Init(nil)
	return &kv
}

func NewLevelKVStore() libstore.KvService {
	kv := store.LevelService{Path: "./test"}
	err := kv.Init(nil)
	if err != nil {
		panic(err)
	}
	return &kv
}

func NewTrie() *Trie {
	kv := NewMemKVStore()
	cs := &crypto.CryptoService{}
	return New(cs, kv)
}

func NewLevelTrie() *Trie {
	kv := NewLevelKVStore()
	cs := &crypto.CryptoService{}
	return New(cs, kv)
}

func TestPutGet(t *testing.T) {
	trie := NewTrie()
	err := trie.Put([]byte("123456"), []byte("A"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("134567"), []byte("B"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("123467"), []byte("C"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("234567"), []byte("D"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("1234567890"), []byte("E"))
	if err != nil {
		t.Error(err.Error())
	}
	err = trie.Put([]byte("12345678"), []byte("F"))
	if err != nil {
		t.Error(err.Error())
	}
	data, err := trie.Get([]byte("123456"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "A" {
		t.Error("key 123456 wrong")
	}
	data, err = trie.Get([]byte("134567"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "B" {
		t.Error("key 134567 wrong")
	}
	data, err = trie.Get([]byte("123467"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "C" {
		t.Error("key 123467 wrong")
	}
	data, err = trie.Get([]byte("234567"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "D" {
		t.Error("key 234567 wrong")
	}
	data, err = trie.Get([]byte("1234567890"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "E" {
		t.Error("key 1234567890 wrong")
	}

	trie.Put([]byte("123456"), []byte("F"))
	data, err = trie.Get([]byte("123456"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "F" {
		t.Error("rewrite key 123456 wrong")
	}
	data, err = trie.Get([]byte("12345678"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "F" {
		t.Error("key 12345678 wrong")
	}
}

func TestPutCommitGet(t *testing.T) {
	trie := NewTrie()
	err := trie.Put([]byte("123456"), []byte("A"))
	if err != nil {
		t.Error(err.Error())
	}
	err = trie.Put([]byte("134567"), []byte("B"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("123467"), []byte("C"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("234567"), []byte("D"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("1234567890"), []byte("E"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("12345678"), []byte("F"))
	if err != nil {
		t.Error(err.Error())
	}
	trie.Commit()
	trie.Abort()
	data, err := trie.Get([]byte("123456"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "A" {
		t.Error("key 123456 wrong")
	}
	data, err = trie.Get([]byte("134567"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "B" {
		t.Error("key 134567 wrong")
	}
	data, err = trie.Get([]byte("123467"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "C" {
		t.Error("key 123467 wrong")
	}
	data, err = trie.Get([]byte("234567"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "D" {
		t.Error("key 234567 wrong")
	}
	data, err = trie.Get([]byte("1234567890"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "E" {
		t.Error("key 1234567890 wrong")
	}

	trie.Put([]byte("123456"), []byte("F"))
	data, err = trie.Get([]byte("123456"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "F" {
		t.Error("rewrite key 123456 wrong")
	}
	data, err = trie.Get([]byte("12345678"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "F" {
		t.Error("key 12345678 wrong")
	}
}

func TestPutAbort(t *testing.T) {
	trie := NewTrie()
	err := trie.Put([]byte("123456"), []byte("A"))
	if err != nil {
		t.Error(err.Error())
	}
	err = trie.Put([]byte("134567"), []byte("B"))
	if err != nil {
		t.Error(err.Error())
	}
	err = trie.Put([]byte("123467"), []byte("C"))
	if err != nil {
		t.Error(err.Error())
	}
	trie.Abort()

	_, err = trie.Get([]byte("123456"))
	if err == nil {
		t.Error("Abort failed")
	}

	err = trie.Put([]byte("123456"), []byte("A"))
	if err != nil {
		t.Error(err.Error())
	}
	err = trie.Put([]byte("134567"), []byte("B"))
	if err != nil {
		t.Error(err.Error())
	}
	err = trie.Put([]byte("123467"), []byte("C"))
	if err != nil {
		t.Error(err.Error())
	}
	trie.Commit()
	err = trie.Put([]byte("234567"), []byte("D"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("1234567890"), []byte("E"))
	if err != nil {
		t.Error(err.Error())
	}
	err = trie.Put([]byte("12345678"), []byte("F"))
	if err != nil {
		t.Error(err.Error())
	}
	data, err := trie.Get([]byte("123456"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "A" {
		t.Error("key 123467 wrong")
	}

	trie.Abort()
	data, err = trie.Get([]byte("123456"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "A" {
		t.Error("key 123467 wrong (after abort)")
	}
	data, err = trie.Get([]byte("12345678"))
	if err == nil {
		t.Error("Abort failed")
	}
}

func TestNodeSerialize(t *testing.T) {
	cs := &crypto.CryptoService{}
	valueNode := ValueNode{
		Value: []byte("123"),
		dirty: true,
		cache: nil,
	}
	data, err := valueNode.Serialize(cs)
	if err != nil {
		t.Error(err.Error())
	}
	newNode, err := DeserializeNode(cs, data)
	if err != nil {
		t.Error(err.Error())
	}
	if string(newNode.(*ValueNode).Value) != "123" {
		t.Error("content does not match")
	}
	bs, err := newNode.Serialize(cs)
	if err != nil {
		t.Error(err.Error())
	}
	if !bytes.Equal(data, bs) {
		t.Error("data does not match")
	}
	shortNode := ShortNode{
		Key:   []byte("123"),
		Value: &valueNode,
		dirty: true,
	}
	data, err = shortNode.Serialize(cs)
	if err != nil {
		t.Error(err.Error())
	}
	newNode, err = DeserializeNode(cs, data)
	if err != nil {
		t.Error(err.Error())
	}
	if string(newNode.(*ShortNode).Key) != "123" {
		t.Error("content does not match")
	}
	bs, err = newNode.Serialize(cs)
	if err != nil {
		t.Error(err.Error())
	}
	if !bytes.Equal(data, bs) {
		t.Error("data does not match")
	}
	fullNode := FullNode{}
	fullNode.Children[0] = &shortNode
	shortNode.dirty = true
	valueNode.dirty = true
	fullNode.dirty = true
	data, err = shortNode.Serialize(cs)
	if err != nil {
		t.Error(err.Error())
	}
	newNode, err = DeserializeNode(cs, data)
	if err != nil {
		t.Error(err.Error())
	}
	bs, err = newNode.Serialize(cs)
	if err != nil {
		t.Error(err.Error())
	}
	if !bytes.Equal(data, bs) {
		t.Error("data does not match")
	}
}

func printNode(level int, n Node, value []byte) {
	switch n.(type) {
	case *FullNode:
		fn := n.(*FullNode)
		fmt.Println(string(bytes.Repeat([]byte{' ', ' '}, level)), "value", len(fn.Children), fn.dirty, reflect.TypeOf(n))
		for i := 0; i < len(fn.Children); i++ {
			c := fn.Children[i]
			if c != nil {
				fmt.Println(string(bytes.Repeat([]byte{' ', ' '}, level)), "children", i, reflect.TypeOf(c), reflect.TypeOf(n))
				printNode(level+1, c, nil)
			}
		}

	case *ShortNode:
		sn := n.(*ShortNode)
		fmt.Println(string(bytes.Repeat([]byte{' ', ' '}, level)), "value", string(sn.Key), sn.dirty, reflect.TypeOf(n))
		printNode(level+1, sn.Value, nil)

	case *ValueNode:
		vn := n.(*ValueNode)
		fmt.Println(string(bytes.Repeat([]byte{' ', ' '}, level)), "value", string(vn.Value), vn.dirty, reflect.TypeOf(n))

	case *HashNode:
		vn := n.(*HashNode)
		fmt.Println(string(bytes.Repeat([]byte{' ', ' '}, level)), "value", hex.EncodeToString(*vn), reflect.TypeOf(n))

	default:
		fmt.Println(string(bytes.Repeat([]byte{' ', ' '}, level)), "value", hex.EncodeToString(value), reflect.TypeOf(n))
	}
}
func TestRootHash(t *testing.T) {
	trie := NewLevelTrie()

	// trie.kv.ListData(func(key, value []byte) error {
	// 	k := hex.EncodeToString(key)
	// 	n, _ := DeserializeNode(trie.cs, value)
	// 	fmt.Println("dump", k, reflect.TypeOf(n))
	// 	printNode(0, n, value)
	// 	return nil
	// })
	/*[hash]00000000000000000000000000
	 *  [hash]5a1757bf8f90b4b2357fdca2ef350c9c1afef48c334c2e657b2da80522fc789d
	 *    [short](1)5a1757bf8f90b4b2357fdca2ef350c9c1afef48c334c2e657b2da80522fc789d <- root
	 *      [full]31bd456e3a8e8596561bedc695d0d7ede1451f90d9099e4969710ca6e38e2cfb
	 *        [hash](2[50])0bad2d1537100e98b33935ce07b5e8b5db0c457151caa6b554411286f29156b7
	 *          [short](3456)0bad2d1537100e98b33935ce07b5e8b5db0c457151caa6b554411286f29156b7
	 *            [value](A)9b11fdb3753264d09517261a9df9c8e25915fa46088e0f646b4e73a7c5765119
	 *        [hash](3[51])298fae2162332e2041e141b6a7d7c1961dddeea7092091be2643162f3c7bb796
	 *          [short](4567)298fae2162332e2041e141b6a7d7c1961dddeea7092091be2643162f3c7bb796
	 *            [value](B)5a9490193ff6db3445d8acd237fb4195897ba597b899c1366d3175e07de623ac
	 */

	err := trie.Put([]byte("123456"), []byte("A"))
	if err != nil {
		t.Error(err.Error())
	}
	err = trie.Put([]byte("134567"), []byte("B"))
	if err != nil {
		t.Error(err.Error())
	}
	trie.Commit()

	rootHash := trie.RootHash()
	fmt.Println("root", hex.EncodeToString(rootHash))
	err = trie.kv.Close()
	if err != nil {
		t.Error(err.Error())
	}

	trie = NewLevelTrie()
	rootHash = trie.RootHash()
	fmt.Println("root", hex.EncodeToString(rootHash))

	// err = trie.Put([]byte("1234567890123"), []byte("0"))
	// if err != nil {
	// 	t.Error(err.Error())
	// }
	// trie.Commit()

	data, err := trie.Get([]byte("123456"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "A" {
		t.Error("key 123456 wrong (after abort)")
	}

	data, err = trie.Get([]byte("134567"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "B" {
		t.Error("key 134567 wrong (after abort)")
	}

	// data, err = trie.Get([]byte("1234567890123"))
	// if err != nil {
	// 	t.Error(err.Error())
	// }
	// if string(data) != "0" {
	// 	t.Error("key 1234567890123 wrong (after abort)")
	// }

	// fmt.Println("root", hex.EncodeToString(trie.RootHash()))
	// printNode(0, trie.root, nil)

	err = trie.kv.Close()
	if err != nil {
		t.Error(err.Error())
	}
}

func TestSerializeDeserialize(t *testing.T) {
	trie := NewTrie()
	err := trie.Put([]byte("123456"), []byte("A"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("134567"), []byte("B"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("123467"), []byte("C"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("234567"), []byte("D"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("1234567890"), []byte("E"))
	if err != nil {
		t.Error(err.Error())
	}
	err = trie.Put([]byte("12345678"), []byte("F"))
	if err != nil {
		t.Error(err.Error())
	}

	trie.Commit()
	data, err := trie.Serialize()
	if err != nil {
		t.Error(err.Error())
	}
	trie = NewTrie()
	err = trie.Deserialize(data)
	if err != nil {
		t.Error(err.Error())
	}
	data, err = trie.Get([]byte("123456"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "A" {
		t.Error("key 123456 wrong")
	}
	data, err = trie.Get([]byte("134567"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "B" {
		t.Error("key 134567 wrong")
	}
	data, err = trie.Get([]byte("123467"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "C" {
		t.Error("key 123467 wrong")
	}
	data, err = trie.Get([]byte("234567"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "D" {
		t.Error("key 234567 wrong")
	}
	data, err = trie.Get([]byte("1234567890"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "E" {
		t.Error("key 1234567890 wrong")
	}

	trie.Put([]byte("123456"), []byte("F"))
	data, err = trie.Get([]byte("123456"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "F" {
		t.Error("rewrite key 123456 wrong")
	}
	data, err = trie.Get([]byte("12345678"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "F" {
		t.Error("key 12345678 wrong")
	}

}

// func TestPutCommitGetLevelDB(t *testing.T) {
// 	trie := NewLevelTrie()
// 	err := trie.Put([]byte("123456"), []byte("A"))
// 	if err != nil {
// 		t.Error(err.Error())
// 	}
// 	err = trie.Put([]byte("134567"), []byte("B"))
// 	if err != nil {
// 		t.Error(err.Error())
// 	}

// 	err = trie.Put([]byte("123467"), []byte("C"))
// 	if err != nil {
// 		t.Error(err.Error())
// 	}

// 	err = trie.Put([]byte("234567"), []byte("D"))
// 	if err != nil {
// 		t.Error(err.Error())
// 	}

// 	err = trie.Put([]byte("1234567890"), []byte("E"))
// 	if err != nil {
// 		t.Error(err.Error())
// 	}

// 	err = trie.Put([]byte("12345678"), []byte("F"))
// 	if err != nil {
// 		t.Error(err.Error())
// 	}
// 	trie.Commit()
// 	trie.Abort()
// 	data, err := trie.Get([]byte("123456"))
// 	if err != nil {
// 		t.Error(err.Error())
// 	}
// 	if string(data) != "A" {
// 		t.Error("key 123456 wrong")
// 	}
// 	data, err = trie.Get([]byte("134567"))
// 	if err != nil {
// 		t.Error(err.Error())
// 	}
// 	if string(data) != "B" {
// 		t.Error("key 134567 wrong")
// 	}
// 	data, err = trie.Get([]byte("123467"))
// 	if err != nil {
// 		t.Error(err.Error())
// 	}
// 	if string(data) != "C" {
// 		t.Error("key 123467 wrong")
// 	}
// 	data, err = trie.Get([]byte("234567"))
// 	if err != nil {
// 		t.Error(err.Error())
// 	}
// 	if string(data) != "D" {
// 		t.Error("key 234567 wrong")
// 	}
// 	data, err = trie.Get([]byte("1234567890"))
// 	if err != nil {
// 		t.Error(err.Error())
// 	}
// 	if string(data) != "E" {
// 		t.Error("key 1234567890 wrong")
// 	}

// 	trie.Put([]byte("123456"), []byte("F"))
// 	data, err = trie.Get([]byte("123456"))
// 	if err != nil {
// 		t.Error(err.Error())
// 	}
// 	if string(data) != "F" {
// 		t.Error("rewrite key 123456 wrong")
// 	}
// 	data, err = trie.Get([]byte("12345678"))
// 	if err != nil {
// 		t.Error(err.Error())
// 	}
// 	if string(data) != "F" {
// 		t.Error("key 12345678 wrong")
// 	}
// }
