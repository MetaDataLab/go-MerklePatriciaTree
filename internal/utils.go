package internal

import (
	"bytes"
	"hash"
)

func ZeroHash(hasher hash.Hash) []byte {
	l := hasher.Size()
	return bytes.Repeat([]byte{0x00}, l)
}

func Hash(hasher hash.Hash, data []byte) ([]byte, error) {
	_, err := hasher.Write(data)
	if err != nil{
		return nil, err
	}
	return hasher.Sum(nil), nil
}
