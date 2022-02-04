// Package cmap implements a thread-safe concurrent string to string hashtable. It uses FNV32 hash function. The hashtable is divided into multiple shards and each shard gets locked while an operation is being done on it. Sharding helps to lower the performance loss due to the lock contention. Instead of locking the whole hashtable, we only lock the appropriate shards.
package cmap

import (
	"sync"
)

// SHARD_COUNT is the number of the shards that the hashtable is divided into.
const SHARD_COUNT = 32

type shard struct {
	Lock sync.RWMutex
	Data map[string]string
}

// HashTable is a slice of shards. Each shard contains a normal map and a lock.
type HashTable []*shard

// New initializes and returns a hashtable.
func New() *HashTable {
	ht := make(HashTable, SHARD_COUNT)
	for i := 0; i < SHARD_COUNT; i++ {
		ht[i] = &shard{Data: make(map[string]string)}
	}
	return &ht
}

// From gets a normal map, constructs, and returns a thread-safe concurrent hashtable out of its records.
func From(data map[string]string) *HashTable {
	ht := New()
	for k, v := range data {
		shard := ht.getShard(k)

		shard.Lock.Lock()
		shard.Data[k] = v
		shard.Lock.Unlock()
	}
	return ht
}

// Get returns true and the value associated with the key. If it doesn't exist, it will return empty string and false.
func (h HashTable) Get(key string) (string, bool) {
	shard := h.getShard(key)

	shard.Lock.RLock()
	defer shard.Lock.RUnlock()

	v, ok := shard.Data[key]

	return v, ok
}

// Put adds a new key-value pair to the hashtable. If there is already a record with a key same as the given key, the value will be overridden.
func (h HashTable) Put(key string, value string) {
	shard := h.getShard(key)

	shard.Lock.Lock()
	defer shard.Lock.Unlock()

	shard.Data[key] = value
}

// PutIfNotExist will add a new key-value pair only if no record with the same key exists. It returns true if the new record added successfully.
func (h HashTable) PutIfNotExist(key string, value string) bool {
	shard := h.getShard(key)

	shard.Lock.Lock()
	defer shard.Lock.Unlock()

	_, ok := shard.Data[key]
	if !ok {
		shard.Data[key] = value
	}

	return !ok
}

// Del deletes the record associated with the given key. If the deletion was successful it will return true. If the record didn't exist, it will return false.
func (h HashTable) Del(key string) (string, bool) {
	shard := h.getShard(key)

	shard.Lock.Lock()
	defer shard.Lock.Unlock()

	v, ok := shard.Data[key]
	delete(shard.Data, key)

	return v, ok
}

// Has returns true if the hashtable contains a record with a key same as the given key.
func (h HashTable) Has(key string) bool {
	shard := h.getShard(key)

	shard.Lock.RLock()
	defer shard.Lock.RUnlock()

	_, ok := shard.Data[key]
	return ok
}

// Len returns the number of key-value pairs stored in the hashtable.
func (h HashTable) Len() int {
	var count int
	for i := 0; i < SHARD_COUNT; i++ {
		shard := h[i]

		shard.Lock.RLock()
		count += len(shard.Data)
		shard.Lock.RUnlock()
	}
	return count
}

// getShard finds the FNV32 hash of the given key. It calculates the modulo SHARD_COUNT to get the index of the shard.
func (h HashTable) getShard(key string) *shard {
	return h[uint(fnv32(key))%uint(SHARD_COUNT)]
}

// fnv32 returns the FNV32 hash of the given key.
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)

	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}

	return hash
}
