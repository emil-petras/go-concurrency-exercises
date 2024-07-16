//////////////////////////////////////////////////////////////////////
//
// Given is some code to cache key-value pairs from a database into
// the main memory (to reduce access time). Note that golang's map are
// not entirely thread safe. Multiple readers are fine, but multiple
// writers are not. Change the code to make this thread safe.
//

package main

import (
	"container/list"
	"sync"
	"testing"

	"golang.org/x/sync/singleflight"
)

// CacheSize determines how big the cache can grow
const CacheSize = 100

// KeyStoreCacheLoader is an interface for the KeyStoreCache
type KeyStoreCacheLoader interface {
	// Load implements a function where the cache should gets it's content from
	Load(string) string
}

type page struct {
	Key   string
	Value string
}

// KeyStoreCache is a LRU cache for string key-value pairs
type KeyStoreCache struct {
	cache map[string]*list.Element
	pages list.List
	load  func(string) string
	mu    sync.Mutex
	group singleflight.Group
}

// New creates a new KeyStoreCache
func New(load KeyStoreCacheLoader) *KeyStoreCache {
	return &KeyStoreCache{
		load:  load.Load,
		cache: make(map[string]*list.Element),
	}
}

// Get gets the key from cache, loads it from the source if needed
func (k *KeyStoreCache) Get(key string) string {
	k.mu.Lock()
	if e, ok := k.cache[key]; ok {
		k.pages.MoveToFront(e)
		value := e.Value.(page).Value
		k.mu.Unlock()
		return value
	}
	k.mu.Unlock()

	// singleflight ensures only one load per key, others wait for it
	value, _, _ := k.group.Do(key, func() (interface{}, error) {
		return k.load(key), nil
	})

	k.mu.Lock()
	defer k.mu.Unlock()

	if e, ok := k.cache[key]; ok {
		return e.Value.(page).Value
	}

	// if cache is full remove the least used item
	if len(k.cache) >= CacheSize {
		end := k.pages.Back()
		if end != nil {
			// remove from map
			delete(k.cache, end.Value.(page).Key)
			// remove from list
			k.pages.Remove(end)
		}
	}

	// create a new page and add it to the cache
	p := page{Key: key, Value: value.(string)}
	element := k.pages.PushFront(p)
	k.cache[key] = element

	return value.(string)
}

// Loader implements KeyStoreLoader
type Loader struct {
	DB *MockDB
}

// Load gets the data from the database
func (l *Loader) Load(key string) string {
	val, err := l.DB.Get(key)
	if err != nil {
		panic(err)
	}

	return val
}

func run(t *testing.T) (*KeyStoreCache, *MockDB) {
	loader := Loader{
		DB: GetMockDB(),
	}
	cache := New(&loader)

	RunMockServer(cache, t)

	return cache, loader.DB
}

func main() {
	run(nil)
}
