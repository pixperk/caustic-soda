package main

import "sync"

type Store struct {
	storageMu sync.Mutex
	storage   map[string]string
}

func NewStore() *Store {
	return &Store{
		storage: make(map[string]string),
	}
}

func (s *Store) Set(key, value string) {
	s.storageMu.Lock()
	defer s.storageMu.Unlock()
	s.storage[key] = value
}

func (s *Store) Get(key string) (string, bool) {
	s.storageMu.Lock()
	defer s.storageMu.Unlock()
	value, ok := s.storage[key]
	return value, ok
}

func main() {
	demoLostUpdate()
}
