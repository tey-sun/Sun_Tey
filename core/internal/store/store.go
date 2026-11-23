package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tey-sun/suntey/core/internal/model"
	bolt "go.etcd.io/bbolt"
)

var (
	bucketConfig = []byte("config")
	bucketTasks  = []byte("tasks")

	ErrNotFound = errors.New("not found")
)

// Store persists config and tasks in a single bbolt file.
type Store struct {
	db *bolt.DB
}

// Open opens (creating parents) the bbolt file at path.
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("bolt open: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketConfig)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(bucketTasks)
		return err
	}); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Path is the bbolt file path. Callers keep sidecar-adjacent artifacts (spooled
// .torrent files) next to the database, so a portable install stays self-contained.
func (s *Store) Path() string { return s.db.Path() }

// LoadConfig returns the persisted config, or a default (not persisted).
func (s *Store) LoadConfig() (model.Config, error) {
	var cfg model.Config
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketConfig).Get([]byte("v"))
		if b == nil {
			return nil
		}
		return json.Unmarshal(b, &cfg)
	})
	return cfg, err
}

func (s *Store) SaveConfig(cfg model.Config) error {
	b, _ := json.Marshal(cfg)
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketConfig).Put([]byte("v"), b)
	})
}

func (s *Store) PutTask(t *model.Task) error {
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketTasks).Put([]byte(t.ID), b)
	})
}

func (s *Store) GetTask(id string) (*model.Task, error) {
	var t model.Task
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketTasks).Get([]byte(id))
		if b == nil {
			return ErrNotFound
		}
		return json.Unmarshal(b, &t)
	})
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (s *Store) ListTasks() ([]*model.Task, error) {
	var out []*model.Task
	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketTasks).Cursor()
		for _, v := c.First(); v != nil; _, v = c.Next() {
			var t model.Task
			if err := json.Unmarshal(v, &t); err != nil {
				return err
			}
			tt := t
			out = append(out, &tt)
		}
		return nil
	})
	return out, err
}

func (s *Store) DeleteTask(id string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketTasks).Delete([]byte(id))
	})
}
