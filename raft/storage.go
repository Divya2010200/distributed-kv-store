package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Storage keeps HardState safe. Save must not return until the
// state would survive a crash.
type Storage interface {
	Load() (HardState, bool, error) // bool = was anything saved before?
	Save(HardState) error
}

// ---------- MemoryStorage (for tests) ----------

type MemoryStorage struct {
	mu    sync.Mutex
	state HardState
	saved bool
}

func (m *MemoryStorage) Load() (HardState, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return copyState(m.state), m.saved, nil
}

func (m *MemoryStorage) Save(s HardState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = copyState(s)
	m.saved = true
	return nil
}

// copyState makes a deep copy, so changing the node's log later
// can't secretly change what's "on disk".
func copyState(s HardState) HardState {
	out := HardState{Term: s.Term, VotedFor: s.VotedFor, Log: make([]Entry, len(s.Log))}
	for i, e := range s.Log {
		out.Log[i] = Entry{Term: e.Term, Command: append([]byte(nil), e.Command...)}
	}
	return out
}

// ---------- FileStorage (for real nodes) ----------

type FileStorage struct {
	path string
}

func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStorage{path: filepath.Join(dir, "raft-state.json")}, nil
}

func (f *FileStorage) Load() (HardState, bool, error) {
	data, err := os.ReadFile(f.path)
	if errors.Is(err, os.ErrNotExist) {
		return HardState{}, false, nil
	}
	if err != nil {
		return HardState{}, false, err
	}
	var s HardState
	if err := json.Unmarshal(data, &s); err != nil {
		return HardState{}, false, fmt.Errorf("decode %s: %w", f.path, err)
	}
	return s, true, nil
}

func (f *FileStorage) Save(s HardState) error {
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	tmp := f.path + ".tmp"
	file, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, f.path)
}
