// Copyright 2017, 2026 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package boma

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/oklog/ulid/v2"
)

const defaultMaxDoneFiles = 1000

// Vault is a safe file storage for in-flight (in-process) messages.
type Vault struct {
	path         string
	list         []string
	done         []string
	maxDoneFiles int
	sync.Mutex
}

// NewVault prepares a vault under the given path.
//
// Files in writing are under $path/tmp,
// removed files move under $path/done,
// all the current content is under $path.
func NewVault(path string) (*Vault, error) {
	dn := path
	os.MkdirAll(dn, 0755)
	dis, err := os.ReadDir(dn)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(dis))
	for _, di := range dis {
		if !di.Type().IsRegular() || !strings.HasSuffix(di.Name(), ".xml") {
			continue
		}
		if fi, err := di.Info(); err != nil {
			continue
		} else if fi.Size() == 0 {
			os.Remove(filepath.Join(dn, di.Name()))
			continue
		}
		names = append(names, di.Name())
	}
	slices.Sort(names)

	dn = filepath.Join(path, "tmp")
	os.RemoveAll(dn)
	os.MkdirAll(dn, 0755)

	dn = filepath.Join(path, "done")
	os.MkdirAll(dn, 0755)
	dis, _ = os.ReadDir(dn)
	dones := make([]string, 0, len(dis))
	for _, di := range dis {
		if !di.Type().IsRegular() {
			continue
		}
		if fi, err := di.Info(); err != nil {
			continue
		} else if fi.Size() == 0 {
			os.Remove(filepath.Join(dn, di.Name()))
			continue
		}
		dones = append(dones, di.Name())
	}
	slices.Sort(dones)
	return &Vault{path: path, list: names, done: dones, maxDoneFiles: defaultMaxDoneFiles}, nil
}

type vaultFile struct {
	f      *os.File
	dst    string
	length int64
}

func (vf *vaultFile) Name() string { return vf.f.Name() }
func (vf *vaultFile) Close() error { return vf.CloseWithError(nil) }
func (vf *vaultFile) Write(p []byte) (int, error) {
	n, err := vf.f.Write(p)
	vf.length += int64(n)
	return n, err
}
func (vf *vaultFile) CloseWithError(err error) error {
	if err != nil && !errors.Is(err, io.EOF) {
		if errors.Is(err, ErrEmptyMessage) {
			vf.f.Close()
			os.Remove(vf.f.Name())
			return nil
		}
		slog.Error("CloseWithError", "length", vf.length, "error", err)
	}
	if err1 := vf.f.Truncate(vf.length); err == nil {
		err = err1
	}
	if err1 := vf.f.Close(); err == nil {
		err = err1
	}
	fn := vf.f.Name()
	if err != nil {
		os.Remove(fn)
		return err
	}
	return os.Rename(fn, filepath.Join(vf.dst, filepath.Base(fn)))
}

// NewWriter returns a new Writer with the preallocated size,
// named with an ULID under the tmp dir.
func (v *Vault) NewWriter(size int) (*vaultFile, error) {
	v.Lock()
	defer v.Unlock()
	name := ulid.Make().String() + ".xml"
	fn := filepath.Join(v.path, "tmp", name)

	f, err := os.OpenFile(fn, os.O_EXCL|os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_SYNC, 0644)
	if err != nil {
		return nil, err
	}
	// allocate the space
	if err := f.Truncate(int64(size)); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, err
	}
	return &vaultFile{f: f, dst: v.path}, nil
}

// Get the next (first) file from the vault.
func (v *Vault) Get() (ReadDeleteNamer, error) {
	v.Lock()
	defer v.Unlock()
	if len(v.list) == 0 {
		return nil, io.EOF
	}
	name := v.list[0]
	fn := filepath.Join(v.path, name)
	fh, err := os.Open(fn)
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}
	return namedReadDelete{
		ReadCloser: fh,
		delete:     func() error { return v.Remove(name) },
		name:       name,
	}, nil
}

// Put the data into the vault (into a new file).
func (v *Vault) Put(data []byte) (func() error, error) {
	f, err := v.NewWriter(len(data))
	if err != nil {
		return nil, err
	}
	n, err := f.Write(data)
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	}
	fn := f.Name()
	if err = f.CloseWithError(err); err != nil {
		os.Remove(fn)
		return nil, err
	}

	name := filepath.Base(fn)
	v.list = append(v.list, name)

	return func() error { return v.Remove(name) }, nil
}

// Len returns the number of messages in the vault.
func (v *Vault) Len() int {
	v.Lock()
	n := len(v.list)
	v.Unlock()
	return n
}

// Remove (move to the done folder) the named file from the vault.
func (v *Vault) Remove(name string) error {
	name = filepath.Base(name)
	v.Lock()
	defer v.Unlock()
	fn := filepath.Join(v.path, name)
	var err error
	if err = os.Rename(fn, filepath.Join(v.path, "done", name)); err != nil {
		err = os.Remove(fn)
	} else {
		v.done = append(v.done, name)
		for len(v.done) > v.maxDoneFiles {
			os.Remove(filepath.Join(v.path, "done", name))
			v.done = v.done[1:]
		}
	}
	for i, x := range v.list {
		if x != name {
			continue
		}
		v.list = append(v.list[:i], v.list[i+1:]...)
	}
	return err
}

type ReadDeleteNamer interface {
	io.ReadCloser
	Delete() error
	Name() string
}
type namedReadDelete struct {
	io.ReadCloser
	delete func() error
	name   string
}

func (rd namedReadDelete) Delete() error { return rd.delete() }
func (rd namedReadDelete) Name() string  { return rd.name }
