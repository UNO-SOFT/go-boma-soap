// Copyright 2017 Tamás Gulácsi. All rights reserved.

package soap

import (
	"fmt"
	"io"
	"os"
	"testing"
)

func TestVault(t *testing.T) {
	dn, err := os.MkdirTemp("", "test-vault-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dn)
	vault, err := NewVault(dn)
	if err != nil {
		t.Fatal(err)
	}
	for i := range vault.maxDoneFiles {
		want := fmt.Sprintf("%03d", i)
		if _, err := vault.Put([]byte(want)); err != nil {
			t.Fatal(err)
		}
		rd, err := vault.Get()
		if err != nil {
			t.Fatalf("get %s: %+v", want, err)
		}
		got, err := io.ReadAll(rd)
		if err != nil {
			t.Fatal(err)
		}
		rd.Close()
		rd.Delete()
		if string(got) != want {
			t.Errorf("got %q, wanted %q", got, want)
		}
	}
}
