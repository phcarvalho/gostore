package main

import (
	"os"
	"testing"
)

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}

	return !info.IsDir()
}

func TestCreateLogger(t *testing.T) {
	const filename = "/tmp/create-logger.txt"
	defer os.Remove(filename)

	tl, err := NewTransactionLogger(filename)

	if err != nil {
		t.Error("Got error:", err)
	}

	if tl == nil {
		t.Error("Logger is nil")
	}


	if !fileExists(filename) {
		t.Errorf("File %s doesn't exist", filename)
	}
}

func TestWriteAppend(t *testing.T) {
	const filename = "/tmp/write-append.txt"
	defer os.Remove(filename)

	tl, err := NewTransactionLogger(filename)
	if err != nil {
		t.Error(nil)
	}
	tl.Run()
	defer tl.Close()

	chev, cherr := tl.ReadEvents()

	count := 0
	for e := range chev {
		count++
		t.Log(e)
	}
	err = <-cherr
	if err != nil {
		t.Error(err)
	}

	if count != 0 {
		t.Error("shouldn't have any items on transaction")
	}

	tl.WritePut("my-key", "my-value")
	tl.WritePut("my-key", "my-value2")
	tl.Wait()

	tl2, err := NewTransactionLogger(filename)
	if err != nil {
		t.Error(nil)
	}
	tl2.Run()
	defer tl2.Close()

	chev, cherr = tl2.ReadEvents()

	for e := range chev {
		count++
		t.Log(e)
	}
	err = <-cherr
	if err != nil {
		t.Error(err)
	}

	if count != 2 {
		t.Error("should read 2 items from the transaction log")
	}

	tl2.WritePut("my-key", "my-value3")
	tl2.WritePut("my-key2", "my-value4")
	tl2.Wait()

	if tl2.lastSequence != 4 {
		t.Errorf("Last sequence mismatch (expected 4, got %d)", tl2.lastSequence)
	}
}
