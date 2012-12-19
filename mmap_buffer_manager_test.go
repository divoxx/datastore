package datastore

import (
	"bytes"
	"testing"
)

const (
	fixtureSize = 4096
	fixtureFile = "test_fixtures/random_4k.data"
)

func TestWriteToBuffer(t *testing.T) {
	data := []byte("test")

	mngr := NewMMapBufferManager(fixtureFile)

	buf, err := mngr.Acquire(0, 512)
	if err != nil {
		t.Error("Error acquiring buffer")
	}

	copy(buf, data)

	err = mngr.Release(buf)
	if err != nil {
		t.Error("Error releasing buffer")
	}

	buf, err = mngr.Acquire(0, 512)
	if err != nil {
		t.Error("Error acquiring buffer")
	}

	if !bytes.Equal(buf[0:len(data)], data) {
		t.Errorf("Data not properly persisted to file: %s", buf[0:len(data)])
	}
}

func TestAcquireUnalignedBuffer(t *testing.T) {
	mngr := NewMMapBufferManager(fixtureFile)

	_, err := mngr.Acquire(1, 511)
	if _, ok := err.(UnalignedBuffer); !ok {
		t.Error("Unaligned offset should return an error")
	}

	_, err = mngr.Acquire(0, 511)
	if _, ok := err.(UnalignedBuffer); !ok {
		t.Error("Unaligned offset should return an error")
	}
}

func TestConcurrencyWrite(t *testing.T) {
	mngr := NewMMapBufferManager(fixtureFile)

	bufA, err := mngr.Acquire(0, 512)
	if err != nil {
		t.Error(err)
	}

	bufB, err := mngr.Acquire(0, 512)
	if err != nil {
		t.Error(err)
	}

	copy(bufA, []byte("buffer A"))
	copy(bufB, []byte("buffer B"))

	err = mngr.Release(bufB)
	if err != nil {
		t.Error(err)
	}

	err = mngr.Release(bufA)
	if err != nil {
		t.Error(err)
	}

	bufC, err := mngr.Acquire(0, 512)
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(bufC[0:8], []byte("buffer B")) {
		t.Errorf("Expected final value to be buffer B's data: %s", bufC[0:8])
	}
}
