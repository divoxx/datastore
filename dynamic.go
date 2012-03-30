package datastore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
)

const (
	blockSize = 512
	dataSize  = blockSize - 64
	fileFlags = os.O_CREATE | os.O_RDWR
	fileMode  = 0666

	maskUsed = 1 << iota
)

var byteOrder binary.ByteOrder

func init() {
	byteOrder = binary.LittleEndian
}

type DynamicStore struct {
	path    string
	fd      *os.File
	_nextId uint32
}

type block struct {
	Flags  uint16
	Length uint16
	Next   uint32
	Data   [dataSize]byte
}

func NewDynamicStore(filepath string) (store *DynamicStore) {
	store = &DynamicStore{path: filepath}
	return
}

func (store *DynamicStore) nextId() uint32 {
	store._nextId += 1
	return store._nextId
}

func (store *DynamicStore) open() error {
	fd, err := os.OpenFile(store.path, fileFlags, fileMode)

	if err != nil {
		return err
	}

	store.fd = fd
	return nil
}

func (store *DynamicStore) close() error {
	if err := store.fd.Sync(); err != nil {
		return err
	}

	if err := store.fd.Close(); err != nil {
		return err
	}

	return nil
}

func (store *DynamicStore) Write(data []byte) (uint32, error) {
	buffer := bytes.NewBuffer(data)

	var (
		slot_id uint32
		prev    *block
	)

	if err := store.open(); err != nil {
		return 0, err
	}

	defer store.close()

	for {
		block_id := store.nextId()

		offset := int64((block_id - 1) * blockSize)
		if slot_id == 0 {
			slot_id = block_id
		}

		block := new(block)
		block.Flags |= maskUsed

		length, err := buffer.Read(block.Data[:])

		if err == io.EOF {
			break
		}

		block.Length = uint16(length)

		if prev != nil {
			prev.Next = block_id
		}

		prev = block

		defer func() {
			store.fd.Seek(offset, os.SEEK_SET)
			binary.Write(store.fd, byteOrder, block)
		}()

		if err == io.EOF {
			break
		}
	}

	return slot_id, nil
}

func (store *DynamicStore) Read(slot_id uint32) ([]byte, error) {
	block_id := slot_id
	data := make([]byte, 0)

	if err := store.open(); err != nil {
		return nil, err
	}

	defer store.close()

	for {
		offset := int64((block_id - 1) * blockSize)
		buffer := bytes.NewBuffer(nil)

		store.fd.Seek(offset, os.SEEK_SET)
		_, err := buffer.ReadFrom(store.fd)

		if err != nil {
			break
		}

		block := new(block)
		binary.Read(buffer, byteOrder, block)

		if block.Flags&maskUsed == 0 {
			err = errors.New("Tried to read unused slot from dynamic store")
			return nil, err
		}

		data = append(data, block.Data[:block.Length]...)

		if block.Next == 0 {
			break
		}

		block_id = block.Next
	}

	return data, nil
}
