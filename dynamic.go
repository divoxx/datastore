package datastore

import (
	"encoding/binary"
	"errors"
	"log"
	"os"
)

const (
	fileFlags = os.O_CREATE | os.O_RDWR
	fileMode  = 0666

	maskUsed = 1 << iota
)

var byteOrder binary.ByteOrder

func init() {
	byteOrder = binary.LittleEndian
}

type DynamicStore struct {
	path      string
	fd        *os.File
	nextId    uint32
	blockSize uint16
	debugMode bool
}

type blockType struct {
	Flags  uint8
	Length uint16
	Next   uint32
}

func NewDynamicStore(path string, blockSize uint16) (store *DynamicStore) {
	store = &DynamicStore{path: path, blockSize: blockSize, nextId: 1}
	return
}

func (store *DynamicStore) Write(data []byte) (uint32, error) {
	var prev *blockType

	id, offset := store.nextBlock()

	blockId := id
	blockNum := 0

	if err := store.open(); err != nil {
		return 0, err
	}

	defer store.close()

	for {
		block := store.newBlock()

		chunk, last := store.readDataChunk(data, blockNum)

		block.Length = uint16(len(chunk))

		if prev != nil {
			prev.Next = blockId
		}

		// We defer to after the method cause only then all previous ref will be set
		defer store.writeBlock(blockId, offset, block, chunk)

		if last {
			break
		}

		prev = block
		blockId, offset = store.nextBlock()
		blockNum += 1
	}

	return id, nil
}

func (store *DynamicStore) writeBlock(id uint32, offset int64, block *blockType, chunk []byte) {
	var err error

	store.log("Writing block #%d to disk: %v", id, block)

	store.fd.Seek(offset, os.SEEK_SET)

	if err = binary.Write(store.fd, byteOrder, block); err != nil {
		panic(err)
	}

	if _, err = store.fd.Write(chunk); err != nil {
		panic(err)
	}
}

func (store *DynamicStore) readDataChunk(data []byte, num int) ([]byte, bool) {
	var last bool

	chunkSize := int(store.blockSize) - 56
	start := num * chunkSize
	end := start + chunkSize

	if end > len(data) {
		last = true
		end = len(data)
	}

	return data[start:end], last
}

func (store *DynamicStore) Read(slot_id uint32) ([]byte, error) {
	var err error

	block_id := slot_id
	data := make([]byte, 0)

	if err := store.open(); err != nil {
		return nil, err
	}

	defer store.close()

	for {
		offset := store.offsetFor(block_id)

		block := store.newBlock()

		store.fd.Seek(offset, os.SEEK_SET)

		if err = binary.Read(store.fd, byteOrder, block); err != nil {
			return nil, err
		}

		store.log("Reading block #%d (%d): %v", block_id, offset, block)

		if block.Flags&maskUsed == 0 {
			err := errors.New("Tried to read unused slot from dynamic store")
			return nil, err
		}

		chunk := make([]byte, block.Length)
		if _, err = store.fd.Read(chunk); err != nil {
			return nil, err
		}

		data = append(data, chunk...)

		if block.Next == 0 {
			break
		}

		block_id = block.Next
	}

	return data, nil
}

func (store *DynamicStore) newBlock() *blockType {
	block := new(blockType)
	block.Flags |= maskUsed
	return block
}

func (store *DynamicStore) nextBlock() (id uint32, offset int64) {
	id = store.nextId
	offset = int64(id-1) * int64(store.blockSize)
	defer func() { store.nextId += 1 }()
	return id, offset
}

func (store *DynamicStore) offsetFor(id uint32) (offset int64) {
	return int64(id-1) * int64(store.blockSize)
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

func (store *DynamicStore) log(data ...interface{}) {
	if store.debugMode {
		if str, ok := data[0].(string); ok {
			log.Printf(str, data[1:]...)
		}
	}
}
