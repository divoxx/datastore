package datastore

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"syscall"
)

type flag uint8

const (
	flagUsed flag = 1 << iota
	flagLast
)

const (
	syscallProtRead   = 0x02
	syscallProtWrite  = 0x04
	syscallFlagShared = 0x1
)

const (
	blockSize = 512
	dataSize  = 488
)

type blockHeader struct {
	Flags  flag
	Len    uint16
	NextId Id
}

type fileStore struct {
	path    string
	fd      *os.File
	fdMtx   sync.Mutex
	free    *list.List
	freeMtx sync.Mutex
}

func NewFileStore(path string) (Store, error) {
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_SYNC, 0600)

	if err != nil {
		return nil, fmt.Errorf("Can't open database file: %s", err.Error())
	}

	store := &fileStore{path: path, fd: fd, free: list.New()}
	return Store(store), nil
}

func (store *fileStore) Persist(data []byte) (Id, error) {
	var (
		id          Id
		err         error
		blocksCount uint
	)

	blocksCount = uint(len(data) / dataSize)

	if len(data)%dataSize > 0 {
		blocksCount++
	}

	for i := uint(0); i < blocksCount; i++ {
		var (
			header   blockHeader
			memSlice []byte
			buffer   *bytes.Buffer
			l, r     uint
		)

		l = (blocksCount - i - 1) * dataSize

		if i == 0 {
			r = uint(len(data))
		} else {
			r = l + dataSize
		}

		header.Flags = flagUsed
		header.Len = uint16(r - l)

		if i == 0 {
			header.Flags |= flagLast
		} else {
			header.NextId = id
		}

		if id, memSlice, err = store.acquireNewBlock(); err != nil {
			return 0, err
		}

		buffer = bytes.NewBuffer(memSlice[:0])

		binary.Write(buffer, binary.BigEndian, header)
		buffer.Write(data[l:r])
		// binary.Write(buffer, binary.BigEndian, data[l:r])

		store.releaseBlock(memSlice)
	}

	return id, nil
}

func (store *fileStore) Retrieve(id Id) ([]byte, error) {
	var (
		err  error
		data []byte
	)

	data = make([]byte, 0)

	for {
		var (
			header   blockHeader
			memSlice []byte
			buffer   *bytes.Reader
		)

		if memSlice, err = store.acquireBlock(id); err != nil {
			return nil, err
		}

		buffer = bytes.NewReader(memSlice)

		binary.Read(buffer, binary.BigEndian, &header)

		data = append(data, make([]byte, header.Len)...)
		buffer.Read(data[len(data)-int(header.Len):])

		store.releaseBlock(memSlice)

		if header.Flags&flagLast > 0 {
			break
		} else {
			id = header.NextId
		}
	}

	return data, nil
}

func (store *fileStore) acquireNewBlock() (Id, []byte, error) {
	var (
		err      error
		fileInfo os.FileInfo
		id       Id
		memSlice []byte
	)

	if store.free.Len() > 0 {
		store.freeMtx.Lock()

		el := store.free.Front()
		store.free.Remove(el)

		store.freeMtx.Unlock()

		id = el.Value.(Id)
	} else {
		store.fdMtx.Lock()

		if fileInfo, err = store.fd.Stat(); err != nil {
			return Id(0), nil, err
		}

		id = Id(fileInfo.Size() / blockSize)
		store.fd.Truncate(fileInfo.Size() + blockSize)

		store.fdMtx.Unlock()
	}

	memSlice, err = store.acquireBlock(id)
	return id, memSlice, err
}

func (store *fileStore) acquireBlock(id Id) ([]byte, error) {
	memSlice, err := syscall.Mmap(int(store.fd.Fd()), int64(id)*blockSize, blockSize, syscallProtRead|syscallProtWrite, syscallFlagShared)
	return memSlice, err
}

func (store *fileStore) releaseBlock(memSlice []byte) error {
	return syscall.Munmap(memSlice)
}
