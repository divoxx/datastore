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

type Flag uint8

const (
	FlagUsed Flag = 1 << iota
	FlagLast
)

const (
	syscallProtRead   = 0x02
	syscallProtWrite  = 0x04
	syscallFlagShared = 0x1
)

const (
	blockSize = 512
	dataSize  = 456
)

type blockHeader struct {
	Flags  Flag
	Len    uint16
	NextId Id
}

type FileStore struct {
	path    string
	fd      *os.File
	fdMtx   sync.Mutex
	free    *list.List
	freeMtx sync.Mutex
}

func NewFileStore(path string) (*FileStore, error) {
	var (
		store *FileStore
		err   error
	)

	store = new(FileStore)

	if store.fd, err = os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_SYNC, 0600); err != nil {
		return nil, fmt.Errorf("Can't open database file: %s", err.Error())
	}

	store.free = list.New()

	return store, nil
}

func (store *FileStore) Write(data []byte) (Id, error) {
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

		header.Flags = FlagUsed
		header.Len = uint16(r - l)

		if i == 0 {
			header.Flags |= FlagLast
		} else {
			header.NextId = id
		}

		if id, memSlice, err = store.acquireNewBlock(); err != nil {
			return 0, err
		}

		buffer = bytes.NewBuffer(memSlice[:0])

		binary.Write(buffer, binary.BigEndian, header)
		binary.Write(buffer, binary.BigEndian, data[l:r])

		store.releaseBlock(memSlice)
	}

	return id, nil
}

func (store *FileStore) Read(id Id) ([]byte, error) {
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

		if header.Flags&FlagLast > 0 {
			break
		} else {
			id = header.NextId
		}
	}

	return data, nil
}

func (store *FileStore) acquireNewBlock() (Id, []byte, error) {
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

func (store *FileStore) acquireBlock(id Id) ([]byte, error) {
	memSlice, err := syscall.Mmap(int(store.fd.Fd()), int64(id)*blockSize, blockSize, syscallProtRead|syscallProtWrite, syscallFlagShared)
	return memSlice, err
}

func (store *FileStore) releaseBlock(memSlice []byte) error {
	return syscall.Munmap(memSlice)
}
