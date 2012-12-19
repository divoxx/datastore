package datastore

import (
	"fmt"
	"launchpad.net/gommap"
	"os"
	"sync"
)

const (
	fileMode = 0600
)

var (
	pageSize = os.Getpagesize()
)

type UnalignedBuffer struct {
	offset int64
	length int64
}

func (err UnalignedBuffer) Error() string {
	return fmt.Sprintf("Can't acquire buffer for unaligned offset (%d) and length (%d)", err.offset, err.length)
}

type SectionAccrossPages struct {
	offset int64
	length int64
}

func (err SectionAccrossPages) Error() string {
	return fmt.Sprintf("Section is spread accross different pages! offset: %d; length: %d", err.offset, err.length)
}

type mMapBufferManager struct {
	fd  *os.File
	mtx map[int]sync.Mutex
}

func NewMMapBufferManager(path string) BufferManager {
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, fileMode)
	if err != nil {
		panic(err)
	}

	return &mMapBufferManager{fd: fd}
}

func (mngr *mMapBufferManager) Acquire(offset, length int64) ([]byte, error) {
	if int64(pageSize)%length != 0 || offset%length != 0 {
		return nil, UnalignedBuffer{offset, length}
	}

	page, err := pageForSection(offset, length)
	if err != nil {
		return nil, err
	}

	relOffset := offset % int64(pageSize)

	mmap, err := gommap.MapAt(0, mngr.fd.Fd(), int64(page)*int64(pageSize), int64(pageSize), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	return []byte(mmap)[relOffset : relOffset+length], err
}

func (mngr *mMapBufferManager) Release(buf []byte) error {
	mmap := gommap.MMap(buf)

	err := mmap.Sync(gommap.MS_SYNC)
	if err != nil {
		return err
	}

	err = mmap.Unlock()
	return err
}

func pageForSection(offset, length int64) (int, error) {
	offsetPage := offset / int64(pageSize)
	lengthPage := length / int64(pageSize)

	if offsetPage != lengthPage {
		return 0, SectionAccrossPages{offset, length}
	}

	return int(offsetPage), nil
}
