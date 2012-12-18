package datastore

type Id uint64

type IterFunc func(uint, []byte)

type Store interface {
	Write([]byte) (Id, error)
	Read(Id) ([]byte, error)
	Scan(IterFunc)
}
