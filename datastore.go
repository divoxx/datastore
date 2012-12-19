// Implements different types of data storage, abstracting away the managing and
// structuring of data in a way that is efficient.
package datastore

// Id represents the location of a data in the store. It is used to read a data
// after it has been written in the store.
type Id uint32

// Store represents a storage engine interface, independent of the implementation.
type Store interface {
	// Persist writtes the given data in the store and returns an Id for further
	// retrieveing of the data.
	Persist([]byte) (Id, error)

	// Retrieve an stored data previously persisted.
	Retrieve(Id) ([]byte, error)
}

// Manage buffers for access to the underlying storage, making sure it respect
// memory constraints and efficiently read/write data.
type BufferManager interface {
	Acquire(offset, length int64) ([]byte, error)
	Release([]byte) error
}
