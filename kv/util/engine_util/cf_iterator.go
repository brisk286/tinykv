package engine_util

import (
	"github.com/Connor1996/badger"
)

type CFItem struct {
	item      *badger.Item
	prefixLen int
}

// String returns a string representation of Item
func (i *CFItem) String() string {
	return i.item.String()
}

func (i *CFItem) Key() []byte {
	return i.item.Key()[i.prefixLen:]
}

func (i *CFItem) KeyCopy(dst []byte) []byte {
	return i.item.KeyCopy(dst)[i.prefixLen:]
}

func (i *CFItem) Version() uint64 {
	return i.item.Version()
}

func (i *CFItem) IsEmpty() bool {
	return i.item.IsEmpty()
}

func (i *CFItem) Value() ([]byte, error) {
	return i.item.Value()
}

func (i *CFItem) ValueSize() int {
	return i.item.ValueSize()
}

func (i *CFItem) ValueCopy(dst []byte) ([]byte, error) {
	return i.item.ValueCopy(dst)
}

func (i *CFItem) IsDeleted() bool {
	return i.item.IsDeleted()
}

func (i *CFItem) EstimatedSize() int64 {
	return i.item.EstimatedSize()
}

func (i *CFItem) UserMeta() []byte {
	return i.item.UserMeta()
}

// BadgerIterator badger的迭代器，使用cf做前缀
type BadgerIterator struct {
	iter   *badger.Iterator
	prefix string
}

//返回badger迭代器
func NewCFIterator(cf string, txn *badger.Txn) *BadgerIterator {
	return &BadgerIterator{
		iter:   txn.NewIterator(badger.DefaultIteratorOptions),
		prefix: cf + "_",
	}
}

func (it *BadgerIterator) Item() DBItem {
	return &CFItem{
		item:      it.iter.Item(),
		prefixLen: len(it.prefix),
	}
}

func (it *BadgerIterator) Valid() bool { return it.iter.ValidForPrefix([]byte(it.prefix)) }

func (it *BadgerIterator) ValidForPrefix(prefix []byte) bool {
	return it.iter.ValidForPrefix(append([]byte(it.prefix), prefix...))
}

func (it *BadgerIterator) Close() {
	it.iter.Close()
}

func (it *BadgerIterator) Next() {
	it.iter.Next()
}

func (it *BadgerIterator) Seek(key []byte) {
	//key = cf_key
	it.iter.Seek(append([]byte(it.prefix), key...))
}

func (it *BadgerIterator) Rewind() {
	it.iter.Rewind()
}

type DBIterator interface {
	// Item returns pointer to the current key-value pair.
	// Item 返回指向当前键值对的指针。
	Item() DBItem
	// Valid returns false when iteration is done.
	// Valid 在迭代完成时返回 false。
	Valid() bool
	// Next would advance the iterator by one. Always check it.Valid() after a Next()
	// to ensure you have access to a valid it.Item().
	// Next 将迭代器前进一个。
	// 始终在 Next() 之后检查 it.Valid() 以确保您可以访问有效的 it.Item()。
	Next()
	// Seek would seek to the provided key if present. If absent, it would seek to the next smallest key
	// greater than provided.
	// 如果存在，Seek 将寻找提供的键。如果不存在，它将寻找比提供的更大的下一个最小密钥。
	Seek([]byte)

	// Close the iterator
	Close()
}

type DBItem interface {
	// Key returns the key.
	Key() []byte
	// KeyCopy returns a copy of the key of the item, writing it to dst slice.
	// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
	// returned.
	// KeyCopy 返回item key的副本，将其写入 dst 切片。
	// 如果传递了 nil，或者 dst 的容量不足，则会分配并返回一个新切片。
	KeyCopy(dst []byte) []byte
	// Value retrieves the value of the item.
	// Value 检索项目的值。
	Value() ([]byte, error)
	// ValueSize returns the size of the value.
	ValueSize() int
	// ValueCopy returns a copy of the value of the item from the value log, writing it to dst slice.
	// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
	// returned.
	// ValueCopy 从 value 日志中返回 item 的 value 的副本，将其写入 dst slice。
	// 如果传递了 nil，或者 dst 的容量不足，则会分配并返回一个新的 slice
	ValueCopy(dst []byte) ([]byte, error)
}
