package latches

import (
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
)

// Latches Latching provides atomicity of TinyKV commands. This should not be confused with SQL transactions which provide atomicity
// for multiple TinyKV commands. For example, consider two commit commands, these write to multiple keys/CFs so if they race,
// then it is possible for inconsistent data to be written. By latching the keys each command might write, we ensure that the
// two commands will not race to write the same keys.
//
// A latch is a per-key lock. There is only one latch per user key, not one per CF or one for each encoded key. Latches are
// only needed for writing. Only one thread can hold a latch at a time and all keys that a command might write must be locked
// at once.
//
// Latching is implemented using a single map which maps keys to a Go WaitGroup. Access to this map is guarded by a mutex
// to ensure that latching is atomic and consistent. Since the mutex is a global lock, it would cause intolerable contention
// in a real system.
// Latching 提供了 TinyKV 命令的原子性。 这不应与提供原子性的 SQL 事务混淆
// 对于多个 TinyKV 命令。 例如，考虑两个提交命令，它们写入多个键/CF，所以如果它们竞争，
// 那么就有可能写入不一致的数据。
// 通过锁定每个命令可能写入的键，我们确保两个命令不会竞相写入相同的键。
//
// 闩锁是一个按键锁。 每个用户密钥只有一个锁存器，而不是每个 CF 一个或每个编码密钥一个。
// 锁存器仅用于写入。 一次只有一个线程可以持有一个闩锁，并且命令可能写入的所有键都必须立即锁定。
//
// Latching 使用单个映射实现，该映射将键映射到 Go WaitGroup。
// 对这个映射的访问由互斥锁保护，以确保锁存是原子的和一致的。
// 由于互斥锁是全局锁，它会在实际系统中引起无法容忍的争用。
type Latches struct {
	// Before modifying any property of a key, the thread must have the latch for that key. `Latches` maps each latched
	// key to a WaitGroup. Threads who find a key locked should wait on that WaitGroup.
	// 在修改键的任何属性之前，线程必须具有该键的闩锁。
	// `Latches` 将每个锁存的键映射到一个 WaitGroup。
	// 发现一个键被锁定的线程应该在那个 WaitGroup 上等待。
	latchMap map[string]*sync.WaitGroup
	// Mutex to guard latchMap. A thread must hold this mutex while it makes any change to latchMap.
	// Mutex 来保护latchMap。 线程在对latchMap 进行任何更改时必须持有此互斥锁。
	latchGuard sync.Mutex
	// An optional validation function, only used for testing.
	// 一个可选的验证函数，仅用于测试。
	Validation func(txn *mvcc.MvccTxn, keys [][]byte)
}

// NewLatches creates a new Latches object for managing a databases latches. There should only be one such object, shared
// between all threads.
// NewLatches 创建一个新的 Latches 对象用于管理数据库锁存器。
// 应该只有一个这样的对象，在所有线程之间共享。
func NewLatches() *Latches {
	l := new(Latches)
	l.latchMap = make(map[string]*sync.WaitGroup)
	return l
}

// AcquireLatches tries lock all Latches specified by keys. If this succeeds, nil is returned. If any of the keys are
// locked, then AcquireLatches requires a WaitGroup which the thread can use to be woken when the lock is free.
func (l *Latches) AcquireLatches(keysToLatch [][]byte) *sync.WaitGroup {
	l.latchGuard.Lock()
	defer l.latchGuard.Unlock()

	// Check none of the keys we want to write are locked.
	for _, key := range keysToLatch {
		if latchWg, ok := l.latchMap[string(key)]; ok {
			// Return a wait group to wait on.
			return latchWg
		}
	}

	// All Latches are available, lock them all with a new wait group.
	wg := new(sync.WaitGroup)
	wg.Add(1)
	for _, key := range keysToLatch {
		l.latchMap[string(key)] = wg
	}

	return nil
}

// ReleaseLatches releases the latches for all keys in keysToUnlatch. It will wakeup any threads blocked on one of the
// latches. All keys in keysToUnlatch must have been locked together in one call to AcquireLatches.
func (l *Latches) ReleaseLatches(keysToUnlatch [][]byte) {
	l.latchGuard.Lock()
	defer l.latchGuard.Unlock()

	first := true
	for _, key := range keysToUnlatch {
		if first {
			wg := l.latchMap[string(key)]
			wg.Done()
			first = false
		}
		delete(l.latchMap, string(key))
	}
}

// WaitForLatches attempts to lock all keys in keysToLatch using AcquireLatches. If a latch ia already locked, then =
// WaitForLatches will wait for it to become unlocked then try again. Therefore WaitForLatches may block for an unbounded
// length of time.
func (l *Latches) WaitForLatches(keysToLatch [][]byte) {
	for {
		wg := l.AcquireLatches(keysToLatch)
		if wg == nil {
			return
		}
		wg.Wait()
	}
}

// Validate calls the function in Validation, if it exists.
func (l *Latches) Validate(txn *mvcc.MvccTxn, latched [][]byte) {
	if l.Validation != nil {
		l.Validation(txn, latched)
	}
}
