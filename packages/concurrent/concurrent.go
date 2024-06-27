package concurrent

import (
	"container/list"
	"sync/atomic"
	"unsafe"
	"sync"
)

/*** TYPE DEFINITIONS ***/

// Node is an element of a concurrent collection.
// Node stores a value of type interface{} and pointers to the following node.
type Node struct {
	// Val is the value of type interface{} located in the position represented by the Node structure.
	Val	interface{}
	// Nxt is a pointer to the following Node.
	// Nxt is nil for the last position of a collection (the bottom of a stack and the tail of a queue).
	Nxt	unsafe.Pointer
}

// ConcurrentCollection is an interface that ConcurrentStack and ConcurrentQueue implement.
// ConcurrentCollection requires that structures posses pointers to 'head' and 'tail' elements.
type ConcurrentCollection interface {
	Head()	*unsafe.Pointer
	Tail()	*unsafe.Pointer
}

// ConcurrentStack is a collection that can be concurrently accessed.
// Elements are accessed in FILO order.
// ConcurrentStack stores the top of the stack.
type ConcurrentStack struct {
	// Top is a pointer to the Node representing the top of the stack.
	Top	unsafe.Pointer
}

// MultiMap is a collection that can be concurrently accessed.
// Elements are accessed with a key of type interface{}.
// Locks are utilized to maintain atomic nature.
type MultiMap struct {
	// Wmap is a pointer to the wrapped map.
	Wmap	map[interface{}]*list.List
	// MapLock is a mutex that locks during map accessing/editing.
	MapLock	sync.Mutex
	// ExLock is a lock that can be used externally.
	ExLock	sync.Mutex
}

/*** NODE IMPLEMENTATION ***/

// A Node is constructed from an element value.
// Nxt and Prv are both initialized to nil.
func newNode(e interface{}) unsafe.Pointer {
	n := new(Node)
	n.Val = e
	n.Nxt = nil
	return unsafe.Pointer(n)
}

/*** CONCURRENTCOLLECTION IMPLEMENTATION ***/

// Push inserts a new element e of type interface{} to the tail of a ConcurrentCollection ch.
// The tail pointer is moved to point to the newest element.
func Push(ch ConcurrentCollection, e interface{}) {
	n := newNode(e)
	for swapped := false; !swapped; swapped = atomic.CompareAndSwapPointer(ch.Tail(), (*Node)(n).Nxt, n) {
		(*Node)(n).Nxt = *ch.Tail()
	}
}

// Peek returns the value of the current head of a ConcurrentCollection ch.
// Peek does not alter the ConcurrentCollection structure.
func Peek(ch ConcurrentCollection) interface{} {
	n := *ch.Head()
	if n != nil {
		return (*Node)(n).Val
	}
	return nil
}

// Pop returns the 'top' element of the stack and deletes the corresponding node.
// The stack top pointer is advanced to the next element.
func Pop(ch ConcurrentCollection) interface{} {
	n := *ch.Head()
	var x unsafe.Pointer
	if (*Node)(n) != nil {
		x = (*Node)(n).Nxt
	} else {
		x = unsafe.Pointer(nil)
	}
	for swapped := false; !swapped; swapped = atomic.CompareAndSwapPointer(ch.Head(), n, x) {
		n = *ch.Head()
		if (*Node)(n) != nil {
			x = (*Node)(n).Nxt
		} else {
			x = unsafe.Pointer(nil)
		}
	}
	if (*Node)(n) == nil {
		return nil
	}
	return (*Node)(n).Val
}

/*** STACK IMPLEMENTATION ***/

// Head returns the top of the stack.
func (s *ConcurrentStack) Head() *unsafe.Pointer { return &s.Top }

// Tail returns the top of the stack.
func (s *ConcurrentStack) Tail() *unsafe.Pointer { return &s.Top }

// NewStack constructs a new stack with no elements.
func NewStack() *ConcurrentStack {
	s := new(ConcurrentStack)
	s.Top = unsafe.Pointer(nil)
	return s
}

/*** MAP IMPLEMENTATION ***/

func NewMap() *MultiMap {
	m := new(MultiMap)
	m.Wmap = make(map[interface{}]*list.List)
	return m
}

// Contains takes as arguments values k and v.
// Contains returns true if v is a value associated with key k and false otherwise.
// The coupling parameter coup can be carefully used to atomically 'couple' several calls to different map methods.
func (m *MultiMap) Contains(k, v interface{}, coup ...bool) bool {
	if !(len(coup) > 0 && coup[0]) {
		m.MapLock.Lock()
	}
	l := m.Wmap[k]
	if l == nil {
		return false
	}
	for e := l.Front(); e != nil; e = e.Next() {
		if e.Value == v {
			if !(len(coup) > 1 && coup[1]) {
				m.MapLock.Unlock()
			}
			return true
		}
	}
	if !(len(coup) > 1 && coup[1]) {
		m.MapLock.Unlock()
	}
	return false
}

// Insert takes as arguments values k and v.
// Insert adds v as one of the values associated with k.
// The coupling parameter coup can be carefully used to atomically 'couple' several calls to different map methods.
func (m *MultiMap) Insert(k, v interface{}, coup ...bool) {
	if !(len(coup) > 0 && coup[0]) {
		m.MapLock.Lock()
	}
	if m.Wmap[k] == nil {
		m.Wmap[k] = list.New()
	}
	m.Wmap[k].PushBack(v)
	if !(len(coup) > 1 && coup[1]) {
		m.MapLock.Unlock()
	}
}

// Erase erases all associations to key k.
// The coupling parameter coup can be carefully used to atomically 'couple' several calls to different map methods.
func (m *MultiMap) Erase(k interface{}, coup ...bool) {
	if !(len(coup) > 0 && coup[0]) {
		m.MapLock.Lock()
	}
	delete(m.Wmap, k)
	if !(len(coup) > 1 && coup[1]) {
		m.MapLock.Unlock()
	}
}