package slicequeue

import (
	"fmt"
	"reflect"
	"sync"
	"time"
)

type Queue struct {
	maxSize       uintptr // Byte unit
	unitSize      uintptr
	unitName      string
	curSize       uintptr
	expireElement bool
	lifeTime      time.Duration
	queue         []Element
	mu            sync.Mutex
}

type Element struct {
	Data  interface{}
	Entry time.Time
}

func (q *Queue) Len() int {
	return len(q.queue)
}

func (q *Queue) Push(e interface{}) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	newElement := Element{
		Data:  e,
		Entry: time.Now(),
	}

	if (q.curSize + newElement.Size()) > q.maxSize {
		return fmt.Errorf("queue reached limitation at %v %v", q.maxSize/q.unitSize, q.unitName)
	}
	q.queue = append(q.queue, newElement)
	q.curSize += newElement.Size()
	return nil
}

func (q *Queue) Pop() error {
	l := len(q.queue)
	if l <= 0 {
		return fmt.Errorf("queue: Pop() called on empty queue")
	}
	q.mu.Lock()
	q.curSize -= q.queue[0].Size()
	q.queue[0] = Element{}
	q.queue = q.queue[1:l]
	q.mu.Unlock()
	return nil
}

func (q *Queue) Peek() (Element, error) {
	l := len(q.queue)
	if l <= 0 {
		return Element{}, fmt.Errorf("queue: Peek() called on empty queue")
	}
	return q.queue[0], nil
}

func (q *Queue) Size() uintptr {
	return q.curSize
}

func (e *Element) Size() uintptr {
	return reflect.TypeOf(*e).Size() + reflect.TypeOf(e.Data).Size()
}

func (q *Queue) ScanAndRemoveExpiredElement() {
	if !q.expireElement {
		return
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	var newSize uintptr = 0
	newQueue := make([]Element, 0)

	for i := 0; i < len(q.queue); i++ {
		if time.Since(q.queue[i].Entry) < q.lifeTime {
			newQueue = append(newQueue, q.queue[i])
			newSize += q.queue[i].Size()

		}
	}

	q.queue = newQueue
	q.curSize = newSize
}

func (q *Queue) IsExpireElement() bool {
	return q.expireElement
}

func New(expireElement bool, lifeTime time.Duration, maxSize uintptr, unitSize uintptr, unitName string) *Queue {
	return &Queue{
		maxSize:       maxSize * unitSize,
		unitSize:      unitSize,
		unitName:      unitName,
		expireElement: expireElement,
		lifeTime:      lifeTime,
		queue:         make([]Element, 0),
	}
}
