package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/nhanlh3/fixedqueue/slicequeue"
	"github.com/robfig/cron/v3"
)

type Queue interface {
	Len() int
	Push(e interface{}) error
	Pop() error
	Peek() (slicequeue.Element, error)
	Size() uintptr
	ScanAndRemoveExpiredElement()
	StartScan(scanPeriod time.Duration, printLog bool) error
	Close()
}

type queue struct {
	*slicequeue.Queue
	cron *cron.Cron
}

const (
	defaultExpireElement bool          = false
	defaultLifeTime      time.Duration = 24 * time.Hour
	defaultMaxSize       uintptr       = 10
	defaultUnit          Unit          = MiB
	defaultScanPeriod    time.Duration = time.Hour
)

type Unit int

const (
	B Unit = iota + 1
	KiB
	MiB
	GiB
)

var sizeOfUnit = map[Unit]uintptr{
	B:   1,
	KiB: 1 << 10,
	MiB: 1 << 20,
	GiB: 1 << 30,
}

var nameOfUnit = map[Unit]string{
	B:   "B",
	KiB: "KiB",
	MiB: "MiB",
	GiB: "GiB",
}

type Options struct {
	ExpireElement   bool
	ElementLifeTime time.Duration
	MaxSize         uint
	Unit            Unit
}

// New() return a new instance of a 'fixed-size queue', the queue is thread-safe and it receive maxSize as the max number of memory allowed to allocate
//
// ***
// By default:
//
//	ExpireElement   : false
//	ElementLifeTime : 24 hours
//	MaxSize         : 10
//	Unit            : MiB
//
// ***
// To enabble cron job to remove element with given expire time, set ExpireElement = true
func New() Queue {
	return NewWithOptions(Options{})
}

// NewWithOptions() return a new instance of a 'fixed-size queue', the queue is thread-safe and it receive maxSize as the max number of memory allowed to allocate
//
// ***
// Empty fields in Options will be filled with default values as below
//
//	ExpireElement   : false
//	ElementLifeTime : 24 hours
//	MaxSize         : 10
//	Unit            : MiB
//
// ***
// To enabble cron job to remove element with given expire time, set ExpireElement = true
func NewWithOptions(options Options) Queue {
	var (
		expireElement bool          = defaultExpireElement
		lifeTime      time.Duration = defaultLifeTime
		maxSize       uintptr       = defaultMaxSize
		unitSize      uintptr       = sizeOfUnit[defaultUnit]
		unitName      string        = nameOfUnit[defaultUnit]
	)

	if options.ExpireElement && options.ElementLifeTime > 0 {
		expireElement = options.ExpireElement
		lifeTime = options.ElementLifeTime
	}
	if options.MaxSize > 0 {
		maxSize = uintptr(options.MaxSize)
	}
	if us, ok := sizeOfUnit[options.Unit]; ok {
		unitSize = us
	}
	if un, ok := nameOfUnit[options.Unit]; ok {
		unitName = un
	}

	fmt.Println(expireElement, lifeTime, maxSize, unitSize, unitName)
	return newSliceQueue(expireElement, lifeTime, maxSize, unitSize, unitName)
}

func newSliceQueue(expireElement bool, lifeTime time.Duration, maxSize uintptr, unitSize uintptr, unitName string) Queue {
	q := slicequeue.New(
		expireElement,
		lifeTime,
		maxSize,
		unitSize,
		unitName,
	)

	return &queue{
		Queue: q,
		cron:  cron.New(),
	}
}

func (q *queue) StartScan(scanPeriod time.Duration, printLog bool) error {
	if q.Queue.IsExpireElement() {
		return errors.New("queue element is set not to expire")
	}

	q.cron.AddFunc(fmt.Sprintf("@every %fs", scanPeriod.Seconds()), func() {
		if printLog {
			fmt.Printf("[%v] Running cron job...\n", time.Now().Format(time.RFC3339))
		}
		q.ScanAndRemoveExpiredElement()
		if printLog {
			fmt.Println("Queue Len:", q.Len())
			fmt.Printf("Queue Size: %v B\n", q.Size())
		}
	})
	q.cron.Start()
	return nil
}

func (q *queue) Close() {
	ctx := q.cron.Stop()
	<-ctx.Done()
}
