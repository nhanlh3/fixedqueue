package fixedqueue

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

type Queue interface {
	Len() int
	Push(e interface{}) error
	Pop() (Element, error)
	Peek() (Element, error)
	Size() uintptr
	ScanAndRemoveExpiredElement() error
	StartScan(scanPeriod time.Duration, printLog bool) error
	StartScanDefault() error
	CloseJobs()
}

type queue struct {
	maxSize       uintptr // Byte unit
	unitSize      uintptr
	unitName      string
	curSize       uintptr
	expireElement bool
	lifeTime      time.Duration
	displayPrefix string
	queue         []Element
	mu            sync.Mutex
	cron          *cron.Cron
	logScanJob    bool
	scanJobID     cron.EntryID
}

type Element struct {
	Data  interface{}
	Entry time.Time
}

const (
	defaultExpireElement bool          = false
	defaultLifeTime      time.Duration = 24 * time.Hour
	defaultMaxSize       uintptr       = 10
	defaultUnit          Unit          = MiB
	defaultScanPeriod    time.Duration = time.Hour
	defaultDisplayPrefix string        = "[fixedqueue]"
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
	DisplayPrefix   string
}

// New return a new instance of a 'fixed-size queue', the queue is thread-safe and it receive maxSize as the max number of memory allowed to allocate
//
// ***
// By default:
//
//	ExpireElement   : false
//	ElementLifeTime : 24 hours
//	MaxSize         : 10
//	Unit            : MiB
//	DisplayName     : [fixedqueue]
//
// ***
// To enabble cron job to remove element with given expire time, set ExpireElement = true
func New() Queue {
	return NewWithOptions(Options{})
}

// NewWithOptions return a new instance of a 'fixed-size queue', the queue is thread-safe and it receive maxSize as the max number of memory allowed to allocate
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
		displayPrefix string        = defaultDisplayPrefix
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
	if len(options.DisplayPrefix) > 0 {
		displayPrefix = options.DisplayPrefix
	}

	return newSliceQueue(expireElement, lifeTime, maxSize, unitSize, unitName, displayPrefix)
}

func newSliceQueue(expireElement bool, lifeTime time.Duration, maxSize uintptr, unitSize uintptr, unitName string, displayPrefix string) Queue {
	return &queue{
		maxSize:       maxSize * unitSize,
		unitSize:      unitSize,
		unitName:      unitName,
		displayPrefix: displayPrefix,
		expireElement: expireElement,
		lifeTime:      lifeTime,
		queue:         make([]Element, 0),
		cron:          cron.New(),
	}
}

func (q *queue) StartScanDefault() error {
	return q.StartScan(defaultScanPeriod, false)
}

func (q *queue) StartScan(scanPeriod time.Duration, printLog bool) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// check if scaning job existed
	if q.scanJobID > 0 {
		return nil
	}

	if !q.expireElement {
		return q.err(" queue element is set not to expire")
	}

	q.logScanJob = printLog

	jobID, err := q.cron.AddFunc(fmt.Sprintf("@every %fs", scanPeriod.Seconds()), func() {
		start := time.Now()
		q.ScanAndRemoveExpiredElement()

		if q.logScanJob {
			fmt.Println(q.msgf("[%v] cron job cleaned expired elements -- execute time: %v ms, Len: %v element(s), Size: %v B", time.Now().Format(time.RFC3339), time.Since(start).Milliseconds(), q.Len(), q.Size()))
		}
	})
	if err != nil {
		return q.err(" failed to add job cleaning expired elements:", err)
	}
	q.scanJobID = jobID
	q.cron.Start()
	return nil
}

func (q *queue) CloseJobs() {
	ctx := q.cron.Stop()
	<-ctx.Done()

	if q.logScanJob {
		fmt.Println(q.msg(" cron jobs closed gracefully"))
	}
}

func (q *queue) Len() int {
	return len(q.queue)
}

func (q *queue) Push(e interface{}) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	newElement := Element{
		Data:  e,
		Entry: time.Now(),
	}

	if (q.curSize + newElement.Size()) > q.maxSize {
		return q.errf(" queue reached limitation at %v %v", q.maxSize/q.unitSize, q.unitName)
	}
	q.queue = append(q.queue, newElement)
	q.curSize += newElement.Size()
	return nil
}

func (q *queue) Pop() (Element, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	l := len(q.queue)
	if l <= 0 {
		return Element{}, q.err(" Pop() called on empty queue")
	}

	front := q.queue[0]
	q.queue[0] = Element{}
	q.queue = q.queue[1:l]
	q.curSize -= front.Size()
	return front, nil
}

func (q *queue) Peek() (Element, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	l := len(q.queue)
	if l <= 0 {
		return Element{}, q.err(" Peek() called on empty queue")
	}
	return q.queue[0], nil
}

func (q *queue) Size() uintptr {
	return q.curSize
}

func (e *Element) Size() uintptr {
	if e == nil {
		return 0
	}
	if e.Data == nil {
		return reflect.TypeOf(*e).Size()
	}
	return reflect.TypeOf(*e).Size() + reflect.TypeOf(e.Data).Size()
}

func (q *queue) ScanAndRemoveExpiredElement() error {
	if !q.expireElement {
		return q.err(" queue element is set not to expire")
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
	return nil
}

func (q *queue) msg(a ...any) string {
	return q.displayPrefix + fmt.Sprint(a...)
}

func (q *queue) msgf(format string, a ...any) string {
	return q.displayPrefix + fmt.Sprintf(format, a...)
}

func (q *queue) err(a ...any) error {
	return errors.New(q.displayPrefix + fmt.Sprint(a...))
}

func (q *queue) errf(format string, a ...any) error {
	return errors.New(q.displayPrefix + fmt.Sprintf(format, a...))
}
