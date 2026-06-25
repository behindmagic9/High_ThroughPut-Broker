package deliverystatus

import (
	"observer/event"
	"observer/isubscriber"
	"sync/atomic"
)

type DeliveryStatus int

const (
	Queued DeliveryStatus = iota
	Processing
	Retrying
	DeadLetter
	Delivered
)

type DeliveryTracker struct {
	Event      *event.Event
	Subscriber isubscriber.Isubscriber
	Retry      int
	Status     DeliveryStatus
}

type Metrics struct {
	Published  atomic.Uint64
	Delivered  atomic.Uint64
	DeadLetter atomic.Uint64
	Retried    atomic.Uint64
	Dropped    atomic.Uint64
}
