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
	Failed
	Dropped
	Delivered
)

type DeliveryTracker struct {
	Event      *event.Event
	Subscriber isubscriber.Isubscriber
	Status     DeliveryStatus
}

type Metrics struct {
	Published  atomic.Uint64
	Delivered  atomic.Uint64
	Failed 	   atomic.Uint64
	Dropped    atomic.Uint64
}
