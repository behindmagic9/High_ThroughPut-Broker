package event
import ( "sync/atomic")
var globalCounter atomic.Int64

type Event struct{
	ID int64
	Topic string
	Message string
	Price int
}

func NewEvent(topic string, message string, price int) *Event{
	// deleting that globalCount++ as using the atomic.Int64 now , which will increment wiht add function , in ordered way 
	return &Event{
		ID : globalCounter.Add(1),
		Topic : topic,
		Message : message,
		Price : price,
	}
}
