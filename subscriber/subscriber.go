package subscriber

import (
	//"fmt"
	//"math/rand"
	"observer/event"
	"sync/atomic"
)

var globalcount atomic.Int64

type Subscriber struct {
	name string
	ID   int64
}

func NewSub(ob_name string) *Subscriber {
	id := globalcount.Add(1)
	return &Subscriber{name: ob_name, ID: id}
}

func (s *Subscriber) Update(data *event.Event) error {
	//if rand.Intn(3) == 0 {
	//	return fmt.Errorf("simulate failure")
	//}
	//	fmt.Printf("this is the subscriber for this %s and here is the message %s price int is %d \n", data.Topic, data.Message, data.Price)
	return nil
}

func (s *Subscriber) GetID() int64 {
	return s.ID
}
