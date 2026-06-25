package broker

import (
	"observer/event"
	"observer/subscriber"
	"testing"
)

func BenchmarkParallelPublish(b *testing.B) {

	br, _ := NewBroker()

	sub := subscriber.NewBenchmarkSubscriber(1)

	br.Subscribe("BTC", sub)

	br.Start()

	ev := &event.Event{
		ID:      1,
		Topic:   "BTC",
		Message: "msg",
		Price:   100,
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {

		for pb.Next() {
			br.Notify(ev)
		}

	})
}
