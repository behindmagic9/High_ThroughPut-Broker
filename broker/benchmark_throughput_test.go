package broker

import (
	"observer/event"
	"observer/subscriber"
	"testing"
)

func BenchmarkThroughput(b *testing.B) {

	br, _ := NewBroker()

	sub1 := subscriber.NewBenchmarkSubscriber(1)
	sub2 := subscriber.NewBenchmarkSubscriber(2)

	br.Subscribe("BTC", sub1)
	br.Subscribe("BTC", sub2)

	br.Start()

	ev := &event.Event{
		ID:      1,
		Topic:   "BTC",
		Message: "msg",
		Price:   100,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		br.Notify(ev)
	}
}
