package broker

import (
	"observer/event"
	"observer/subscriber"
	"testing"
)

func BenchmarkNotify(b *testing.B) {
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

	for i := 0; i < b.N; i++ {
		br.Notify(ev)
	}
}
