package broker

import (
	"fmt"
	"observer/event"
	"observer/subscriber"
	"testing"
)

func BenchmarkFanout100Subscribers(b *testing.B) {

	br, _ := NewBroker()

	for i := 0; i < 100; i++ {
		br.Subscribe(
			"BTC",
			subscriber.NewBenchmarkSubscriber(int64(i)),
		)
	}

	br.Start()

	ev := &event.Event{
		ID:      1,
		Topic:   "BTC",
		Message: fmt.Sprintf("msg"),
		Price:   100,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		br.Notify(ev)
	}
}
