package broker

import (
	"fmt"
	"observer/event"
	"observer/subscriber"
	"testing"
)

func BenchmarkMultipleTopics(b *testing.B) {

	br, _ := NewBroker()

	sub := subscriber.NewBenchmarkSubscriber(1)

	for i := 0; i < 100; i++ {
		br.Subscribe(
			fmt.Sprintf("TOPIC-%d", i),
			sub,
		)
	}

	br.Start()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {

		topic := fmt.Sprintf(
			"TOPIC-%d",
			i%100,
		)

		br.Notify(
			&event.Event{
				ID:      int64(i),
				Topic:   topic,
				Message: "msg",
				Price:   100,
			},
		)
	}
}
