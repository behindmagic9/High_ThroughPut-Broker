package broker

import (
	"observer/event"
	"observer/subscriber"
	"testing"
)
func BenchmarkQueuePressure(b *testing.B) {
    br, _ := NewBroker()

    sub := subscriber.NewBenchmarkSubscriber(1)

    br.Subscribe("BTC", sub)
    br.Start()

    b.ResetTimer()

    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            br.Notify(event.NewEvent("BTC", "msg", 100))
        }
    })

    b.StopTimer()

    metrics := br.GetMetrics()

    b.Logf(
        "Published=%d Delivered=%d Dropped=%d",
        metrics.Published.Load(),
        metrics.Delivered.Load(),
        metrics.Dropped.Load(),
    )
}
