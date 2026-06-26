package main

import (
	"fmt"
	"log"
	"observer/broker"
	"observer/deliverystatus"
	"observer/event"
	"observer/publisher"
	"observer/subscriber"
	"sync"
	"runtime"
	"time"
)

func PrintMetrics(metrics *deliverystatus.Metrics) {
	fmt.Printf("Published %d \n", metrics.Published.Load())
	fmt.Printf("Delivered %d \n", metrics.Delivered.Load())
	fmt.Printf("Failed %d \n", metrics.Failed.Load())
	fmt.Printf("Dropped %d \n", metrics.Dropped.Load())
}

func main() {

	var wg sync.WaitGroup

	sub1 := subscriber.NewSub("sub1")
	sub2 := subscriber.NewSub("sub2")

	// Configure Broker
	brkr, err := broker.NewBroker(
		10000, // queue size
		1, // worker per thread
		runtime.NumCPU(), // Queue Count
		runtime.NumCPU(), // Shard Count
		32 // BATCH SIZE
	)
	if err != nil {
		log.Fatal(err)
		return
	}

	brkr.Subscribe("BTC", sub1)
	brkr.Subscribe("BTC", sub2)
	brkr.Subscribe("ETH", sub2)
	brkr.Subscribe("ETH", sub1)
	brkr.Subscribe("SOL", sub1)
	brkr.Subscribe("SOL", sub2)
	brkr.Subscribe("BNB", sub2)
	brkr.Subscribe("BNB", sub1)
	brkr.Subscribe("ADA", sub2)
	brkr.Subscribe("ADA", sub1)
	brkr.Subscribe("XRP", sub2)
	brkr.Subscribe("XRP", sub1)

	publisher1 := publisher.NewPublisher("pub1", brkr)

	publisher2 := publisher.NewPublisher("pub2", brkr)

	wg.Add(1)

	brkr.Start() // cant let the start to be run in diff groutine for now cause that be difficult to debug rn
	start := time.Now()
	go func() {
		defer wg.Done()
		for i := 0; i < 100000; i++ {
			publisher1.Publish(event.NewEvent("BTC", "UP", 21))
			publisher2.Publish(event.NewEvent("ETH", "UP", 23))
			publisher2.Publish(event.NewEvent("ADA", "DOWN", 23))
			publisher2.Publish(event.NewEvent("XRP", "DOWN ", 23))
			publisher2.Publish(event.NewEvent("BNB", "DOWN", 23))
			publisher2.Publish(event.NewEvent("SOL", "UP", 23))
		}
	}()

	wg.Wait()
	elapsed := time.Since(start)
	brkr.Close()

	fmt.Println(elapsed)
	metrics := brkr.GetMetrics()
	PrintMetrics(metrics)
	rate := float64(metrics.Delivered.Load()) / elapsed.Seconds()
	fmt.Printf("Rate: %f msg/sec \n", rate)
}

// just remember to close the channel and when the channel are in blocking and nonblocking cause that can be causing the probelm
// and confirm before closing channel if any goruotine associated with them is complete can be doneusing waitgroup
// and use of mutex for locking for read and write end , so that no else can read or write when u are , other alternative for varibale is atomic\
