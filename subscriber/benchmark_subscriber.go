package subscriber

import "observer/event"

type BenchmarkSubscriber struct {
	ID int64
}

func NewBenchmarkSubscriber(id int64) *BenchmarkSubscriber {
	return &BenchmarkSubscriber{
		ID: id,
	}
}

func (s *BenchmarkSubscriber) Update(e *event.Event) error {
	return nil
}

func (s *BenchmarkSubscriber) GetID() int64 {
	return s.ID
}
