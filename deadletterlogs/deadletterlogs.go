package deadletterlogs

import (
	"encoding/json"
	"fmt"
	"observer/deliverystatus"
	"observer/event"
	"os"
	"sync"
	"sync/atomic"
)

type DeadLetterStore interface {
	Save(*deliverystatus.DeliveryTracker) error
	CloseFile() error
}

type FileDeadLetter struct {
	file   *os.File
	mu     sync.Mutex
	closed atomic.Bool
}

type DeadLetterEntry struct {
	Topic        string                        `json:"topic"`
	SubscriberID int64                         `json:"subscriber_id"`
	Retry        int                           `json:"retry"`
	Status       deliverystatus.DeliveryStatus `json:"status"`
	Event        *event.Event                  `json:"event"`
}

func NewFileDeadLetterStore() (*FileDeadLetter, error) {
	err := os.MkdirAll("deadletter", 0755)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(
		"deadletter/deadletter.txt",
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return nil, err
	}

	return &FileDeadLetter{
		file: file,
	}, nil
}

func (f *FileDeadLetter) Save(e *deliverystatus.DeliveryTracker) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed.Load() {
		return nil
	}
	entry := DeadLetterEntry{
		Topic:        e.Event.Topic,
		SubscriberID: e.Subscriber.GetID(),
		Retry:        e.Retry,
		Status:       e.Status,
		Event:        e.Event,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	_, err = f.file.Write(append(data, '\n'))
	if err != nil {
		fmt.Printf("write failed of logs")
	}
	return err
}

func (f *FileDeadLetter) CloseFile() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.file == nil {
		return nil
	}
	f.closed.Store(true)
	err := f.file.Close()
	return err
}
