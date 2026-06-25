package broker

import (
	"context"
	"hash/fnv"
	"observer/deliverystatus"
	"observer/event"
	"observer/isubscriber"
	"sync"
	"runtime"
	"sync/atomic"
)

type Broker struct {
	record          map[string][]isubscriber.Isubscriber
	queues          []chan *deliverystatus.DeliveryTracker
	//	bufferQueue chan *event.Event
	metrics     deliverystatus.Metrics
	closed      atomic.Bool
	mu          sync.RWMutex
	wg          sync.WaitGroup
	closeOnce   sync.Once
	startOnce   sync.Once
	ctx         context.Context
	cancel      context.CancelFunc
	trackerPool sync.Pool
}

// why we use struct{} cause its j:ust zero memory allocation and can be use to pass the signal only , that we needed right now

// var is not used inside the struct

const MAX_RETRY int = 5
const MAX_QUEUE_SIZE = 10000
const MAX_BUFFER_SIZE = 2000

var WORKERS_PER_THREAD = runtime.NumCPU()

var QUEUE_COUNT = runtime.NumCPU()

const BATCH_SIZE = 32

func NewBroker() (*Broker, error) {
	ques := make([]chan *deliverystatus.DeliveryTracker, QUEUE_COUNT)
	for i := 0; i < QUEUE_COUNT; i++ {
		ques[i] = make(chan *deliverystatus.DeliveryTracker, MAX_QUEUE_SIZE)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Broker{
		record:          make(map[string][]isubscriber.Isubscriber),
		queues:          ques,
		closed:          atomic.Bool{},
		ctx:             ctx,
		cancel:          cancel,
		trackerPool: sync.Pool{
			New: func() interface{} {
				return &deliverystatus.DeliveryTracker{}
			},
		},
	}, nil
}

func (s *Broker) Subscribe(topic string, obs isubscriber.Isubscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.record[topic] = append(s.record[topic], obs)
}

func (s *Broker) releaseTracker(t *deliverystatus.DeliveryTracker) {
	*t = deliverystatus.DeliveryTracker{}
	s.trackerPool.Put(t)
}

func (s *Broker) encapsulate(data *event.Event, sb isubscriber.Isubscriber) *deliverystatus.DeliveryTracker {
	tk := s.trackerPool.Get().(*deliverystatus.DeliveryTracker)
	tk.Event = data
	tk.Subscriber = sb
	tk.Status = deliverystatus.Queued
	return tk
}

func fnvHash(topic string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(topic))
	return h.Sum32()
}

func (s *Broker) route(topic string) int {
	// retrun the hasjh in int
	// make that % modulo bound to QUEUE_COUNT (hash % QUEUE_COUNT)
	h := fnvHash(topic)
	return int(h % uint32(QUEUE_COUNT))
}

func (s *Broker) Notify(data *event.Event) {
	if s.closed.Load() { // if close then return , no more publishing
		return
	}
	if data == nil { // for no null event
		return
	}

	s.mu.RLock()
	subs := append([]isubscriber.Isubscriber(nil), s.record[data.Topic]...)
	s.mu.RUnlock()

	shrd := s.route(data.Topic)

	for _, sub := range subs {
		tk := s.encapsulate(data, sub)
		select {
		case <-s.ctx.Done():
			s.releaseTracker(tk)
			return
		case s.queues[shrd] <- tk:
			s.metrics.Published.Add(1)
		default:
			s.metrics.Dropped.Add(1)
			s.releaseTracker(tk)
		}
	}
}

// can give buffer option in starting confif of server
func (s *Broker) Start() {
	// just putting a check to check if cahnnel close then not call start again anyhow by mistake
	s.startOnce.Do(func() {

		if s.closed.Load() {
			return
		}

		for shard := 0; shard < QUEUE_COUNT; shard++ {
			for w := 0; w < WORKERS_PER_THREAD; w++ {
				s.wg.Add(1)
				go s.ProcessEvents(shard)
			}
		}
	})
}

func (s *Broker) ProcessEvents(shard int) {
	defer s.wg.Done()
	// cant check multiple times as on recivieng the event we only get here so after retrieivn that event , if the queue contians only one elemtn became null now so check of len greater than zero wont pass
	/*for ev := range s.queues[shard] {
		if ev == nil{
			fmt.Println("error")
		}
		s.evaluateEvents(ev)
	}
	*/

	//batching code
	queue := s.queues[shard]
	batch := make([]*deliverystatus.DeliveryTracker, 0, BATCH_SIZE)

	for {
		first, ok := <-queue
		if !ok {
			return // reading from a closed cahnnel will return niull which can cause panic in evalute events
		}
		batch = append(batch, first)
		draining := true
		for draining && len(batch) < BATCH_SIZE {
			select {
			case ev, ok := <-queue:
				if !ok {
					draining = false
					continue
				}
				batch = append(batch, ev)
			default:
				draining = false
			}
		}

		for i := 0; i < len(batch); i++ {
			s.evaluateEvents(batch[i])
		}

		batch = batch[:0]
	}

}

// without adding the recieveer param will act like independent function not like memeber function of Broker
func (s *Broker) evaluateEvents(first *deliverystatus.DeliveryTracker) {
	first.Status = deliverystatus.Processing
	err := first.Subscriber.Update(first.Event) // as the Update gonna return the err
	if err != nil {
		s.metrics.Failed.Add(1) // increment faied here
		first.Status = deliverystatus.Failed
		s.releaseTracker(first)
		return
	}
	first.Status = deliverystatus.Delivered
	s.metrics.Delivered.Add(1) // increment delivered here
	s.releaseTracker(first)
}

func (s *Broker) Unsubscribe(topic string, subb isubscriber.Isubscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	subscribers := s.record[topic]
	for i, sb := range subscribers {
		if sb.GetID() == subb.GetID() {
			s.record[topic] = append(subscribers[:i], subscribers[i+1:]...)
			// appening/joining.. the observers underlying array froms start to previous elment of i and then next element of i to last
			break
		}
	}
}

func (s *Broker) GetMetrics() *deliverystatus.Metrics {
	return &s.metrics
}

func (s *Broker) closeChannel() {
	for i := 0; i < QUEUE_COUNT; i++ {
		close(s.queues[i])
	}
}

func (s *Broker) Close() {
	// mutex lock so that no two thing can lock it simuntanoeusly
	s.closeOnce.Do(func() {

		// defer s.mu.Unlock() // can put the defer here cause now the worker will be waiting for the read lock(Rlock) there in evaluate_events and close will wait for workers to Done
		// instead have to release lock self
		s.mu.Lock()
		s.closed.Store(true)
		s.closeChannel()
		s.mu.Unlock()
		// will wait for 	// will wait for the this write and will release the lock now the this write and will release the lock now
		//  === Rule of this is  -> never hold mutex while calling Wait() ====
		s.wg.Wait()
		s.cancel()
	})
}
