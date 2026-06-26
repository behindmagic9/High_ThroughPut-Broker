package broker

import (
	"context"
	//"hash/fnv"
	"observer/deliverystatus"
	"observer/event"
	"observer/isubscriber"
	//"runtime"
	"sync"
	"sync/atomic"
)

type SubcriberMap map[string][]isubscriber.Isubscriber

type Shard struct {
	queue       chan *deliverystatus.DeliveryTracker
	subscribers atomic.Pointer[SubcriberMap]
}

type Broker struct {
	shards      []Shard
	Metrics     deliverystatus.Metrics
	closed      atomic.Bool
	wg          sync.WaitGroup
	closeOnce   sync.Once
	startOnce   sync.Once
	ctx         context.Context
	cancel      context.CancelFunc
	trackerPool sync.Pool
}

// why we use struct{} cause its j:ust zero memory allocation and can be use to pass the signal only , that we needed right now

// var is not used inside the struct

var MAX_RETRY int
var MAX_QUEUE_SIZE int

var WORKERS_PER_THREAD int

var QUEUE_COUNT int

var SHARD_COUNT int

var BATCH_SIZE int

func NewBroker(maxqueuesize int,workerperthread int,queuecount int,shardcount int, batchsize int) (*Broker, error) {
	MAX_QUEUE_SIZE = maxqueuesize
	WORKERS_PER_THREAD = workerperthread
	QUEUE_COUNT = queuecount
	SHARD_COUNT = shardcount
	BATCH_SIZE = batchsize
	shrds := make([]Shard, SHARD_COUNT)
	for i := 0; i < SHARD_COUNT; i++ {
		m := make(SubcriberMap)
		var p atomic.Pointer[SubcriberMap]
		p.Store(&m)
		shrds[i] = Shard{
			queue:       make(chan *deliverystatus.DeliveryTracker, MAX_QUEUE_SIZE),
			subscribers: p,
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Broker{
		shards: shrds,
		closed: atomic.Bool{},
		ctx:    ctx,
		cancel: cancel,
		trackerPool: sync.Pool{
			New: func() interface{} {
				return &deliverystatus.DeliveryTracker{}
			},
		},
	}, nil
}

func (s *Broker) Subscribe(topic string, obs isubscriber.Isubscriber) {
	// find the hash and related shard  fmr shareds
	shrd := &s.shards[s.route(topic)]
	oldMap := shrd.subscribers.Load()
	newMap := make(SubcriberMap)
	for k, v := range *oldMap { // copy from old Map
		newMap[k] = append([]isubscriber.Isubscriber(nil), v...)
	}
	newMap[topic] = append(newMap[topic], obs) // appneding ot new map
	shrd.subscribers.Store(&newMap)
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

/*
func fnvHash(topic string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(topic))
	return h.Sum32()
}
*/

func (s *Broker) route(topic string) int {
	// retrun the hasjh in int
	// make that % modulo bound to QUEUE_COUNT (hash % QUEUE_COUNT)
	var h uint32 = 2166136261
	for i := 0; i < len(topic); i++ {
		h ^= uint32(topic[i])
		h *= 16777619
	}
	return int(h % uint32(SHARD_COUNT))
	//h := fnvHash(topic)
	//return int(h % uint32(QUEUE_COUNT))
}

func (s *Broker) Notify(data *event.Event) {
	if s.closed.Load() { // if close then return , no more publishing
		return
	}
	if data == nil { // for no null event
		return
	}

	topic := data.Topic
	hashh := s.route(topic)
	m := s.shards[hashh].subscribers.Load()
	subs,ok := (*m)[topic]
	if !ok{
		return
	}
	for _, sub := range subs {
		tk := s.encapsulate(data, sub)
		select {
		case <-s.ctx.Done():
			s.releaseTracker(tk)
			return
		case s.shards[hashh].queue <- tk:
			s.Metrics.Published.Add(1)
		default:
			s.Metrics.Dropped.Add(1)
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

		for shard := 0; shard < SHARD_COUNT; shard++ {
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
	shad := &s.shards[shard]
	batch := make([]*deliverystatus.DeliveryTracker, 0, BATCH_SIZE)

	for {
		first, ok := <-shad.queue
		if !ok {
			return // reading from a closed cahnnel will return niull which can cause panic in evalute events
		}
		batch = append(batch, first)
		draining := true
		for draining && len(batch) < BATCH_SIZE {
			select {
			case ev, ok := <-shad.queue:
				if !ok {
					draining = false
					continue
				}
				batch = append(batch, ev)
			default:
				draining = false
			}
		}

		for i := range batch {
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
		s.Metrics.Failed.Add(1) // increment faied here
		first.Status = deliverystatus.Failed
		s.releaseTracker(first)
		return
	}
	first.Status = deliverystatus.Delivered
	s.Metrics.Delivered.Add(1) // increment delivered here
	s.releaseTracker(first)
}

func (s *Broker) Unsubscribe(topic string, subb isubscriber.Isubscriber) {
	hashh := s.route(topic)
	shard := &s.shards[hashh]
	oldMap := shard.subscribers.Load()
	newMap := make(SubcriberMap)
	for k, v := range *oldMap {
		newMap[k] = append([]isubscriber.Isubscriber(nil), v...)
	}
	subs := newMap[topic]
	for i, sb := range subs {
		if sb.GetID() == subb.GetID() {
			newMap[topic] = append(subs[:i], subs[i+1:]...)
			// appening/joining.. the observers underlying array froms start to previous elment of i and then next element of i to last
			break
		}
	}
	shard.subscribers.Store(&newMap)
}

func (s *Broker) GetMetrics() *deliverystatus.Metrics {
	return &s.Metrics
}

func (s *Broker) closeChannel() {
	for i := 0; i < SHARD_COUNT; i++ {
		close(s.shards[i].queue)
	}
}

func (s *Broker) Close() {
	// mutex lock so that no two thing can lock it simuntanoeusly
	s.closeOnce.Do(func() {

		// defer s.mu.Unlock() // can put the defer here cause now the worker will be waiting for the read lock(Rlock) there in evaluate_events and close will wait for workers to Done
		// instead have to release lock self
		s.closed.Store(true)
		s.closeChannel()
		// will wait for 	// will wait for the this write and will release the lock now the this write and will release the lock now
		//  === Rule of this is  -> never hold mutex while calling Wait() ====
		s.wg.Wait()
		s.cancel()
	})
}
