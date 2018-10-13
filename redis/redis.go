package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/armon/relay"
	"github.com/armon/relay/broker"
	"github.com/go-redis/redis"
	"github.com/hashicorp/go-uuid"
)

// RedisBroker implements the Broker interface in-memory
type RedisBroker struct {
	Prefix string
	ID     string

	closed bool

	counter *uint64
	client  *redis.Client

	wg     sync.WaitGroup
	ctx    context.Context
	cancel func()
}

// RedisConsumer implements the Consumer interface
type RedisConsumer struct {
	broker  *RedisBroker
	queue   string
	closed  bool
	needAck bool

	lock      sync.RWMutex
	tempQueue string
}

// RedisConsumer implements the Publisher interface
type RedisPublisher struct {
	broker *RedisBroker
	queue  string
	closed bool
}

type Options struct {
	Addr     string
	Password string
	DB       int
	Prefix   string
}

func NewRedisBroker(opts Options) *RedisBroker {
	var ropts redis.Options

	ropts.Addr = opts.Addr
	ropts.Password = opts.Password
	ropts.DB = opts.DB

	ctx, cancel := context.WithCancel(context.Background())

	id, err := uuid.GenerateUUID()
	if err != nil {
		panic(err)
	}

	rb := &RedisBroker{
		Prefix: opts.Prefix,
		ID:     id,

		counter: new(uint64),
		client:  redis.NewClient(&ropts),
		ctx:     ctx,
		cancel:  cancel,
	}

	rb.wg.Add(2)
	go rb.heartbeat()
	go rb.autoReclaimDeadClients()

	return rb
}

func (i *RedisBroker) Close() error {
	if i.closed {
		return nil
	}

	i.closed = true

	i.cancel()

	i.wg.Wait()

	return i.client.Close()
}

func (i *RedisBroker) listName(q string) string {
	return fmt.Sprintf("%s:%s", i.Prefix, q)
}

func (i *RedisBroker) tempClientPrefix(id string) string {
	// This formatting is used to avoid collisions with queue names
	return fmt.Sprintf("%s!tmp:%s", i.Prefix, id)
}

func (i *RedisBroker) tempListName(q string) string {
	return fmt.Sprintf("%s:%s", i.tempClientPrefix(i.ID), q)
}

func (i *RedisBroker) Consumer(q string) (broker.Consumer, error) {
	c := &RedisConsumer{
		broker:    i,
		queue:     i.listName(q),
		tempQueue: fmt.Sprintf("%s:%d", i.tempListName(q), atomic.AddUint64(i.counter, 1)),
	}
	return c, nil
}

func (i *RedisBroker) Publisher(q string) (broker.Publisher, error) {
	p := &RedisPublisher{
		broker: i,
		queue:  i.listName(q),
	}
	return p, nil
}

func (i *RedisPublisher) Close() error {
	i.closed = true
	return nil
}

func (i *RedisPublisher) Publish(in interface{}) error {
	data, err := json.Marshal(in)
	if err != nil {
		return err
	}

	return i.broker.client.LPush(i.queue, string(data)).Err()
}

func (i *RedisConsumer) Close() error {
	if i.needAck {
		i.Nack()
	}
	i.closed = true
	return nil
}

func (i *RedisConsumer) Consume(out interface{}) error {
	return i.ConsumeTimeout(out, 0)
}

func (i *RedisConsumer) ConsumeAck(out interface{}) error {
	err := i.ConsumeTimeout(out, 0)
	if err == nil {
		return i.Ack()
	}
	return err
}

var ErrNeedAck = errors.New("need to ack")

type timeoutI interface {
	Timeout() bool
}

func (i *RedisConsumer) ConsumeTimeout(out interface{}, timeout time.Duration) error {
	if i.needAck {
		return ErrNeedAck
	}

	data, err := i.broker.client.BRPopLPush(i.queue, i.tempQueue, timeout).Bytes()
	if err != nil {
		if to, ok := err.(timeoutI); ok {
			if to.Timeout() {
				return relay.TimedOut
			}
		}

		if err == redis.Nil {
			return relay.TimedOut
		}

		return err
	}

	err = json.Unmarshal(data, out)
	if err != nil {
		return err
	}

	i.lock.Lock()
	defer i.lock.Unlock()

	// Set that we need ack
	i.needAck = true

	return nil
}

func (i *RedisConsumer) Ack() error {
	if !i.needAck {
		return nil
	}

	i.lock.Lock()
	defer i.lock.Unlock()

	err := i.broker.client.LPop(i.tempQueue).Err()
	if err != nil {
		return err
	}

	i.needAck = false
	return nil
}

func (i *RedisConsumer) Nack() error {
	if !i.needAck {
		return nil
	}

	i.lock.Lock()
	defer i.lock.Unlock()

	data, err := i.broker.client.LPop(i.tempQueue).Bytes()
	if err != nil {
		return err
	}

	err = i.broker.client.RPush(i.queue, data).Err()
	if err != nil {
		return err
	}

	i.needAck = false
	return nil
}
