package redis

import (
	"os"
	"testing"
	"time"

	"github.com/armon/relay"
	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func CheckInteg(t *testing.T) {
	if os.Getenv("INTEG_TESTS") != "true" {
		t.SkipNow()
	}
}

var prefix = "relayTest"

func wipeRedis() {
	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

	keys, err := c.Keys(prefix + "*").Result()
	if err != nil {
		panic(err)
	}

	if len(keys) == 0 {
		return
	}

	err = c.Del(keys...).Err()
	if err != nil {
		panic(err)
	}
}

func TestRedisBroker(t *testing.T) {
	CheckInteg(t)

	wipeRedis()
	defer wipeRedis()

	broker := NewRedisBroker(Options{
		Addr:   "127.0.0.1:6379",
		Prefix: prefix,
	})

	defer broker.Close()

	t.Run("can publish and consume", func(t *testing.T) {
		pub, err := broker.Publisher("test")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		err = pub.Publish("hi")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		err = pub.Publish("there")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		err = pub.Close()
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		cons, err := broker.Consumer("test")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		var out string
		err = cons.Consume(&out)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if out != "hi" {
			t.Fatalf("bad: %v", out)
		}
		err = cons.Ack()
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		err = cons.ConsumeAck(&out)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if out != "there" {
			t.Fatalf("bad: %v", out)
		}

		cons.Close()
	})

	t.Run("can consume with timeouts", func(t *testing.T) {
		pub, err := broker.Publisher("test")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		err = pub.Publish("joe")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		cons, err := broker.Consumer("test")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		var out string

		err = cons.ConsumeTimeout(&out, 5*time.Millisecond)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if out != "joe" {
			t.Fatalf("bad: %v", out)
		}

		// Push back
		err = cons.Nack()
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		// Should get it back
		err = cons.ConsumeAck(&out)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if out != "joe" {
			t.Fatalf("bad: %v", out)
		}

		// Should timeout
		err = cons.ConsumeTimeout(&out, 5*time.Millisecond)
		if err != relay.TimedOut {
			t.Fatalf("err: %v", err)
		}
	})

	t.Run("can reclaim data from missing consumers", func(t *testing.T) {
		pub, err := broker.Publisher("test")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		err = pub.Publish("joe2")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		cons, err := broker.Consumer("test")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		var out string

		err = cons.ConsumeTimeout(&out, 5*time.Millisecond)
		require.NoError(t, err)

		require.Equal(t, "joe2", out)

		// pretend that things went south

		cnt, err := broker.Reclaim("test")
		require.NoError(t, err)

		assert.Equal(t, int(1), cnt)

		cons, err = broker.Consumer("test")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		out = ""

		err = cons.ConsumeAck(&out)
		require.NoError(t, err)

		require.Equal(t, "joe2", out)
	})
}

func TestRedisCleanup(t *testing.T) {
	CheckInteg(t)

	wipeRedis()
	defer wipeRedis()

	before := deadDuration
	beforeCheck := deadCheckInterval

	defer func() {
		deadDuration = before
		deadCheckInterval = beforeCheck
	}()

	t.Run("can detect dead clients", func(t *testing.T) {
		broker := NewRedisBroker(Options{
			Addr:   "127.0.0.1:6379",
			Prefix: prefix,
		})

		defer broker.Close()

		broker.cancel()

		deadDuration = time.Second

		broker.updateHeartbeat()

		list, err := broker.findDeadClients()
		require.NoError(t, err)

		require.Equal(t, 0, len(list))

		time.Sleep(1 * time.Second)

		list, err = broker.findDeadClients()
		require.NoError(t, err)

		require.Equal(t, 1, len(list))
		assert.Equal(t, broker.ID, list[0])
	})

	t.Run("can reclaim data from missing consumers", func(t *testing.T) {
		wipeRedis()

		broker := NewRedisBroker(Options{
			Addr:   "127.0.0.1:6379",
			Prefix: prefix,
		})

		defer broker.Close()

		deadDuration = time.Second

		pub, err := broker.Publisher("test")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		err = pub.Publish("joe2")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		broker.updateHeartbeat()

		cons, err := broker.Consumer("test")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		var out string

		err = cons.ConsumeTimeout(&out, 5*time.Millisecond)
		require.NoError(t, err)

		require.Equal(t, "joe2", out)

		// pretend that things went south

		time.Sleep(time.Second)

		cnt, err := broker.ReclaimFromDead()
		require.NoError(t, err)

		assert.Equal(t, int(1), cnt)

		cons, err = broker.Consumer("test")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		out = ""

		err = cons.ConsumeTimeout(&out, time.Second)
		require.NoError(t, err)

		cons.Ack()

		require.Equal(t, "joe2", out)
	})

	t.Run("automatically cleans up dead clients", func(t *testing.T) {
		wipeRedis()

		broker := NewRedisBroker(Options{
			Addr:   "127.0.0.1:6379",
			Prefix: prefix,
		})

		defer broker.Close()

		deadDuration = time.Second
		deadCheckInterval = 2 * time.Second

		pub, err := broker.Publisher("test")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		err = pub.Publish("joe2")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		cons, err := broker.Consumer("test")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		var out string

		err = cons.ConsumeTimeout(&out, 5*time.Millisecond)
		require.NoError(t, err)

		require.Equal(t, "joe2", out)

		// pretend that things went south

		broker.cancel()

		time.Sleep(deadCheckInterval)

		b2 := NewRedisBroker(Options{
			Addr:   "127.0.0.1:6379",
			Prefix: prefix,
		})

		defer b2.Close()

		time.Sleep(time.Second)

		cons, err = broker.Consumer("test")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		out = ""

		err = cons.ConsumeTimeout(&out, time.Second)
		require.NoError(t, err)

		cons.Ack()

		require.Equal(t, "joe2", out)
	})

}
