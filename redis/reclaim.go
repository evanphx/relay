package redis

import (
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

func (b *RedisBroker) Reclaim(q string) (int, error) {
	lists, err := b.client.Keys(b.tempListName(q) + "*").Result()
	if err != nil {
		return 0, err
	}

	var cnt int

	for _, list := range lists {
		for {
			val, err := b.client.RPopLPush(list, b.listName(q)).Bytes()
			if err != nil {
				if err == redis.Nil {
					break
				}

				return cnt, err
			}

			if val == nil {
				break
			}

			cnt++
		}
	}

	return cnt, nil
}

func (b *RedisBroker) queueFromTemp(tmp string) string {
	prefix := len(b.tempListName(""))

	dash := strings.LastIndexByte(tmp, ':')
	if dash == -1 {
		return ""
	}

	return tmp[prefix:dash]
}

func (b *RedisBroker) ReclaimFromDead() (int, error) {
	clients, err := b.findDeadClients()
	if err != nil {
		return 0, err
	}

	var cnt int

	for _, client := range clients {
		lists, err := b.client.Keys(b.tempClientPrefix(client) + "*").Result()
		if err != nil {
			return 0, err
		}

		for _, list := range lists {
			for {
				q := b.queueFromTemp(list)
				if q == "" {
					continue
				}

				val, err := b.client.RPopLPush(list, b.listName(q)).Bytes()
				if err != nil {
					if err == redis.Nil {
						break
					}

					return cnt, err
				}

				if val == nil {
					break
				}

				cnt++
			}
		}
	}

	return cnt, nil
}

var (
	// How often to update the timestamp
	heartbeatInterval = 10 * time.Second

	// Give each client many intervals to have missed before
	// we decide that it is actually dead.
	deadDuration = time.Minute

	// How often to check for any dead clients.
	deadCheckInterval = time.Minute

	// How often to resync the time offset with redis, expreesed
	// in the number of dead check intervals to reset it.
	resyncInterval = 60
)

func (rb *RedisBroker) heartbeat() {
	defer rb.wg.Done()

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	rb.updateHeartbeat()

	for {
		select {
		case <-rb.ctx.Done():
			return
		case <-ticker.C:
			rb.updateHeartbeat()
		}
	}
}

func (rb *RedisBroker) currentTime() int64 {
	if !rb.setTimeOffset {
		st, err := rb.client.Time().Result()
		if err != nil {
			return time.Now().UnixNano()
		}

		rb.setTimeOffset = true
		rb.timeOffset = st.Sub(time.Now())
	}

	return time.Now().Add(rb.timeOffset).UnixNano()
}

func (rb *RedisBroker) updateHeartbeat() error {
	return rb.client.HSet(rb.Prefix+"!clients", rb.ID, strconv.FormatInt(rb.currentTime(), 10)).Err()
}

func (rb *RedisBroker) findDeadClients() ([]string, error) {
	clients, err := rb.client.HGetAll(rb.Prefix + "!clients").Result()
	if err != nil {
		return nil, err
	}

	var dead []string

	for k, v := range clients {
		ts, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			dead = append(dead, k)
		} else {
			t := time.Unix(0, ts)
			if time.Since(t) >= deadDuration {
				dead = append(dead, k)
			}
		}
	}

	return dead, nil
}

func (rb *RedisBroker) autoReclaimDeadClients() {
	defer rb.wg.Done()

	ticker := time.NewTicker(deadCheckInterval)
	defer ticker.Stop()

	var timeSync int

	for {
		select {
		case <-rb.ctx.Done():
			return
		case <-ticker.C:
			if timeSync == resyncInterval {
				rb.setTimeOffset = false
				timeSync = 0
			} else {
				timeSync++
			}

			set, err := rb.client.SetNX(rb.Prefix+"!lock", rb.ID, time.Minute).Result()
			if err != nil {
				log.Printf("[ERR] Unable to get cleanup lock: %s", err)
			} else if set {
				_, err := rb.ReclaimFromDead()
				if err != nil {
					log.Printf("[ERR] Error reclaiming jobs from dead clients: %s", err)
				}

				err = rb.client.Del(rb.Prefix + "!lock").Err()
				if err != nil {
					log.Printf("[ERR] Error deleting cleanup lock: %s", err)
				}
			}
		}
	}
}
