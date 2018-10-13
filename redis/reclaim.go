package redis

import (
	"log"
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
	heartbeatInterval = 10 * time.Second
	deadDuration      = 20 * time.Second
	deadCheckInterval = time.Minute
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

func (rb *RedisBroker) updateHeartbeat() error {
	return rb.client.HSet(rb.Prefix+"!clients", rb.ID, time.Now().Format(time.RFC3339Nano)).Err()
}

func (rb *RedisBroker) findDeadClients() ([]string, error) {
	clients, err := rb.client.HGetAll(rb.Prefix + "!clients").Result()
	if err != nil {
		return nil, err
	}

	var dead []string

	for k, v := range clients {
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			if time.Since(t) >= deadDuration {
				dead = append(dead, k)
			}
		} else {
			dead = append(dead, k)
		}
	}

	return dead, nil
}

func (rb *RedisBroker) autoReclaimDeadClients() {
	defer rb.wg.Done()

	ticker := time.NewTicker(deadCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rb.ctx.Done():
			return
		case <-ticker.C:
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
