package resque

import (
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/szmcdull/go-resque"
	"github.com/szmcdull/go-resque/driver"
)

func init() {
	resque.Register("redis-go", &drv{})
}

type drv struct {
	client *redis.Client
	driver.Enqueuer
	schedule  map[string]struct{}
	nameSpace string
}

func (d *drv) SetClient(name string, client interface{}) {
	d.client = client.(*redis.Client)
	d.schedule = make(map[string]struct{})
	d.nameSpace = name
}

func (d *drv) ListPush(queue string, jobJSON string) (int64, error) {
	// Ensure the queue exists
	_, err := d.client.SAdd(d.nameSpace+"queues", queue).Result()
	if err != nil {
		return -1, err
	}

	listLength, err := d.client.RPush(d.nameSpace+"queue:"+queue, jobJSON).Result()
	if err != nil {
		return -1, err
	}

	return int64(listLength), err
}
func (d *drv) ListPushDelay(t time.Time, queue string, jobJSON string) (bool, error) {
	_, err := d.client.ZAdd(queue, &redis.Z{Score: float64(t.UnixNano()), Member: jobJSON}).Result()
	if err != nil {
		return false, err
	}
	if _, ok := d.schedule[queue]; !ok {
		d.schedule[queue] = struct{}{}
	}
	return true, nil
}

func (d *drv) Poll() {
	go func(d *drv) {
		for {
			for key := range d.schedule {
				now := time.Now()
				jobs, _ := d.client.ZRangeByScore(key, &redis.ZRangeBy{
					Min:    `-inf`,
					Max:    strconv.FormatInt(now.UnixNano(), 10),
					Offset: 0,
					Count:  1,
				}).Result()
				if len(jobs) == 0 {
					continue
				}
				removed, _ := d.client.ZRem(key, jobs[0]).Result()
				if removed == 0 {
					queue := strings.TrimPrefix(key, d.nameSpace)
					d.client.LPush(d.nameSpace+"queue:"+queue, jobs[0])
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}(d)
}
