package main

import (
	"math/rand"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis"
)

// Stub for sorting by date
type byDate []time.Time

func (s byDate) Len() int {
	return len(s)
}
func (s byDate) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s byDate) Less(i, j int) bool {
	return s[i].Unix() < s[j].Unix()
}

// Shuffles an array in place
func shuffle(vals []string) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for len(vals) > 0 {
		n := len(vals)
		randIndex := r.Intn(n)
		vals[n-1], vals[randIndex] = vals[randIndex], vals[n-1]
		vals = vals[:n-1]
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func newRedisClient() *redis.Client {
	// Create the Redis client
	return redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ENDPOINT"),
		Password: "",
		DB:       0,
	})
}

func newKafkaProducer() (sarama.SyncProducer, error) {
	return sarama.NewSyncProducer([]string{broker}, nil)
}
