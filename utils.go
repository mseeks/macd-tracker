package main

import (
	"os"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis"
)

type marketOpenQuery struct {
	ExtendedClosesAt interface{} `json:"extended_closes_at"`
	ExtendedOpensAt  interface{} `json:"extended_opens_at"`
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
