package main

import (
	"os"

	"github.com/go-redis/redis"
)

func newRedisClient() *redis.Client {
	// Create the Redis client
	return redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ENDPOINT"),
		Password: "",
		DB:       0,
	})
}
