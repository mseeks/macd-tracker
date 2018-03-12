package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis"
	timezone "github.com/tkuchiki/go-timezone"
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

func isExtendedMarketOpen() (bool, error) {
	var body []byte
	var query marketOpenQuery

	now := time.Now()

	est, err := timezone.FixedTimezone(now, "America/New_York")
	if err != nil {
		return false, err
	}

	redisClient := newRedisClient()
	defer redisClient.Close()

	dayKey := est.Format("XNYS_hours_2006_01_02")
	result := redisClient.Get(dayKey)
	results := result.Val()

	if results == "" {
		resp, err := http.Get(fmt.Sprintf("https://api.robinhood.com/markets/XNYS/hours/%v/", est.Format("2006-01-02")))
		if err != nil {
			return false, err
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return false, err
		}

		redisClient.Set(dayKey, string(body), 0)
	} else {
		body = []byte(results)
	}

	json.Unmarshal(body, &query)

	isOpen := false

	extendedOpen := query.ExtendedOpensAt
	extendedClose := query.ExtendedClosesAt

	if extendedOpen == nil || extendedClose == nil {
		return false, nil
	}

	opensAt, err := time.Parse("2006-01-02T15:04:05Z", extendedOpen.(string))
	if err != nil {
		return false, err
	}

	closesAt, err := time.Parse("2006-01-02T15:04:05Z", extendedClose.(string))
	if err != nil {
		return false, err
	}

	if est.Sub(opensAt) > 0 && est.Sub(closesAt) < 0 {
		isOpen = true
	}

	return isOpen, nil
}
