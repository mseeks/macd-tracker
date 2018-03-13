package main

import (
	"fmt"
	"os"

	cluster "github.com/bsm/sarama-cluster"
)

var (
	broker        string
	consumerGroup string
	consumerTopic string
	producerTopic string
)

// Entrypoint for the program
func main() {
	broker = os.Getenv("KAFKA_ENDPOINT")
	consumerTopic = os.Getenv("KAFKA_CONSUMER_TOPIC")
	consumerGroup = os.Getenv("KAFKA_CONSUMER_GROUP")
	producerTopic = os.Getenv("KAFKA_PRODUCER_TOPIC")

	// init config
	config := cluster.NewConfig()

	// init consumer
	brokers := []string{broker}
	topics := []string{consumerTopic}

	for {
		consumer, err := cluster.NewConsumer(brokers, consumerGroup, topics, config)
		defer consumer.Close()
		if err != nil {
			fmt.Println(err)
			continue
		}

		// consume messages
		for {
			select {
			case msg, ok := <-consumer.Messages():
				if ok {
					symbol := string(msg.Key)

					watchedEquity, err := newEquity(symbol, msg.Value)
					if err != nil {
						fmt.Println("Error:", err)
						return
					}

					// Query the MACD stats for the equity
					err = watchedEquity.calculateMacd()
					if err != nil {
						fmt.Println("Error:", err)
						return
					}

					err = watchedEquity.broadcastStats()
					if err != nil {
						fmt.Println("Error:", err)
						return
					}

					consumer.MarkOffset(msg, "") // mark message as processed
				}
			}
		}
	}
}
