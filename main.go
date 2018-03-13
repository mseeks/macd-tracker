package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

var (
	broker        string
	consumer      *cluster.Consumer
	consumerGroup string
	consumerTopic string
	producer      sarama.AsyncProducer
	producerTopic string
)

// Entrypoint for the program
func main() {
	broker = os.Getenv("KAFKA_ENDPOINT")
	consumerTopic = os.Getenv("KAFKA_CONSUMER_TOPIC")
	consumerGroup = os.Getenv("KAFKA_CONSUMER_GROUP")
	producerTopic = os.Getenv("KAFKA_PRODUCER_TOPIC")

	// init config
	consumerConfig := cluster.NewConfig()
	producerConfig := sarama.NewConfig()

	producerConfig.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	producerConfig.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	producerConfig.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	// init consumer
	brokers := []string{broker}
	topics := []string{consumerTopic}

	for {
		var err error

		consumer, err = cluster.NewConsumer(brokers, consumerGroup, topics, consumerConfig)
		if err != nil {
			fmt.Println(err)
			continue
		}
		defer consumer.Close()

		producer, err = sarama.NewAsyncProducer(brokers, producerConfig)
		if err != nil {
			fmt.Println(err)
			continue
		}
		defer producer.Close()

		go func() {
			for err := range consumer.Errors() {
				log.Println("Error:", err)
			}
		}()

		go func() {
			for err := range producer.Errors() {
				log.Println("Error:", err)
			}
		}()

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

					watchedEquity.broadcastStats()

					consumer.MarkOffset(msg, "") // mark message as processed
				}
			}
		}
	}
}
