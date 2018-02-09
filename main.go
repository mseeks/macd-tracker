package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	apiEndpoint    string
	apiKey         string
	broker         string
	producerTopic  string
	tickerInterval time.Duration
)

// Macro function to run the tracking process
func trackEquity(symbol string) {
	watchedEquity := newEquity(symbol)

	// Query the MACD stats for the equity
	err := watchedEquity.track()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	watchedEquity.broadcastStats()
}

// Entrypoint for the program
func main() {
	apiEndpoint = "https://www.alphavantage.co/query"
	apiKey = os.Getenv("ALPHAVANTAGE_API_KEY")
	broker = os.Getenv("KAFKA_ENDPOINT")
	tickerIntervalSeconds, err := strconv.Atoi(getEnv("TICKER_INTERVAL_SECONDS", "5"))
	if err != nil {
		panic(err)
	}
	tickerInterval = time.Duration(tickerIntervalSeconds) * time.Second
	producerTopic = os.Getenv("KAFKA_PRODUCER_TOPIC")

	// Get a list of equities from the environment variable
	equityEatchlist := strings.Split(os.Getenv("EQUITY_WATCHLIST"), ",")

	// Shuffle watchlist so ENV order of equities isn't a weighted factor and it's more dependent on time-based priority
	// This should only really matter if there's a case where to equities change direction during the same interval (unlikely)
	shuffle(equityEatchlist)

	for {
		for _, equitySymbol := range equityEatchlist {
			time.Sleep(tickerInterval)
			trackEquity(equitySymbol)
		}
	}
}
