package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/antonholmquist/jason"
	"github.com/jasonlvhit/gocron"
	"github.com/segmentio/kafka-go"
	"gopkg.in/resty.v1"
)

// Used to represent an equity that we're watching for signal changes
type equity struct {
	symbol     string
	macd       string
	macdSignal string
}

type message struct {
	Macd       string `json:"macd"`
	MacdSignal string `json:"macd_signal"`
	At         string `json:"at"`
}

func (equity *equity) query() ([]byte, error) {
	resp, err := resty.R().
		SetQueryParams(map[string]string{
			"function":    "MACD",
			"symbol":      equity.symbol,
			"interval":    "daily",
			"series_type": "close",
			"apikey":      os.Getenv("ALPHAVANTAGE_API_KEY"),
		}).
		SetHeader("Accept", "application/json").
		Get("https://www.alphavantage.co/query")
	if err != nil {
		return []byte(""), err
	}

	if resp.StatusCode() != 200 {
		return []byte(""), fmt.Errorf("Incorrect status code: %v", resp.Status())
	}

	return resp.Body(), nil
}

func (equity *equity) track() error {
	query, err := equity.query()
	if err != nil {
		return err
	}

	value, err := jason.NewObjectFromBytes(query)
	if err != nil {
		return err
	}

	technicalAnalysis, err := value.GetObject("Technical Analysis: MACD")
	if err != nil {
		return err
	}

	var todayKeys []string
	var yesterdayKeys []string

	today := time.Now().UTC().Format("2006-01-02")
	yesterday := time.Now().AddDate(0, 0, -1).UTC().Format("2006-01-02")

	for key := range technicalAnalysis.Map() {
		if strings.Contains(key, today) {
			todayKeys = append(todayKeys, key)
		}
		if strings.Contains(key, yesterday) {
			yesterdayKeys = append(yesterdayKeys, key)
		}
	}

	key := ""

	if len(todayKeys) > 0 {
		key = todayKeys[0]
	} else if len(todayKeys) == 0 && len(yesterdayKeys) > 0 {
		key = yesterdayKeys[0]
	}

	macdString, err := value.GetString("Technical Analysis: MACD", key, "MACD")
	if err != nil {
		return err
	}

	macdSignalString, err := value.GetString("Technical Analysis: MACD", key, "MACD_Signal")
	if err != nil {
		return err
	}

	equity.macd = macdString
	equity.macdSignal = macdSignalString

	return nil
}

func (equity *equity) broadcastStats() {
	producer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{os.Getenv("KAFKA_ENDPOINT")},
		Topic:    os.Getenv("KAFKA_TOPIC"),
		Balancer: &kafka.LeastBytes{},
	})
	defer producer.Close()

	signalMessage := message{
		Macd:       equity.macd,
		MacdSignal: equity.macdSignal,
		At:         time.Now().UTC().Format("2006-01-02 15:04:05 -0700"),
	}

	jsonMessage, err := json.Marshal(signalMessage)
	if err != nil {
		panic(err)
	}

	producer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(equity.symbol),
			Value: jsonMessage,
		},
	)

	jsonMessageString := string(jsonMessage)
	fmt.Println(equity.symbol, "->", jsonMessageString)
}

func trackEquity(symbol string) {
	go func() {
		watchedEquity := equity{strings.ToUpper(symbol), "", ""}

		// Query the MACD stats for the equity
		err := watchedEquity.track()
		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		watchedEquity.broadcastStats()
	}()
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

// Entrypoint for the program
func main() {
	// Initialize a new scheduler
	scheduler := gocron.NewScheduler()

	// Get a list of equities from the environment variable
	equityEatchlist := strings.Split(os.Getenv("EQUITY_WATCHLIST"), ",")

	// Shuffle watchlist so ENV order of equities isn't a weighted factor and it's more dependent on time-based priority
	// This should only really matter if there's a case where to equities change direction during the same interval (unlikely)
	shuffle(equityEatchlist)

	// For each equity in the watchlist schedule it to be watched every 5 minutes
	for _, equitySymbol := range equityEatchlist {
		time.Sleep(5 * time.Second)
		scheduler.Every(5).Minutes().Do(trackEquity, equitySymbol)
		trackEquity(equitySymbol) // Watch the signal immediately rather than waiting until next trigger
	}

	// Start the scheduler process
	<-scheduler.Start()
}
