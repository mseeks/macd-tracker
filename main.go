package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/antonholmquist/jason"
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
		if strings.Contains(err.Error(), "API call frequency") {
			return fmt.Errorf("External API has enforced rate limiting")
		}
		return err
	}

	var days []time.Time
	dateLayout := "2006-01-02"
	dateAndTimeLayout := "2006-01-02 15:04:05"

	for key := range technicalAnalysis.Map() {
		day, e := time.Parse(dateLayout, key)
		if e != nil {
			day, e = time.Parse(dateAndTimeLayout, key)
			if e != nil {
				return err
			}
		}

		days = append(days, day)
	}

	sort.Sort(byDate(days))

	lastKey := days[len(days)-1]
	keyShort := lastKey.Format(dateLayout)
	keyLong := lastKey.Format(dateAndTimeLayout)

	macdString, err := value.GetString("Technical Analysis: MACD", keyShort, "MACD")
	if err != nil {
		macdString, err = value.GetString("Technical Analysis: MACD", keyLong, "MACD")
		if err != nil {
			return err
		}
	}

	macdSignalString, err := value.GetString("Technical Analysis: MACD", keyShort, "MACD_Signal")
	if err != nil {
		macdSignalString, err = value.GetString("Technical Analysis: MACD", keyLong, "MACD_Signal")
		if err != nil {
			return err
		}
	}

	equity.macd = macdString
	equity.macdSignal = macdSignalString

	return nil
}

func (equity *equity) broadcastStats() {
	broker := os.Getenv("KAFKA_ENDPOINT")
	topic := os.Getenv("KAFKA_PRODUCER_TOPIC")

	producer, err := sarama.NewSyncProducer([]string{broker}, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
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

	jsonMessageString := string(jsonMessage)
	fmt.Println("Sending:", equity.symbol, "->", jsonMessageString)

	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(jsonMessage), Key: sarama.StringEncoder(equity.symbol)}
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func trackEquity(symbol string) {
	watchedEquity := equity{strings.ToUpper(symbol), "", ""}

	// Query the MACD stats for the equity
	err := watchedEquity.track()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	watchedEquity.broadcastStats()
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
	// Get a list of equities from the environment variable
	equityEatchlist := strings.Split(os.Getenv("EQUITY_WATCHLIST"), ",")

	// Shuffle watchlist so ENV order of equities isn't a weighted factor and it's more dependent on time-based priority
	// This should only really matter if there's a case where to equities change direction during the same interval (unlikely)
	shuffle(equityEatchlist)

	for {
		for _, equitySymbol := range equityEatchlist {
			time.Sleep(10 * time.Second)
			trackEquity(equitySymbol)
		}
	}
}
