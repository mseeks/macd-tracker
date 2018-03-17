package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/DannyBen/quandl"
	"github.com/Shopify/sarama"
	ema "github.com/erdelmaero/go-ema"
	"github.com/shopspring/decimal"
	timezone "github.com/tkuchiki/go-timezone"
)

// The formatter for passing messages into Kafka
type message struct {
	Macd       string `json:"macd"`
	MacdSignal string `json:"macd_signal"`
	At         string `json:"at"`
}

// Used to represent an equity that we're watching for signal changes
type equity struct {
	symbol      string
	latestQuote float64
	macd        float64
	macdSignal  float64
	historicals []float64 // last values are latest dates
	at          string
}

type quoteMessage struct {
	Quote string `json:"quote"`
	At    string `json:"at"`
}

// Initializer method for creating a new equity object
func newEquity(symbol string, latestQuote []byte) (equity, error) {
	message := quoteMessage{}
	json.Unmarshal(latestQuote, &message)
	quote, err := strconv.ParseFloat(message.Quote, 64)
	if err != nil {
		return equity{}, err
	}

	return equity{
		symbol:      strings.ToUpper(symbol),
		latestQuote: quote,
		macd:        0.0,
		macdSignal:  0.0,
		historicals: []float64{},
		at:          message.At,
	}, nil
}

func (equity *equity) calculateMacd() error {
	err := equity.backfillHistoricals()
	if err != nil {
		return err
	}

	long := ema.NewEma(26)
	short := ema.NewEma(12)
	for _, value := range equity.historicals[len(equity.historicals)-26:] {
		long.Add(1, value)
		short.Add(1, value)
	}

	shortPoints := short.GetPoints()
	longPoints := long.GetPoints()

	macd := []float64{}
	for i, longPoint := range longPoints {
		macd = append(macd, shortPoints[i].Ema-longPoint.Ema)
	}

	signal := ema.NewEma(9)
	for _, value := range macd {
		signal.Add(1, value)
	}

	signalPoints := signal.GetPoints()

	equity.macd = macd[len(macd)-1]
	equity.macdSignal = signalPoints[len(signalPoints)-1].Ema

	return nil
}

func (equity *equity) backfillHistoricals() error {
	now := time.Now()

	est, err := timezone.FixedTimezone(now, "America/New_York")
	if err != nil {
		return err
	}

	redisClient := newRedisClient()
	defer redisClient.Close()

	dayKey := est.Format(fmt.Sprintf("%v_close_2006_01_02", equity.symbol))
	result := redisClient.Get(dayKey)
	results := result.Val()

	if results == "" {
		var closeList []string

		quotes, err := quandl.GetSymbol(fmt.Sprintf("WIKI/%v", equity.symbol), nil)
		if err != nil {
			return err
		}

		for _, item := range quotes.Data {
			dateString := fmt.Sprintf("%v", item[0])
			closeString := fmt.Sprintf("%v", item[11])

			quoteTime, err := time.Parse("2006-01-02", dateString)
			if err != nil {
				return err
			}

			equityTime, err := time.Parse("2006-01-02 15:04:05 -0700", equity.at)
			if err != nil {
				return err
			}

			if quoteTime.Sub(equityTime) <= 0 {
				closeList = append(closeList, closeString)
			}
		}

		reverse(closeList)

		closes := strings.Join(closeList, ",")

		redisClient.Set(dayKey, closes, 0)
		results = closes
	}

	for _, result := range strings.Split(results, ",") {
		if result == "" {
			continue
		}

		decimal, err := decimal.NewFromString(result)
		if err != nil {
			return err
		}

		float, _ := decimal.Float64()

		equity.historicals = append(equity.historicals, float)
	}

	equity.historicals = append(equity.historicals, equity.latestQuote)

	return nil
}

func (equity *equity) generateMessage() []byte {
	signalMessage := message{
		Macd:       decimal.NewFromFloat(equity.macd).Round(2).String(),
		MacdSignal: decimal.NewFromFloat(equity.macdSignal).Round(2).String(),
		At:         equity.at,
	}

	jsonMessage, err := json.Marshal(signalMessage)
	if err != nil {
		panic(err)
	}

	jsonMessageString := string(jsonMessage)
	fmt.Println(equity.symbol, jsonMessageString)

	return jsonMessage
}

func (equity *equity) broadcastStats() {
	signalMessage := equity.generateMessage()

	producer.Input() <- &sarama.ProducerMessage{
		Topic: producerTopic,
		Key:   sarama.StringEncoder(equity.symbol),
		Value: sarama.StringEncoder(signalMessage),
	}
}
