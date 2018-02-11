package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/antonholmquist/jason"
	"github.com/go-redis/redis"
	"github.com/shopspring/decimal"
	"gopkg.in/resty.v1"
)

// The formatter for passing messages into Kafka
type message struct {
	Macd              string `json:"macd"`
	MacdSignal        string `json:"macd_signal"`
	MacdDecayedSignal string `json:"macd_decayed_signal"`
	At                string `json:"at"`
}

// Used to represent an equity that we're watching for signal changes
type equity struct {
	symbol            string
	macd              string
	macdSignal        string
	macdDecayedSignal string
}

// Initializer method for creating a new equity object
func newEquity(symbol string) equity {
	return equity{strings.ToUpper(symbol), "", "", ""}
}

func (equity *equity) decaySignal() {
	hasChanged, err := equity.hasChanged()
	if err != nil {
		fmt.Println(err)
		return
	}

	if hasChanged {
		equity.resetDecay()
	}

	equity.broadcastStats()
}

func (equity *equity) hasChanged() (bool, error) {
	var signal string

	macd, err := strconv.ParseFloat(equity.macd, 64)
	if err != nil {
		fmt.Println(err)
		return false, err
	}

	macdSignal, err := strconv.ParseFloat(equity.macdSignal, 64)
	if err != nil {
		fmt.Println(err)
		return false, err
	}

	if macd > macdSignal {
		signal = "BUY"
	} else {
		signal = "SELL"
	}

	hasChanged, err := equity.hasChanged()
	if err != nil {
		return false, err
	}

	if hasChanged {
		equity.resetDecay()
	}

	// Create the Redis client
	client := newRedisClient()
	defer client.Close()

	// Check if there's an existing value in Redis
	existingValue, err := client.Get(fmt.Sprint(equity.symbol, "_signal")).Result()
	if err == redis.Nil {
		fmt.Println("REDIS VALUE NOT SET, TRYING TO SET")
		// Set the value if it didn't exist already
		setErr := client.Set(fmt.Sprint(equity.symbol, "_signal"), signal, 0).Err()
		if setErr != nil {
			return false, err
		}
	} else if err != nil {
		return false, err
	} else {
		if existingValue != signal {
			// Set to the new value
			err := client.Set(fmt.Sprint(equity.symbol, "_signal"), signal, 0).Err()
			if err != nil {
				return false, err
			}

			return true, nil
		}
	}

	return false, nil
}

func (equity *equity) resetDecay() error {
	// Create the Redis client
	client := newRedisClient()
	defer client.Close()

	err := client.Set(fmt.Sprint(equity.symbol, "_decay_start"), time.Now().UTC().Format("2006-01-02 15:04:05 -0700"), 0).Err()
	if err != nil {
		return err
	}

	return nil
}

func (equity *equity) decayedSignalPeriod() int64 {
	// Create the Redis client
	client := newRedisClient()
	defer client.Close()

	decayStart, err := client.Get(fmt.Sprint(equity.symbol, "_decay_start")).Result()
	if err == redis.Nil {
		decayStart = time.Now().UTC().Format("2006-01-02 15:04:05 -0700")
	} else if err != nil {
		panic(err)
	}

	decayStartTime, err := time.Parse("2006-01-02 15:04:05 -0700", decayStart)
	if err != nil {
		fmt.Println(err)
		return 9
	}

	diff := time.Now().Sub(decayStartTime)

	decayRate := 5.0 / 7.0

	result := 9 - decimal.NewFromFloat(diff.Hours()/24.0*decayRate).Round(0).IntPart()

	if result < 2 {
		result = 2
	}

	return result
}

func (equity *equity) query(decayed bool) ([]byte, error) {
	signalWindow := int64(9)

	if decayed {
		signalWindow = equity.decayedSignalPeriod()
	}

	resp, err := resty.R().
		SetQueryParams(map[string]string{
			"function":     "MACD",
			"symbol":       equity.symbol,
			"interval":     "daily",
			"series_type":  "close",
			"signalperiod": strconv.FormatInt(signalWindow, 10),
			"apikey":       apiKey,
		}).
		SetHeader("Accept", "application/json").
		Get(apiEndpoint)
	if err != nil {
		return []byte(""), err
	}

	if resp.StatusCode() != 200 {
		return []byte(""), fmt.Errorf("Incorrect status code: %v", resp.Status())
	}

	return resp.Body(), nil
}

func (equity *equity) track() error {
	macdString, macdSignalString, err := equity.getStats(false)
	if err != nil {
		return err
	}
	_, macdDecayedSignalString, err := equity.getStats(true)
	if err != nil {
		return err
	}

	equity.macd = macdString
	equity.macdSignal = macdSignalString
	equity.macdDecayedSignal = macdDecayedSignalString

	return nil
}

func (equity *equity) getStats(decayed bool) (string, string, error) {
	query, err := equity.query(decayed)
	if err != nil {
		return "", "", err
	}

	value, err := jason.NewObjectFromBytes(query)
	if err != nil {
		return "", "", err
	}

	technicalAnalysis, err := value.GetObject("Technical Analysis: MACD")
	if err != nil {
		if strings.Contains(err.Error(), "API call frequency") {
			return "", "", fmt.Errorf("External API has enforced rate limiting")
		}
		return "", "", err
	}

	var days []time.Time
	dateLayout := "2006-01-02"
	dateAndTimeLayout := "2006-01-02 15:04:05"

	for key := range technicalAnalysis.Map() {
		day, e := time.Parse(dateLayout, key)
		if e != nil {
			day, e = time.Parse(dateAndTimeLayout, key)
			if e != nil {
				return "", "", err
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
			return "", "", err
		}
	}

	macdSignalString, err := value.GetString("Technical Analysis: MACD", keyShort, "MACD_Signal")
	if err != nil {
		macdSignalString, err = value.GetString("Technical Analysis: MACD", keyLong, "MACD_Signal")
		if err != nil {
			return "", "", err
		}
	}

	return macdString, macdSignalString, nil
}

func (equity *equity) generateMessage() []byte {
	signalMessage := message{
		Macd:              equity.macd,
		MacdSignal:        equity.macdSignal,
		MacdDecayedSignal: equity.macdDecayedSignal,
		At:                time.Now().UTC().Format("2006-01-02 15:04:05 -0700"),
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
	producer, err := newKafkaProducer()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer producer.Close()

	signalMessage := equity.generateMessage()

	message := &sarama.ProducerMessage{Topic: producerTopic, Value: sarama.StringEncoder(signalMessage), Key: sarama.StringEncoder(equity.symbol)}
	_, _, err = producer.SendMessage(message)
	if err != nil {
		fmt.Println(err)
		return
	}
}
