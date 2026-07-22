package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/btoll/weather"
	"github.com/segmentio/kafka-go"
)

func main() {
	l := []string{
		"Paris, Texas",
	}
	forecasts := weather.GetWeather(context.Background(), l)
	b, _ := json.Marshal(forecasts)
	w := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9093", "localhost:9094", "localhost:9095"),
		Topic:        "weather-results",
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
	}
	err := w.WriteMessages(context.Background(), kafka.Message{
		Value: b,
	})
	if err != nil {
		log.Printf("Failed to write messages to topic %s: %v", w.Topic, err)
	}
}
