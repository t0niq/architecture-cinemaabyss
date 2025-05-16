package main

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
)

var kafkaAddr = getKafkaAddr()

func getKafkaAddr() string {
	addr := os.Getenv("KAFKA_BROKERS")
	if addr == "" {
		log.Println("⚠️ KAFKA_BROKERS not set, defaulting to localhost:9092")
		return "localhost:9092"
	}
	return addr
}

func produceToKafka(topic string, event Event) (int, int64, error) {
	writer := kafka.Writer{
		Addr:     kafka.TCP(kafkaAddr),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	msgBytes, err := json.Marshal(event)
	if err != nil {
		return 0, 0, err
	}

	msg := kafka.Message{
		Key:   []byte(event.Type),
		Value: msgBytes,
	}

	err = writer.WriteMessages(context.Background(), msg)
	if err != nil {
		return 0, 0, err
	}

	return 0, 0, nil
}
