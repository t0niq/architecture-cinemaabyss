package main

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
)

func consumeTopic(topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaAddr},
		Topic:   topic,
		GroupID: "event-consumer-group-" + topic,
	})
	
	defer reader.Close()

	log.Printf("Kafka consumer started for topic: %s", topic)

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("[%s] Error reading: %v", topic, err)
			continue
		}

		log.Printf("[%s] Key=%s Value=%s", topic, string(msg.Key), string(msg.Value))
	}
}
