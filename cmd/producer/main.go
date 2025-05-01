package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	fmt.Println("Hello, World!")
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka-fullcycle-kafka-1:9092",
	}

	producer, err := kafka.NewProducer(configMap)

	if err != nil {
		log.Println("Failed to create producer:", err)
		return nil
	}

	return producer
}
