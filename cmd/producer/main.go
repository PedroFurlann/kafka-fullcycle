package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	producer := NewKafkaProducer()
	Publish("Mensagem", "teste", producer, nil)
	producer.Flush(1000)
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

func Publish(msg string, topic string, producer *kafka.Producer, key []byte) error {
	kafkaMsg := &kafka.Message{
		Value: []byte(msg),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key: key,
	}

	err := producer.Produce(kafkaMsg, nil)
	if err != nil {
		return err
	}

	return nil
}
