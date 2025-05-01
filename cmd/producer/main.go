package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliverChan := make(chan kafka.Event)
	producer := NewKafkaProducer()
	Publish("Mensagem", "teste", producer, nil, deliverChan)

	event := <-deliverChan
	msg := event.(*kafka.Message)
	if msg.TopicPartition.Error != nil {
		fmt.Println("Erro ao enviar mensagem" + msg.TopicPartition.Error.Error())
	} else {
		fmt.Println("Mensagem enviada com sucesso para o tópico", msg.TopicPartition)
	}

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

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	kafkaMsg := &kafka.Message{
		Value: []byte(msg),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key: key,
	}

	err := producer.Produce(kafkaMsg, deliveryChan)
	if err != nil {
		return err
	}

	return nil
}
