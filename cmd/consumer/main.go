package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka-fullcycle-kafka-1:9092",
		"client.id":         "goapp-consumer",
		"group.id":          "goapp-group-1",
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(configMap)

	if err != nil {
		fmt.Println("Falha ao criar um consumer consumer:", err.Error())
		return
	}

	topics := []string{"teste"}

	consumer.SubscribeTopics(topics, nil)

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			fmt.Println("Falha ao ler mensagem:", err.Error())
			continue
		}

		fmt.Println(
			"Mensagem recebida:",
			string(msg.Value),
			"Tópico:", msg.TopicPartition,
			"Partição:", msg.TopicPartition.Partition, "Offset:",
			msg.TopicPartition.Offset,
		)
	}
}
