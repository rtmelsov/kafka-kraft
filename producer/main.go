package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
	"os"
)

type Item struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

type Order struct {
	Offset     int     `json:"offset"`
	OrderID    string  `json:"order_id"`
	UserID     string  `json:"user_id"`
	Items      []Item  `json:"items"`
	TotalPrice float64 `json:"totalPrice"`
}

func main() {
	if len(os.Args) != 3 {
		log.Fatalf("Пример использования: %s <bootstrap-servers> <topic>\n", os.Args[0])
	}

	bServer := os.Args[1]
	topic := os.Args[2]

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bServer,
	})

	if err != nil {
		log.Fatalf("ошибка при попытке создать продюсера: %s\n", err.Error())
	}

	log.Printf("Продюсер создан %v\n", p)

	deliveryChan := make(chan kafka.Event)

	value := &Order{
		OrderID: "0001",
		UserID:  "00001",
		Items: []Item{
			{ProductID: "444", Quantity: 1, Price: 300},
			{ProductID: "123", Quantity: 2, Price: 500},
		},
		TotalPrice: 800.00,
	}

	payload, err := json.Marshal(value)
	if err != nil {
		log.Fatalf("ошибка при попытке сериализовать заказ: %s\n", err.Error())
	}

	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: payload,
		Headers: []kafka.Header{
			{
				Key:   "myTestHeader",
				Value: []byte("header values are bynary"),
			},
		},
	}, deliveryChan)

	e := <-deliveryChan

	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("ошибка доставки сообщения: %s\n", m.TopicPartition.Error.Error())
	}

	p.Close()

	close(deliveryChan)

}
