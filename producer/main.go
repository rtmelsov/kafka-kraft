package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"strconv"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/jsonschema"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/joho/godotenv"
)

type Item struct {
	ProductID int     `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

type Order struct {
	Offset     int     `json:"offset"`
	OrderID    string  `json:"order_id"`
	UserID     int     `json:"user_id"`
	Items      []Item  `json:"items"`
	TotalPrice float64 `json:"total_price"`
}

var wg = sync.WaitGroup{}

const topicWaitingFactor = 60 * time.Second

func main() {
	err := godotenv.Load()
	if err != nil {
		fmt.Println("Error loading .env file")
	}

	kafkaAddr := os.Getenv("KAFKA_ADDR")
	schemaregisty := os.Getenv("SCHEMA_REGISTY")
	topic := os.Getenv("TOPIC")

	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaregisty))
	if err != nil {
		log.Fatalf("Ошибка при попытке создать клиента в schema registry")
	}

	serializer, err := jsonschema.NewSerializer(srClient, serde.ValueSerde, jsonschema.NewSerializerConfig())
	if err != nil {
		log.Fatalf("Ошибка при попытке создать сериализатора JSON")
	}

	// Cоздаём новый админский клиент.
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": kafkaAddr})
	if err != nil {
		log.Fatalf("Ошибка создания админского клиента: %s\n", err)
	}

	results, err := admin.CreateTopics(
		context.Background(),
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     5,
			ReplicationFactor: 3,
		}},
		kafka.SetAdminOperationTimeout(topicWaitingFactor),
	)
	if err != nil {
		log.Fatalf("ошибка при создании топика: %s", err.Error())
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			log.Fatalf("ошибка при создании топика: %s", result.Error.String())
		}
	}

	log.Printf("Создан топик: %s", topic)

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaAddr,
	})

	if err != nil {
		log.Fatalf("ошибка при попытке создать продюсера: %s\n", err.Error())
	}

	log.Printf("Продюсер создан %v\n", p)

	deliveryChan := make(chan kafka.Event, 100)

	go func() {
		for e := range deliveryChan {
			m := e.(*kafka.Message)
			if m.TopicPartition.Error != nil {
				fmt.Printf("ошибка доставки сообщения: %s\n", m.TopicPartition.Error.Error())
			}
			wg.Done()
		}
	}()

	for i := range 100 {
		wg.Add(1)
		fmt.Printf("order on work %d\r\n", i)
		id := strconv.Itoa(i)
		value := &Order{
			OrderID: id,
			UserID:  1,
			Items: []Item{
				{ProductID: 444, Quantity: 1, Price: 300},
				{ProductID: 123, Quantity: 2, Price: 500},
			},
			TotalPrice: 800.00,
		}

		payload, err := serializer.Serialize(topic, &value)
		if err != nil {
			log.Fatalf("ошибка при попытке сериализовать заказ: %s\n", err.Error())
		}

		msg := kafka.Message{
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
		}

		for range 5 {
			err = p.Produce(&msg, deliveryChan)
			if err == nil {
				break
			}
			time.Sleep(time.Second * 5)
		}
		if err != nil {
			wg.Done()
		}

		fmt.Printf("waiting for 5 seconds %d\r\n", i)
		time.Sleep(5 * time.Second)
	}
	fmt.Printf("out of range \r\n")

	p.Close()

	wg.Wait()
	close(deliveryChan)

}
