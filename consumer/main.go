package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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

const timeoutMs = 100

func main() {

	if len(os.Args) < 3 {
		log.Fatalf("Пример использования: %s <bootstrap-server> <group> <topic...>\n", os.Args[0])

	}

	bServer := os.Args[1]
	group := os.Args[2]
	topics := os.Args[3]

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  bServer,
		"group.id":           group,
		"session.timeout.ms": 6000,
		"enable.auto.commit": true,
		"auto.offset.reset":  "earliest",
		// "debug":              "generic,broker,cgrp",
	})

	if err != nil {
		log.Fatalf("ошибка при попытке создать консюмера: %s\n", err.Error())
	}

	fmt.Printf("создан консюмер: %v\n", c)

	if err = c.Subscribe(topics, nil); err != nil {
		log.Fatalf("невозможно подписаться на топик: %s\n", err)
	}

	run := true

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("передан сигнал %v: приложение останавливается...\n", sig)
			run = false
		default:
			ev := c.Poll(timeoutMs)
			switch e := ev.(type) {
			case *kafka.Message:
				value := Order{}
				err := json.Unmarshal(e.Value, &value)
				if err != nil {
					fmt.Printf("Ошибка десериализации: %s\n", err)

				} else {
					fmt.Printf("%% Получено сообщение в топик %s:\n%+v\n", e.TopicPartition, value)
				}
				if e.Headers != nil {
					fmt.Printf("%% Заголовки: %v\n", e.Headers)
				}

			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
			default:
				if e != nil {
					fmt.Printf("Другие события: %v\n", e)
				}
			}
		}
	}

	c.Close()

}
