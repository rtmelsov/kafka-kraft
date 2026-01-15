package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"consumer/handlers"

	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/jsonschema"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

var readedMsgCount int = 0
var ctx = context.Background()

func main() {
	time.Sleep(time.Second * 30)
	err := godotenv.Load()
	if err != nil {
		fmt.Println("Error loading .env file")
	}

	kafkaAddr := os.Getenv("KAFKA_ADDR")
	redisAddr := os.Getenv("REDIS_ADDR")
	group := os.Getenv("GROUP")
	topics := os.Getenv("TOPICS")
	schemaregisty := os.Getenv("SCHEMA_REGISTY")
	autoCommit := os.Getenv("AUTO_COMMIT")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  kafkaAddr,
		"group.id":           group,
		"session.timeout.ms": 6000,
		"enable.auto.commit": autoCommit,
		"auto.offset.reset":  "earliest",
		// "debug":              "generic,broker,cgrp",
	})

	if err != nil {
		log.Fatalf("ошибка при попытке создать консюмера: %s\n", err.Error())
	}

	fmt.Printf("создан консюмер: %s, %s, %s\n", topics, group, kafkaAddr)
	fmt.Printf("kafkaAddr = %s,redisAddr = %s, group = %s, topics = %s, schemaregisty = %s,autoCommit = %s\r\n", kafkaAddr, redisAddr, group, topics, schemaregisty, autoCommit)

	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaregisty))
	if err != nil {
		log.Fatalf("Ошибка при попытке создать клиента в schema registry")
	}

	deserializer, err := jsonschema.NewDeserializer(srClient, serde.ValueSerde, jsonschema.NewDeserializerConfig())
	if err != nil {
		log.Fatalf("Ошибка при попытке создать сериализатора JSON")
	}
	if err != nil {
		log.Fatalf("Ошибка при попытке создать сериализатора JSON")
	}

	r := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "",
		DB:       0,
	})
	_, err = r.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("ошибка при попытке соединиться к редис серверу: %s\n", err.Error())
	}

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
			if autoCommit == "true" {
				handlers.HandleSingleMsg(ctx, topics, deserializer, r, c)
			}
			if autoCommit == "false" {
				handlers.HandleMultiMsg(ctx, topics, deserializer, r, c, &readedMsgCount)
			}
		}
	}

	c.Close()

}
