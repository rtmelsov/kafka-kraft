package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/jsonschema"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/redis/go-redis/v9"

	"consumer/models"
)

const timeoutMs = 100

func HandleSingleMsg(ctx context.Context, topic string, deserializer *jsonschema.Deserializer, client *redis.Client, c *kafka.Consumer) {
	ev := c.Poll(timeoutMs)
	switch msg := ev.(type) {
	case *kafka.Message:
		fmt.Printf("Raw payload: %q\n", msg.Value)
		value := models.Order{}
		err := deserializer.DeserializeInto(topic, msg.Value, &value)
		if err != nil {
			fmt.Printf("Ошибка десериализации: %s\n", err)
		}
		text, err := json.Marshal(value)
		fmt.Println("value", string(text))
		if err != nil {
			fmt.Printf("Ошибка десериализации: %s\n", err)

		} else {
			fmt.Printf("%% Получено сообщение в топик %s:\n%+v\n", msg.TopicPartition, value)
			ok, err := client.SetNX(ctx, fmt.Sprintf("%s-%s", "single-", value.OrderID), text, time.Hour*24).Result()
			if err != nil {
				fmt.Printf("Ошибка записи клиента в redis: %s\n", err)
				return
			}
			if ok {
				fmt.Printf("Прочитали сообщение: %s\r\n", msg.Value)
			} else {
				fmt.Printf("Такие данные уже есть в базе\r\n")
			}
		}
		if msg.Headers != nil {
			fmt.Printf("%% Заголовки: %v\n", msg.Headers)
		}

	case kafka.Error:
		fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", msg.Code(), msg)
	default:
		if msg != nil {
			fmt.Printf("Другие события: %v\n", msg)
		}
	}

}
