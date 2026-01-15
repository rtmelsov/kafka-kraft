// Package handlers
package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/jsonschema"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/redis/go-redis/v9"

	"consumer/models"
)

func HandleMultiMsg(ctx context.Context, topic string, deserializer *jsonschema.Deserializer, client *redis.Client, c *kafka.Consumer, readedMsgCount *int) {
	msg, err := c.ReadMessage(time.Second * 5)
	if err != nil {
		return
	}
	if msg != nil {
		fmt.Printf("Raw payload: %q\n", msg.Value)
		value := models.Order{}
		err := deserializer.DeserializeInto(topic, msg.Value, &value)
		if err != nil {
			fmt.Printf("Ошибка десериализации: %s\n", err)
		}
		text, err := json.Marshal(value)
		fmt.Println("value", string(text))
		if err != nil {
			fmt.Printf("Ошибка при unmarshal заказа: %s\n", err)
		} else {
			ok, err := client.SetNX(ctx, fmt.Sprintf("%s-%s", "multi-", value.OrderID), text, time.Hour*24).Result()
			if err != nil {
				fmt.Printf("Ошибка записи клиента в redis: %s\n", err)
				*readedMsgCount++
				return
			}
			if ok {
				fmt.Printf("Прочитали сообщение номер %d, %s\r\n", *readedMsgCount, msg.Value)
			} else {
				fmt.Printf("Такие данные уже есть в базе\r\n")
			}

			*readedMsgCount++
		}
	}
	if *readedMsgCount >= 10 {
		fmt.Printf("Это было последнее сообщение, начинаем заново считать\r\n")
		_, err := c.CommitMessage(msg)
		if err != nil {
			fmt.Printf("ошибка при попытке коммита данных в кафку: %s\r\n", err.Error())
		} else {
			*readedMsgCount = 0
		}
	}
}
