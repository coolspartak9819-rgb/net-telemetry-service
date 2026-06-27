package service

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net-telemetry-service/internal/model"
	"time"

	"github.com/segmentio/kafka-go"
)

type Receiver struct {
	devices []string
	writer  *kafka.Writer
}

func NewReceiver(kafkaAddr string) *Receiver {
	return &Receiver{
		devices: []string{"router-spb-center-01", "router-spb-office-02", "router-spb-vokzal-03"},
		writer: &kafka.Writer{
			Addr:     kafka.TCP(kafkaAddr),
			Topic:    "telemetry",
			Balancer: &kafka.LeastBytes{}, // Балансировщик для распределения нагрузки
		},
	}
}

func (r *Receiver) Start(ctx context.Context) {
	log.Println("[Collector] Запущен сбор метрик и отправка в Kafka...")
	defer r.writer.Close()

	for {
		select {
		case <-ctx.Done():
			log.Println("[Collector] Сбор данных остановлен.")
			return
		case <-time.After(1 * time.Second):
			for _, id := range r.devices {
				metrics := model.Telemetry{
					DeviceID:  id,
					CPUUsage:  rand.Intn(40) + 60,
					Timestamp: time.Now(),
				}

				// Маршалим в JSON перед отправкой в очередь
				jsonData, err := json.Marshal(metrics)
				if err != nil {
					log.Printf("[-] Ошибка маршалинга метрик: %v", err)
					continue
				}

				// Публикуем сообщение в Kafka
				err = r.writer.WriteMessages(ctx, kafka.Message{
					Key:   []byte(id),
					Value: jsonData,
				})
				if err != nil {
					log.Printf("[-] Ошибка отправки в Kafka: %v", err)
				}
			}
		}
	}
}
