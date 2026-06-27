package service

import (
	"context"
	"encoding/json"
	"log"
	"net-telemetry-service/internal/model"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type Analyzer struct {
	rdb    *redis.Client
	reader *kafka.Reader
}

func NewAnalyzer(redisAddr, kafkaAddr string) *Analyzer {
	// Подключаемся к Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	// Настраиваем чтение из Kafka
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaAddr},
		Topic:    "telemetry",
		GroupID:  "analyzer-group", // Имя группы консюмеров для масштабируемости
		MinBytes: 10e3,             // 10KB
		MaxBytes: 10e6,             // 10MB
	})

	return &Analyzer{
		rdb:    rdb,
		reader: reader,
	}
}

func (a *Analyzer) Start(ctx context.Context) {
	log.Println("[Analyzer] Ожидание метрик из Apache Kafka...")
	defer a.reader.Close()
	defer a.rdb.Close()

	for {
		// Читаем сообщение из Kafka (блокирующий вызов)
		msg, err := a.reader.ReadMessage(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				log.Println("[Analyzer] Анализ метрик остановлен.")
				return
			default:
				log.Printf("[-] Ошибка чтения из Kafka: %v", err)
				continue
			}
		}

		// Демаршалим JSON в структуру Telemetry
		var telemetry model.Telemetry
		if err := json.Unmarshal(msg.Value, &telemetry); err != nil {
			log.Printf("[-] Ошибка демаршалинга: %v", err)
			continue
		}

		// 1. Сохраняем слепок в Redis (на 10 секунд)
		a.rdb.Set(ctx, telemetry.DeviceID, msg.Value, 10*time.Second)

		// 2. Event-Driven логика: проверяем перегрузку CPU
		if telemetry.CPUUsage > 90 {
			log.Printf("⚠️  [Analyzer] Критическая нагрузка на %s: %d%%! Сгенерировано событие оптимизации.", telemetry.DeviceID, telemetry.CPUUsage)
			// В будущем здесь будет отправка команды в другой топик Kafka, а пока пишем в лог
		}
	}
}
