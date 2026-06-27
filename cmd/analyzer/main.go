package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

type TelemetryMessage struct {
	DeviceID    string `json:"device_id"`
	CPUUsage    int    `json:"cpu_usage"`
	MemoryUsage int    `json:"memory_usage"`
	Status      string `json:"status"`
	Timestamp   string `json:"timestamp"`
}

var (
	processedMetrics = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "telemetry_processed_metrics_total",
			Help: "Total number of processed telemetry metrics",
		},
		[]string{"device_id", "status"},
	)

	alertsGenerated = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "telemetry_alerts_total",
			Help: "Total number of generated optimization alerts",
		},
	)
)

func init() {
	prometheus.MustRegister(processedMetrics)
	prometheus.MustRegister(alertsGenerated)
}

func main() {
	log.Println("[Main Analyzer] Старт сервиса анализатора...")

	// Запуск сервера метрик Prometheus
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("[Metrics] Сервер метрик Prometheus запущен на порту :2112/metrics")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Printf("Ошибка сервера метрик: %v", err)
		}
	}()

	dbURL := os.Getenv("DATABASE_URL")
	log.Println("[Analyzer] Ожидание подключения к PostgreSQL...")
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Ошибка подключения к БД: %v", err)
	}
	defer db.Close()

	// Простая проверка связи с БД
	for i := 0; i < 10; i++ {
		if err = db.Ping(); err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("Не удалось подключиться к PostgreSQL: %v", err)
	}
	log.Println("[Analyzer] Успешное подключение к PostgreSQL")

	kafkaURL := os.Getenv("KAFKA_URL")
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaURL},
		Topic:    "telemetry",
		GroupID:  "analyzer-group",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	defer r.Close()

	log.Println("[Analyzer] Ожидание метрик из Apache Kafka...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("[Analyzer] Остановка анализатора...")
		cancel()
	}()

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("Ошибка чтения из Kafka: %v", err)
			continue
		}

		var msg TelemetryMessage
		if err := json.Unmarshal(m.Value, &msg); err != nil {
			log.Printf("Ошибка парсинга JSON: %v", err)
			continue
		}

		// Запись в историю БД
		_, err = db.ExecContext(ctx,
			"INSERT INTO device_telemetry_history (device_id, cpu_usage, memory_usage, status, timestamp) VALUES ($1, $2, $3, $4, $5)",
			msg.DeviceID, msg.CPUUsage, msg.MemoryUsage, msg.Status, time.Now())
		if err != nil {
			log.Printf("Ошибка записи в БД: %v", err)
			continue
		}

		// Инкрементируем метрику Prometheus после успешной обработки
		processedMetrics.WithLabelValues(msg.DeviceID, msg.Status).Inc()

		// Логика генерации алертов (если нагрузка высокая)
		if msg.CPUUsage > 85 || msg.MemoryUsage > 85 {
			alertsGenerated.Inc()
			log.Printf("⚠️ ALERT: Высокая нагрузка на устройстве %s! CPU: %d%%, Mem: %d%%", msg.DeviceID, msg.CPUUsage, msg.MemoryUsage)
		}
	}
}
