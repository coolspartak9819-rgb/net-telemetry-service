package config

import (
	"log"

	"github.com/caarlos0/env/v6"
)

type Config struct {
	RedisAddr string `env:"REDIS_ADDR" envDefault:"localhost:6379"`
	KafkaAddr string `env:"KAFKA_ADDR" envDefault:"localhost:9092"`
}

func New() *Config {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		log.Fatalf("Не удалось распарсить конфигурацию: %v", err)
	}
	return cfg
}
