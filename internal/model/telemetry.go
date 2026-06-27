package model

import "time"

// Telemetry описывает метрики, которые роутер шлет на сервер
type Telemetry struct {
	DeviceID  string    `json:"device_id"`
	CPUUsage  int       `json:"cpu_usage"`
	Timestamp time.Time `json:"timestamp"`
}

// Command описывает приказ, который сервер шлет обратно на роутер
type Command struct {
	DeviceID string    `json:"device_id"`
	Action   string    `json:"action"`
	Time     time.Time `json:"time"`
}
