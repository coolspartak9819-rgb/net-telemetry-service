package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "net-telemetry-service/proto"
)

type server struct {
	pb.UnimplementedTelemetryServiceServer
	db    *sql.DB
	rdb   *redis.Client
}

func (s *server) GetDeviceState(ctx context.Context, req *pb.DeviceRequest) (*pb.DeviceStateResponse, error) {
	deviceID := req.GetDeviceId()
	if deviceID == "" {
		return nil, status.Error(codes.InvalidArgument, "device_id is required")
	}

	fields, err := s.rdb.HGetAll(ctx, "device:"+deviceID).Result()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "redis error: %v", err)
	}
	if len(fields) == 0 {
		return nil, status.Errorf(codes.NotFound, "device %s not found in cache", deviceID)
	}

	var cpu, mem int32
	fmt.Sscanf(fields["cpu_usage"], "%d", &cpu)
	fmt.Sscanf(fields["memory_usage"], "%d", &mem)

	return &pb.DeviceStateResponse{
		DeviceId:    deviceID,
		CpuUsage:    cpu,
		MemoryUsage: mem,
		Status:      fields["status"],
		Timestamp:   fields["timestamp"],
	}, nil
}

func (s *server) GetDeviceHistory(ctx context.Context, req *pb.DeviceRequest) (*pb.DeviceHistoryResponse, error) {
	deviceID := req.GetDeviceId()
	if deviceID == "" {
		return nil, status.Error(codes.InvalidArgument, "device_id is required")
	}

	rows, err := s.db.QueryContext(ctx, 
		"SELECT device_id, cpu_usage, memory_usage, status, timestamp FROM device_telemetry_history WHERE device_id = $1 ORDER BY timestamp DESC LIMIT 10", 
		deviceID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "postgres error: %v", err)
	}
	defer rows.Close()

	var history []*pb.DeviceStateResponse
	for rows.Next() {
		var rID, cpuStr, memStr, stat string
		var t time.Time
		if err := rows.Scan(&rID, &cpuStr, &memStr, &stat, &t); err != nil {
			return nil, status.Errorf(codes.Internal, "scan error: %v", err)
		}

		var cpu, mem int32
		fmt.Sscanf(cpuStr, "%d", &cpu)
		fmt.Sscanf(memStr, "%d", &mem)

		history = append(history, &pb.DeviceStateResponse{
			DeviceId:    rID,
			CpuUsage:    cpu,
			MemoryUsage: mem,
			Status:      stat,
			Timestamp:   t.Format(time.RFC3339),
		})
	}

	return &pb.DeviceHistoryResponse{
		DeviceId: deviceID,
		History:  history,
	}, nil
}

func main() {
	log.Println("[Main API] Инициализация gRPC API Сервиса...")

	dbURL := os.Getenv("DATABASE_URL")
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Ошибка открытия БД: %v", err)
	}
	defer db.Close()

	redisAddr := os.Getenv("REDIS_ADDR")
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterTelemetryServiceServer(s, &server{db: db, rdb: rdb})

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan
		log.Println("[Main API] Остановка gRPC сервера...")
		s.GracefulStop()
	}()

	log.Println("[API] gRPC сервер успешно запущен на порту :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
