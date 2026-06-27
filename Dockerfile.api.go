FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o telemetry_api ./cmd/api/main.go

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/telemetry_api .
CMD ["./telemetry_api"]