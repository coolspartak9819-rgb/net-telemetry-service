version: '3.8'

services:
# 1. Наш старый добрый Redis
redis:
image: redis:7-alpine
container_name: telemetry_redis
ports:
- "6379:6379"

# 2. Брокер сообщений Apache Kafka
kafka:
image: confluentinc/cp-kafka:7.5.0
container_name: telemetry_kafka
ports:
- "9092:9092"
environment:
KAFKA_NODE_ID: 1
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,CHOSEN:PLAINTEXT'
KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:29092,CHOSEN://localhost:9092'
KAFKA_OFFERS_LISTENERS: 'PLAINTEXT://0.0.0.0:29092,CHOSEN://0.0.0.0:9092'
KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
KAFKA_CONTROLLER_LISTENERS: 'CONTROLLER://0.0.0.0:29093'
KAFKA_LANGUAGE_CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'
KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:29093'
KAFKA_PROCESS_ROLES: 'broker,controller'
KAFKA_LOG_DIRS: '/tmp/kraft-combined-logs'

# 3. Первый микросервис: собирает данные и шлет в Kafka
collector:
build:
context: .
dockerfile: Dockerfile.collector
container_name: telemetry_collector
environment:
- KAFKA_ADDR=kafka:29092
depends_on:
- kafka

# 4. Второй микросервис: читает из Kafka, пишет в Redis
analyzer:
build:
context: .
dockerfile: Dockerfile.analyzer
container_name: telemetry_analyzer
environment:
- KAFKA_ADDR=kafka:29092
- REDIS_ADDR=redis:6379
depends_on:
- kafka
- redis