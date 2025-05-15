# Sales Total Realtime Aggregation

프랜차이즈별, 브랜드별 전 지점의 당일 실시간 총 판매 금액을 집계하는 Flink 애플리케이션입니다.

## 주요 기능

- Kafka의 `receipt_data` 토픽에서 영수증 데이터를 읽어옴 (Avro 포맷)
- 당일 데이터만 필터링
- 프랜차이즈별, 브랜드별로 지점 수와 총 판매 금액을 집계
- 10초 단위로 윈도우 집계 수행
- 결과를 `sales_total_realtime` 토픽으로 전송 (JSON 포맷)

## 프로젝트 구조

```
sales-total-realtime/
├── src/
│   └── main/
│       ├── avro/           # Avro 스키마 정의
│       ├── java/
│       │   └── com/kafka/sales/
│       │       ├── model/       # 데이터 모델 클래스
│       │       ├── functions/   # Flink 함수들
│       │       ├── utils/       # 유틸리티 클래스
│       │       └── SalesTotalRealtimeApp.java  # 메인 애플리케이션
│       └── resources/
│           ├── application.properties  # 설정 파일
│           └── logback.xml            # 로깅 설정
├── build.gradle
├── docker-compose.yml
├── run.bat / run.sh
└── README.md
```

## 출력 데이터 형식

```json
{
  "franchise_id": 1,
  "store_brand": "스타벅스",
  "store_count": 45,
  "total_sales": 12500000,
  "update_time": "2024-01-15 10:30:00"
}
```

## 빌드 및 실행

### 빌드

```bash
# Windows
run.bat

# Linux/Mac
./run.sh
```

### 로컬 실행

1. Kafka 토픽 생성:
```bash
kafka-topics.sh --create --topic receipt_data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
kafka-topics.sh --create --topic sales_total_realtime --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

2. Flink Job 실행:
```bash
flink run -c com.kafka.sales.SalesTotalRealtimeApp build/libs/sales-total-realtime.jar
```

### Docker Compose 실행

```bash
# 빌드 먼저 수행
./gradlew shadowJar

# Docker Compose로 실행
docker-compose up
```

## 설정

`src/main/resources/application.properties` 파일에서 설정 변경:

```properties
# Kafka 설정
kafka.bootstrap.servers=localhost:9092
kafka.source.topic=receipt_data
kafka.sink.topic=sales_total_realtime
kafka.consumer.group=sales-total-group

# Schema Registry 설정
schema.registry.url=http://localhost:8081

# Flink 설정
flink.window.size.seconds=10
flink.checkpoint.interval=60000
```

## 모니터링

- Flink Web UI: http://localhost:8081
- Job 상태, 메트릭, 백프레셔 등 확인 가능

## 주의사항

- 당일 데이터만 처리하므로 영수증의 `time` 필드가 `yyyy-MM-dd` 형식으로 시작해야 함
- 메모리 부족 시 Flink TaskManager 메모리 증가 필요
- 프랜차이즈-브랜드 조합으로 그룹화하므로 Kafka 메시지 키는 `franchiseId_brand` 형식

## 문제 해결

### 메모리 부족
```yaml
taskmanager.memory.process.size: 4gb
taskmanager.memory.flink.size: 3gb
```

### 백프레셔 발생
- 병렬 처리 수준 증가: `flink run -p 8 ...`
- 윈도우 크기 조정
- 집계 로직 최적화

### Kafka 랙 발생
- 컨슈머 병렬성 증가
- 역직렬화 최적화
- 네트워크 대역폭 확인

## 라이선스

Apache License 2.0
