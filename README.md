# Sales Total Realtime Aggregation

프랜차이즈별 매출 데이터를 실시간으로 집계하는 Apache Flink 스트리밍 애플리케이션입니다. Kafka에서 Avro 형식의 영수증 데이터를 읽어와 프랜차이즈별로 일별 누적 매출을 계산하고 결과를 다시 Kafka로 전송합니다.

## 주요 기능

- Kafka의 `test-topic` 토픽에서 영수증 데이터를 실시간으로 읽어옴 (Avro 형식)
- 당일 데이터만 필터링하여 처리
- 프랜차이즈별 매장 수 및 총 매출액을 누적 집계
- 집계 결과를 `sales_total_realtime` 토픽으로 전송 (JSON 형식)
- 일별 상태 초기화 기능 제공

## 프로젝트 구조

```
sales_total_realtime/
├── src/
│   └── main/
│       ├── avro/           # Avro 스키마 정의
│       │   └── receipt.avsc
│       ├── java/
│       │   └── com/kafka/sales/
│       │       ├── functions/   # Flink 처리 함수
│       │       │   ├── DailyCumulativeSalesProcessor.java  # 일별 누적 집계 프로세서
│       │       │   ├── FranchiseKeySelector.java           # 프랜차이즈 ID 키 선택기
│       │       │   └── TodayReceiptFilter.java             # 당일 데이터 필터
│       │       ├── model/       # 데이터 모델
│       │       │   ├── ReceiptData.java                   # 입력 영수증 데이터 모델
│       │       │   └── SalesTotalData.java                # 출력 매출 집계 데이터 모델
│       │       ├── utils/       # 유틸리티 클래스
│       │       │   ├── SimpleAvroDeserializationSchema.java      # Avro 역직렬화
│       │       │   └── SalesTotalJsonSerializationSchema.java    # JSON 직렬화
│       │       └── SalesTotalRealtimeApp.java              # 메인 애플리케이션
│       └── resources/
│           ├── application.properties  # 애플리케이션 설정
│           └── logback.xml             # 로깅 설정
├── build.gradle          # Gradle 빌드 스크립트
├── gradlew / gradlew.bat # Gradle 래퍼
└── README.md
```

## 출력 데이터 형식

```json
{
  "franchise_id": 1,
  "store_brand": "CoffeeShop",
  "store_count": 25,
  "total_sales": 4500000,
  "update_time": "2025-05-21 14:30:00"
}
```

## 빌드 및 실행

### 빌드

```bash
# 실행 가능한 JAR 파일 생성 (Uber JAR)
./gradlew clean shadowJar
```

빌드된 JAR 파일은 `build/libs/sales-total-realtime.jar`에 생성됩니다.

### 로컬 실행 (Flink Standalone)

```bash
# Apache Flink가 설치되어 있어야 합니다
flink run -c com.kafka.sales.SalesTotalRealtimeApp build/libs/sales-total-realtime.jar
```

### AWS 환경 배포

Apache Flink와 Kafka가 AWS에 배포된 환경에서 실행하려면:

1. 애플리케이션 설정 조정:
    - `application.properties`에서 Kafka 브로커 주소 및 Schema Registry URL 수정

2. JAR 업로드:
    - 빌드된 JAR 파일을 AWS 환경으로 업로드

3. Flink 작업 제출:
   ```bash
   flink run -d -c com.kafka.sales.SalesTotalRealtimeApp sales-total-realtime.jar
   ```

## 설정

`src/main/resources/application.properties` 파일에서 다음 설정을 변경할 수 있습니다:

```properties
# Kafka 설정
kafka.bootstrap.servers=localhost:9092,localhost:9093,localhost:9094
kafka.source.topic=test-topic
kafka.sink.topic=sales_total_realtime
kafka.consumer.group=sales-total-group

# Schema Registry 설정
schema.registry.url=http://localhost:8081,http://localhost:8085

# Flink 설정
flink.parallelism=1
flink.checkpoint.interval=60000

# 애플리케이션 설정
app.timezone=Asia/Seoul
```

## 성능 최적화

리팩토링을 통해 다음과 같은 성능 최적화가 이루어졌습니다:

1. **로깅 최적화**:
    - 불필요한 로그 출력 감소
    - 디버그 로그는 DEBUG 모드에서만 출력되도록 수정

2. **메모리 사용량 최적화**:
    - 불필요한 객체 생성 최소화
    - 효율적인 상태 관리 적용

3. **성능 튜닝 파라미터**:
    - 병렬 처리 레벨 설정 가능
    - 체크포인트 간격 조정 가능

## 문제 해결

### 데이터 처리가 되지 않는 경우
- Kafka 브로커 주소가 올바른지 확인
- 소스 토픽(`test-topic`)에 데이터가 들어오고 있는지 확인
- 애플리케이션 로그에서 오류 확인

### 백프레셔(Backpressure) 발생 시
- 병렬 처리 수준 증가: `application.properties`에서 `flink.parallelism` 값 증가
- 체크포인트 간격 조정: `flink.checkpoint.interval` 값 증가

### Kafka 연결 문제
- 모든 Kafka 브로커 주소 지정: `kafka.bootstrap.servers`에 모든 브로커 추가
- 연결 타임아웃 설정 조정: Kafka 클라이언트 속성 `request.timeout.ms` 증가

## 라이선스

Apache License 2.0