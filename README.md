# 🚀 Sales Total Realtime Application


Apache Flink 기반의 실시간 매출 집계 스트리밍 애플리케이션입니다.

## 📋 프로젝트 개요

### 🎯 **주요 기능**
- **실시간 매출 집계**: Kafka에서 영수증 데이터를 실시간으로 처리
- **프랜차이즈별 집계**: 프랜차이즈 단위로 누적 매출 및 매장 수 계산
- **오늘 날짜 필터링**: 당일 데이터만 선별하여 처리
- **상태 기반 처리**: Flink 상태를 이용한 실시간 누적 계산
- **장애 복구**: 체크포인트 기반 데이터 무손실 보장


### 🔄 **데이터 플로우**
```
Kafka Topic (Avro)     →     Flink Processing     →     Kafka Topic (JSON)
     ↓                             ↓                           ↓
  영수증 데이터              일일 누적 매출 집계            매출 총계 데이터
 (test-topic)              (실시간 상태 관리)        (sales_total_realtime)
```

### 🏗️ **아키텍처 구조**
```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Kafka Input   │───▶│  Flink Pipeline  │───▶│  Kafka Output   │
│                 │    │                  │    │                 │
│  • Avro Format  │    │ • Today Filter   │    │ • JSON Format   │
│  • Receipt Data │    │ • KeyBy Franchise│    │ • Sales Total   │
│  • Real-time    │    │ • State Aggreg.  │    │ • Real-time     │
└─────────────────┘    └──────────────────┘    └─────────────────┘

```

---

## 📁 프로젝트 구조

### 🗂️ **디렉토리 구성**
```
src/main/
├── java/com/kafka/sales/
│   ├── 📱 SalesTotalRealtimeApp.java          # 메인 애플리케이션
│   ├── config/
│   ├── functions/                              # Flink 처리 함수들
│   │   ├── 🔍 TodayReceiptFilter.java         # 오늘 날짜 필터
│   │   ├── 🔑 FranchiseKeySelector.java       # 프랜차이즈 키 선택기
│   │   └── 📊 DailyCumulativeSalesProcessor.java # 누적 매출 처리
│   ├── model/                                  # 데이터 모델
│   │   ├── 📄 ReceiptData.java                # 영수증 데이터 모델
│   │   └── 📈 SalesTotalData.java             # 매출 총계 모델
│   └── utils/                                  # 유틸리티 클래스
│       ├── ⚙️ AppProperties.java              # 설정 관리
│       ├── 📥 SimpleAvroDeserializationSchema.java # Avro 역직렬화
│       └── 📤 SalesTotalJsonSerializationSchema.java # JSON 직렬화
├── avro/
│   └── 📋 receipt.avsc                        # Avro 스키마 정의
└── resources/
    └── ⚙️ application.properties              # 애플리케이션 설정

```

### 🔧 **핵심 컴포넌트**

| 컴포넌트 | 역할 | 설명 |
|----------|------|------|
| **TodayReceiptFilter** | 📅 날짜 필터링 | 오늘 날짜 영수증만 통과 |
| **FranchiseKeySelector** | 🔑 키 선택 | 프랜차이즈별 데이터 그룹화 |
| **DailyCumulativeSalesProcessor** | 📊 상태 집계 | 실시간 누적 매출 계산 |
| **SimpleAvroDeserializationSchema** | 📥 역직렬화 | Avro → Java 객체 변환 |
| **SalesTotalJsonSerializationSchema** | 📤 직렬화 | Java 객체 → JSON 변환 |

---

## ⚙️ 설정 및 실행

### 🔧 **환경 설정**

#### **1. application.properties 설정**
```properties
# Kafka 설정
kafka.bootstrap.servers=13.209.157.53:9092,15.164.111.153:9092,3.34.32.69:9092
kafka.source.topic=test-topic
kafka.sink.topic=sales_total_realtime
kafka.consumer.group=sales-total-group

# Schema Registry 설정
schema.registry.url=http://43.201.175.172:8081,http://43.202.127.159:8081

# Flink 설정
flink.parallelism=1
flink.checkpoint.interval=60000

# 애플리케이션 설정
app.timezone=Asia/Seoul
```

#### **2. JVM 설정** (Java 17 모듈 시스템 대응)
```bash
--add-opens java.base/java.util=ALL-UNNAMED
--add-opens java.base/java.lang=ALL-UNNAMED
```

### 🚀 **빌드 및 실행**

#### **빌드**
```bash
# 의존성 포함된 실행 가능한 JAR 생성
./gradlew clean shadowJar
```

#### **실행**
```bash
# 로컬 실행
java --add-opens java.base/java.util=ALL-UNNAMED \
     --add-opens java.base/java.lang=ALL-UNNAMED \
     -jar build/libs/sales-total-realtime.jar
```

#### **개발 모드 실행**
```bash
# 개발 환경에서 직접 실행
./gradlew run
```

---

## 📊 데이터 스키마

### 📥 **입력 데이터 (ReceiptData)**
```json
{
  "franchise_id": 101,
  "store_brand": "Starbucks",
  "store_id": 1001,
  "store_name": "강남점",
  "region": "서울",
  "store_address": "서울시 강남구...",
  "menu_items": [
    {
      "menu_id": 1,
      "menu_name": "아메리카노",
      "unit_price": 4500,
      "quantity": 2
    }
  ],
  "total_price": 9000,
  "user_id": 12345,
  "time": "2025-05-22 14:30:15",
  "user_name": "홍길동",
  "user_gender": "M",
  "user_age": 30
}
```

### 📤 **출력 데이터 (SalesTotalData)**
```json
{
  "franchise_id": 101,
  "store_brand": "Starbucks",
  "store_count": 15,
  "total_sales": 2450000,
  "update_time": "2025-05-22 14:30:15"
}
```

---

## 🔍 모니터링 및 로깅


### 📈 **주요 메트릭**
- **처리량**: 초당 처리되는 영수증 수
- **지연시간**: 데이터 입력부터 출력까지의 시간
- **상태 크기**: 각 프랜차이즈별 상태 데이터 크기
- **체크포인트**: 장애 복구를 위한 상태 저장 주기

### 📋 **로그 레벨**
```properties
# 운영 환경
logging.level.com.kafka.sales=INFO

# 개발 환경 (상세 디버깅)
logging.level.com.kafka.sales=DEBUG
```

### 📁 **로그 위치**
- **콘솔 출력**: 실시간 모니터링
- **파일 출력**: `logs/sales-total-realtime.log`

---

## 🛠️ 개발 가이드

### 🔄 **데이터 처리 플로우**

1. **📥 데이터 수신**
   ```java
   // Kafka Consumer가 Avro 데이터 수신
   FlinkKafkaConsumer<ReceiptData> consumer = new FlinkKafkaConsumer<>(
       sourceTopic, new SimpleAvroDeserializationSchema<>(), consumerProps);
   ```

2. **🔍 필터링**
   ```java
   // 오늘 날짜 데이터만 통과
   .filter(new TodayReceiptFilter())
   ```

3. **🔑 키 기반 그룹화**
   ```java
   // 프랜차이즈별로 데이터 분할
   .keyBy(new FranchiseKeySelector())
   ```

4. **📊 상태 기반 집계**
   ```java
   // 실시간 누적 계산
   .process(new DailyCumulativeSalesProcessor())
   ```

5. **📤 결과 출력**
   ```java
   // JSON으로 직렬화하여 Kafka 전송
   .addSink(new FlinkKafkaProducer<>(...))
   ```

### 🧪 **테스트 방법**

#### **단위 테스트**
```bash
./gradlew test
```

#### **통합 테스트**
1. 로컬 Kafka 클러스터 실행
2. 테스트 데이터 전송
3. 결과 검증

---

## 🚨 트러블슈팅

### ❓ **자주 발생하는 문제들**

| 문제 | 원인 | 해결방법 |
|------|------|----------|
| **Avro schema file not found** | 스키마 파일 경로 문제 | `sourceSets` 설정 확인 |
| **InaccessibleObjectException** | Java 17 모듈 시스템 | JVM 옵션 추가 |
| **Kafka connection failed** | 브로커 주소 오류 | `application.properties` 확인 |
| **Consumer lag 증가** | 처리 성능 부족 | 병렬도 증가 고려 |

### 🔧 **성능 튜닝**

#### **병렬 처리 최적화**
```properties
# 병렬도 증가 (CPU 코어 수에 맞춰 조정)
flink.parallelism=4
```

#### **체크포인트 간격 조정**
```properties
# 체크포인트 간격 (장애 복구 vs 성능 트레이드오프)
flink.checkpoint.interval=30000  # 30초
```

#### **Kafka 설정 최적화**
```properties
# 배치 처리 최적화
batch.size=32768
linger.ms=5
```

---

## 🔄 확장 계획

### 📈 **단기 개선사항**
- [ ] **알림 시스템**: 매출 임계값 초과 시 알림
- [ ] **대시보드 연동**: 실시간 매출 현황 시각화
- [ ] **다중 브랜드 지원**: 브랜드별 별도 집계
- [ ] **시간대별 분석**: 시간별 매출 패턴 분석

### 🚀 **장기 로드맵**
- [ ] **머신러닝 예측**: 매출 예측 모델 통합
- [ ] **멀티 클러스터**: 글로벌 확장 지원
- [ ] **스트림 조인**: 고객 정보와 매출 데이터 조인
- [ ] **실시간 추천**: 개인화된 메뉴 추천 시스템

---

## 👥 기여 가이드

### 🤝 **기여 방법**
1. **Fork** 및 **Clone**
2. **Feature Branch** 생성
3. **코드 작성** 및 **테스트**
4. **Pull Request** 생성

### 📝 **코딩 스타일**
- **Java Code Convention** 준수
- **주석**: 모든 클래스와 메서드에 상세 주석
- **테스트**: 새로운 기능에 대한 단위 테스트 필수
- **로깅**: 적절한 로그 레벨 사용

### 🔍 **코드 리뷰 체크포인트**
- **성능**: 메모리 누수 및 성능 최적화
- **안정성**: 예외 처리 및 에러 핸들링
- **가독성**: 코드 구조 및 주석 품질
- **테스트**: 테스트 커버리지 및 품질

---

## 📜 라이선스

이 프로젝트는 **MIT License**하에 배포됩니다.

---

## 📞 연락처

- **프로젝트 관리자**: Sales Analytics Team
- **기술 문의**: 개발팀 Slack 채널
- **버그 리포트**: GitHub Issues

---

Apache License 2.0

