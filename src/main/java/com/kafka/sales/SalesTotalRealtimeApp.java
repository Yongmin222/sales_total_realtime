package com.kafka.sales;

import com.kafka.sales.functions.FranchiseKeySelector;
import com.kafka.sales.functions.DailyCumulativeSalesProcessor;
import com.kafka.sales.functions.TodayReceiptFilter;
import com.kafka.sales.model.ReceiptData;
import com.kafka.sales.model.SalesTotalData;
import com.kafka.sales.utils.AppProperties;
import com.kafka.sales.utils.SalesTotalJsonSerializationSchema;
import com.kafka.sales.utils.SimpleAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.TimeZone;

/**
 * =======================================================
 * 실시간 매출 집계 메인 애플리케이션
 * =======================================================
 * 
 * 📋 기능 개요:
 * - Kafka에서 Avro 형식의 영수증 데이터를 실시간으로 읽어옴
 * - 오늘 날짜의 데이터만 필터링
 * - 프랜차이즈별로 그룹화하여 누적 매출 계산
 * - 결과를 JSON 형식으로 Kafka에 전송
 * 
 * 🔄 데이터 플로우:
 * Kafka(Avro) → Filter(오늘) → KeyBy(프랜차이즈) → Process(집계) → Kafka(JSON)
 * 
 * ⚙️ 주요 설정:
 * - 입력: test-topic (Avro 형식)
 * - 출력: sales_total_realtime (JSON 형식)
 * - 타임존: Asia/Seoul
 */
public class SalesTotalRealtimeApp {
    private static final Logger LOG = LoggerFactory.getLogger(SalesTotalRealtimeApp.class);
    
    /**
     * =======================================================
     * 애플리케이션 진입점
     * =======================================================
     */
    public static void main(String[] args) throws Exception {
        // 🔧 시스템 타임존 설정 (한국 시간)
        TimeZone.setDefault(TimeZone.getTimeZone(AppProperties.getTimezone()));
        LOG.info("Setting timezone to: {}", AppProperties.getTimezone());
        
        // 📊 애플리케이션 시작 로그
        logApplicationInfo();
        
        // 🎯 Flink 실행 환경 구성
        StreamExecutionEnvironment env = createFlinkEnvironment();
        
        // 📥 Kafka Consumer 설정 (입력 스트림)
        FlinkKafkaConsumer<ReceiptData> consumer = createKafkaConsumer();
        
        // 📤 Kafka Producer 설정 (출력 스트림)
        FlinkKafkaProducer<SalesTotalData> producer = createKafkaProducer();
        
        // 🔄 데이터 처리 파이프라인 구성
        buildDataPipeline(env, consumer, producer);
        
        // 🚀 잡 실행
        env.execute("Sales Total Realtime Aggregation");
    }
    
    /**
     * =======================================================
     * 애플리케이션 정보 로깅
     * =======================================================
     */
    private static void logApplicationInfo() {
        LOG.info("Starting Sales Total Realtime Application");
        LOG.info("Source Topic: {} → Sink Topic: {}", AppProperties.getSourceTopic(), AppProperties.getSinkTopic());
        LOG.info("Bootstrap Servers: {}", AppProperties.getBootstrapServers());
        LOG.info("Consumer Group: {}", AppProperties.getConsumerGroup());
        LOG.info("Parallelism: {}", AppProperties.getParallelism());
    }
    
    /**
     * =======================================================
     * Flink 실행 환경 생성 및 구성
     * =======================================================
     * 
     * 🔧 설정 항목:
     * - 병렬 처리 수준 (parallelism)
     * - 체크포인트 간격 (장애 복구용)
     */
    private static StreamExecutionEnvironment createFlinkEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(AppProperties.getParallelism());
        env.enableCheckpointing(AppProperties.getCheckpointInterval());
        
        LOG.info("Flink environment configured with parallelism: {} and checkpoint interval: {}ms", 
                AppProperties.getParallelism(), AppProperties.getCheckpointInterval());
        return env;
    }
    
    /**
     * =======================================================
     * Kafka Consumer 생성 (입력 스트림)
     * =======================================================
     * 
     * 📥 역할:
     * - test-topic에서 Avro 형식의 영수증 데이터 읽기
     * - 자동으로 ReceiptData 객체로 역직렬화
     * - Consumer Group으로 중복 처리 방지
     */
    private static FlinkKafkaConsumer<ReceiptData> createKafkaConsumer() {
        Properties consumerProps = createConsumerProperties();
        
        return new FlinkKafkaConsumer<>(
            AppProperties.getSourceTopic(),
            new SimpleAvroDeserializationSchema<>(ReceiptData.class),
            consumerProps
        );
    }
    
    /**
     * =======================================================
     * Kafka Producer 생성 (출력 스트림)
     * =======================================================
     * 
     * 📤 역할:
     * - SalesTotalData 객체를 JSON으로 직렬화
     * - sales_total_realtime 토픽으로 전송
     * - At-Least-Once 보장 (데이터 손실 방지)
     */
    private static FlinkKafkaProducer<SalesTotalData> createKafkaProducer() {
        Properties producerProps = createProducerProperties();
        
        return new FlinkKafkaProducer<>(
            AppProperties.getSinkTopic(),
            new SalesTotalJsonSerializationSchema(AppProperties.getSinkTopic()),
            producerProps,
            FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );
    }
    
    /**
     * =======================================================
     * 데이터 처리 파이프라인 구성
     * =======================================================
     * 
     * 🔄 처리 순서:
     * 1. Kafka에서 영수증 데이터 읽기
     * 2. 오늘 날짜 데이터만 필터링
     * 3. 프랜차이즈 ID별로 그룹화
     * 4. 누적 매출 계산 (상태 기반)
     * 5. 결과를 Kafka로 전송
     */
    private static void buildDataPipeline(StreamExecutionEnvironment env, 
                                         FlinkKafkaConsumer<ReceiptData> consumer,
                                         FlinkKafkaProducer<SalesTotalData> producer) {
        
        env.addSource(consumer)
            .name("Receipt Data Source")
            
            // 📊 디버깅을 위한 데이터 로깅
            .map(receipt -> {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Received receipt: franchise_id={}, time={}", 
                             receipt.getFranchise_id(), receipt.getTime());
                }
                return receipt;
            })
            
            // 🗓️ 오늘 날짜 데이터만 필터링
            .filter(new TodayReceiptFilter())
            .name("Today Filter")
            
            // 🔑 프랜차이즈 ID로 그룹화 (병렬 처리를 위한 키 분할)
            .keyBy(new FranchiseKeySelector())
            
            // 📈 누적 매출 계산 (상태 기반 처리)
            .process(new DailyCumulativeSalesProcessor())
            .name("Sales Aggregation")
            
            // 📤 결과를 Kafka로 전송
            .addSink(producer)
            .name("Sales Total Sink");
    }
    
    /**
     * =======================================================
     * Kafka Consumer 설정 생성
     * =======================================================
     * 
     * 🔧 주요 설정:
     * - bootstrap.servers: Kafka 브로커 주소
     * - group.id: Consumer Group (중복 처리 방지)
     * - auto.offset.reset: 처음 실행 시 latest부터 읽기
     */
    private static Properties createConsumerProperties() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppProperties.getBootstrapServers());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, AppProperties.getConsumerGroup());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return props;
    }
    
    /**
     * =======================================================
     * Kafka Producer 설정 생성
     * =======================================================
     * 
     * 🔧 주요 설정:
     * - acks=all: 모든 복제본에 쓰기 완료 후 응답 (안정성)
     * - retries=3: 실패 시 3번 재시도
     * - 배치 처리 및 메모리 최적화 설정
     */
    private static Properties createProducerProperties() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppProperties.getBootstrapServers());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        return props;
    }
}
