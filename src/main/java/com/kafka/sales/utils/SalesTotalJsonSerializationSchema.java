package com.kafka.sales.utils;

import com.kafka.sales.model.SalesTotalData;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * =======================================================
 * 매출 총계 데이터 JSON 직렬화 클래스
 * =======================================================
 * 
 * 📋 기능:
 * - SalesTotalData 객체를 JSON 형태로 직렬화
 * - Kafka ProducerRecord 생성 (키-값 쌍)
 * - 효율적인 파티셔닝을 위한 키 생성
 * 
 * 🔑 키 생성 규칙:
 * - 형식: "{프랜차이즈ID}_{브랜드명}"
 * - 예시: "101_Starbucks", "102_McDonald's"
 * - 목적: 같은 프랜차이즈+브랜드 조합이 같은 파티션으로 이동
 * 
 * 📤 출력 형태:
 * - Key: "101_Starbucks" (UTF-8 바이트)
 * - Value: {"franchise_id":101,...} (JSON UTF-8 바이트)
 * 
 * 🛡️ 안전성:
 * - Null 체크 및 예외 처리
 * - 상세한 에러 로깅
 * - Jackson 설정 최적화
 */
public class SalesTotalJsonSerializationSchema implements KafkaSerializationSchema<SalesTotalData> {
    private static final Logger LOG = LoggerFactory.getLogger(SalesTotalJsonSerializationSchema.class);
    
    // =======================================================
    // 필드 및 초기화
    // =======================================================
    
    private final ObjectMapper objectMapper;
    private final String topic;

    /**
     * 생성자: 토픽명과 ObjectMapper 설정
     * @param topic 출력할 Kafka 토픽명
     */
    public SalesTotalJsonSerializationSchema(String topic) {
        this.topic = topic;
        this.objectMapper = createOptimizedObjectMapper();
    }

    /**
     * =======================================================
     * 최적화된 ObjectMapper 생성
     * =======================================================
     * 
     * 🔧 설정 내용:
     * - FAIL_ON_EMPTY_BEANS: false (빈 객체 허용)
     * - WRITE_DATES_AS_TIMESTAMPS: false (날짜를 문자열로)
     */
    private ObjectMapper createOptimizedObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        return mapper;
    }

    /**
     * =======================================================
     * 메인 직렬화 메서드
     * =======================================================
     * 
     * 🔄 직렬화 과정:
     * 1. Null 체크
     * 2. 키 생성 (프랜차이즈ID_브랜드명)
     * 3. JSON 직렬화
     * 4. ProducerRecord 생성
     * 
     * @param element 직렬화할 SalesTotalData 객체
     * @param timestamp 타임스탬프 (nullable)
     * @return Kafka로 전송할 ProducerRecord
     */
    @Override
    public ProducerRecord<byte[], byte[]> serialize(SalesTotalData element, @Nullable Long timestamp) {
        try {
            // 🚫 Null 데이터 체크
            if (element == null) {
                LOG.warn("Received null SalesTotalData element");
                return null;
            }

            // 🔑 키 생성: "{프랜차이즈ID}_{브랜드명}"
            String key = createPartitionKey(element);
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            
            // 📋 JSON 직렬화
            byte[] valueBytes = objectMapper.writeValueAsBytes(element);
            
            // 🔍 디버깅 로그 (필요시)
            if (LOG.isDebugEnabled()) {
                LOG.debug("Serialized data for key: {}, franchise_id: {}, brand: {}", 
                         key, element.getFranchise_id(), element.getStore_brand());
            }
            
            // 📤 ProducerRecord 생성 및 반환
            return new ProducerRecord<>(topic, keyBytes, valueBytes);
            
        } catch (Exception e) {
            // 🚨 에러 로깅 (상세 정보 포함)
            LOG.error("Failed to serialize SalesTotalData: franchise_id={}, brand={}", 
                     element != null ? element.getFranchise_id() : "null",
                     element != null ? element.getStore_brand() : "null", e);
            throw new RuntimeException("Failed to serialize SalesTotalData", e);
        }
    }

    /**
     * =======================================================
     * 파티션 키 생성 (안전한 키 생성)
     * =======================================================
     * 
     * 🔑 키 형식:
     * - 정상: "{프랜차이즈ID}_{브랜드명}"
     * - 브랜드 없음: "{프랜차이즈ID}_unknown"
     * 
     * 🎯 목적:
     * - 같은 프랜차이즈+브랜드 조합을 같은 파티션으로 라우팅
     * - 효율적인 데이터 분산 및 순서 보장
     * 
     * @param element 키를 생성할 데이터 객체
     * @return 생성된 파티션 키
     */
    private String createPartitionKey(SalesTotalData element) {
        return String.format("%d_%s", 
                           element.getFranchise_id(), 
                           element.getStore_brand() != null ? element.getStore_brand() : "unknown");
    }
}
