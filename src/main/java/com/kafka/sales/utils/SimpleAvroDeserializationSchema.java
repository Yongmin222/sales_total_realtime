package com.kafka.sales.utils;

import com.kafka.sales.model.ReceiptData;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * =======================================================
 * Avro 바이너리 데이터를 Java 객체로 변환하는 클래스
 * =======================================================
 * 
 * 📋 기능:
 * - Kafka에서 받은 Avro 바이너리 데이터를 ReceiptData 객체로 변환
 * - Confluent Schema Registry 헤더 자동 처리
 * - 스키마 파일 기반 안전한 역직렬화
 * 
 * 🔄 변환 과정:
 * 1. 바이너리 데이터 → GenericRecord (Avro)
 * 2. GenericRecord → ReceiptData (Java Object)
 * 3. 중첩된 MenuItem 배열도 함께 변환
 * 
 * 📁 스키마 파일: src/main/avro/receipt.avsc
 * (sourceSets 설정으로 classpath에 자동 포함)
 */
public class SimpleAvroDeserializationSchema<T> implements DeserializationSchema<T> {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleAvroDeserializationSchema.class);
    
    // =======================================================
    // 필드 및 상수
    // =======================================================
    
    private final Class<T> targetType;
    private transient Schema schema;              // Avro 스키마 (직렬화 불가)
    private transient DatumReader<GenericRecord> datumReader;  // Avro 리더 (직렬화 불가)
    
    // Confluent Schema Registry 헤더 식별 바이트
    private static final byte MAGIC_BYTE = 0x00;
    
    // 스키마 파일 경로 (classpath 기준)
    private static final String SCHEMA_RESOURCE_PATH = "receipt.avsc";

    /**
     * 생성자: 대상 타입 클래스 지정
     * @param targetType 변환할 대상 클래스 (예: ReceiptData.class)
     */
    public SimpleAvroDeserializationSchema(Class<T> targetType) {
        this.targetType = targetType;
    }

    /**
     * =======================================================
     * 초기화: 스키마 파일 로드 및 리더 설정
     * =======================================================
     * 
     * 🔧 수행 작업:
     * 1. classpath에서 receipt.avsc 파일 찾기
     * 2. Avro 스키마 파싱
     * 3. GenericDatumReader 초기화
     * 
     * 📁 스키마 파일 위치:
     * - 원본: src/main/avro/receipt.avsc
     * - 런타임: JAR 내부 classpath
     */
    @Override
    public void open(InitializationContext context) throws Exception {
        // sourceSets 설정으로 src/main/avro가 classpath에 포함됨
        try (InputStream schemaInputStream = getClass().getClassLoader().getResourceAsStream(SCHEMA_RESOURCE_PATH)) {
            if (schemaInputStream == null) {
                throw new IOException("Avro schema file not found in classpath: " + SCHEMA_RESOURCE_PATH);
            }
            
            // 🔧 스키마 파싱 및 리더 초기화
            this.schema = new Schema.Parser().parse(schemaInputStream);
            this.datumReader = new GenericDatumReader<>(schema);
            LOG.info("Successfully loaded Avro schema from classpath: {}", SCHEMA_RESOURCE_PATH);
        } catch (Exception e) {
            LOG.error("Failed to load Avro schema: {}", SCHEMA_RESOURCE_PATH, e);
            throw e;
        }
    }

    /**
     * =======================================================
     * 메인 역직렬화 메서드
     * =======================================================
     * 
     * 🔄 변환 과정:
     * 1. 바이너리 데이터 입력 스트림 생성
     * 2. Confluent Schema Registry 헤더 처리
     * 3. Avro 디코더로 GenericRecord 생성
     * 4. GenericRecord → ReceiptData 변환
     * 
     * @param bytes Kafka에서 받은 Avro 바이너리 데이터
     * @return 변환된 ReceiptData 객체
     */
    @Override
    public T deserialize(byte[] bytes) throws IOException {
        // 🚫 빈 데이터 처리
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        try {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
            
            // 🏷️ Confluent Schema Registry 헤더 처리
            // Magic Byte(1) + Schema ID(4) = 5바이트 스킵
            if (bytes.length > 5 && bytes[0] == MAGIC_BYTE) {
                inputStream.skip(5); // Magic byte + Schema ID
            }
            
            // 🔄 Avro 바이너리 → GenericRecord 변환
            Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
            GenericRecord record = datumReader.read(null, decoder);
            
            // 🎯 GenericRecord → ReceiptData 변환
            return (T) convertToReceiptData(record);
        } catch (Exception e) {
            LOG.debug("Failed to deserialize Avro message: {}", e.getMessage());
            throw new IOException("Failed to deserialize Avro message", e);
        }
    }

    /**
     * =======================================================
     * GenericRecord를 ReceiptData로 변환
     * =======================================================
     * 
     * 🔄 변환 대상:
     * - 기본 필드: franchise_id, store_brand 등
     * - 중첩 배열: menu_items (MenuItem 객체 배열)
     * 
     * 🛡️ 안전성: Null 값 처리 및 타입 검증
     */
    private ReceiptData convertToReceiptData(GenericRecord record) {
        ReceiptData receipt = new ReceiptData();
        
        // 📊 기본 영수증 정보 변환
        receipt.setFranchise_id(getIntField(record, "franchise_id"));
        receipt.setStore_brand(getStringField(record, "store_brand"));
        receipt.setStore_id(getIntField(record, "store_id"));
        receipt.setStore_name(getStringField(record, "store_name"));
        receipt.setRegion(getStringField(record, "region"));
        receipt.setStore_address(getStringField(record, "store_address"));
        receipt.setTotal_price(getIntField(record, "total_price"));
        receipt.setUser_id(getIntField(record, "user_id"));
        receipt.setTime(getStringField(record, "time"));
        receipt.setUser_name(getStringField(record, "user_name"));
        receipt.setUser_gender(getStringField(record, "user_gender"));
        receipt.setUser_age(getIntField(record, "user_age"));
        
        // 🍔 메뉴 아이템 배열 변환
        receipt.setMenu_items(convertMenuItems(record));
        
        return receipt;
    }

    /**
     * =======================================================
     * 메뉴 아이템 배열 변환
     * =======================================================
     * 
     * 🔄 변환 과정:
     * 1. GenericRecord에서 menu_items 배열 추출
     * 2. 각 배열 요소를 MenuItem 객체로 변환
     * 3. 변환된 MenuItem 목록 반환
     */
    private List<ReceiptData.MenuItem> convertMenuItems(GenericRecord record) {
        List<ReceiptData.MenuItem> menuItems = new ArrayList<>();
        
        // 📋 타입 안전성을 위한 SuppressWarnings
        @SuppressWarnings("unchecked")
        List<GenericRecord> items = (List<GenericRecord>) record.get("menu_items");
        
        if (items != null) {
            for (GenericRecord item : items) {
                ReceiptData.MenuItem menuItem = new ReceiptData.MenuItem();
                menuItem.setMenu_id(getIntField(item, "menu_id"));
                menuItem.setMenu_name(getStringField(item, "menu_name"));
                menuItem.setUnit_price(getIntField(item, "unit_price"));
                menuItem.setQuantity(getIntField(item, "quantity"));
                menuItems.add(menuItem);
            }
        }
        
        return menuItems;
    }

    // =======================================================
    // 안전한 필드 추출 헬퍼 메서드
    // =======================================================

    /**
     * 정수 필드 안전 추출 (Null → 0 변환)
     */
    private int getIntField(GenericRecord record, String fieldName) {
        Object value = record.get(fieldName);
        return value != null ? (Integer) value : 0;
    }

    /**
     * 문자열 필드 안전 추출 (Null → "" 변환)
     */
    private String getStringField(GenericRecord record, String fieldName) {
        Object value = record.get(fieldName);
        return value != null ? value.toString() : "";
    }

    // =======================================================
    // Flink 인터페이스 구현 메서드
    // =======================================================

    /**
     * 스트림 종료 조건 (항상 false = 무한 스트림)
     */
    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    /**
     * 반환 타입 정보 제공 (Flink 내부 사용)
     */
    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(targetType);
    }
}
