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

public class SimpleAvroDeserializationSchema<T> implements DeserializationSchema<T> {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleAvroDeserializationSchema.class);
    
    private final Class<T> targetType;
    private transient Schema schema;
    private transient DatumReader<GenericRecord> datumReader;
    
    // Magic byte and schema ID length (Confluent Schema Registry wire format)
    private static final byte MAGIC_BYTE = 0x00;
    private static final int SCHEMA_ID_LENGTH = 4;
    
    // Avro 스키마 파일 경로 - resources에서 읽기
    private static final String SCHEMA_RESOURCE_PATH = "receipt.avsc";

    public SimpleAvroDeserializationSchema(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        // 여러 경로에서 스키마 파일 찾기 시도
        String[] possiblePaths = {
            "receipt.avsc",                           // classpath root
            "src/main/avro/receipt.avsc",            // 프로젝트 상대 경로
            "./src/main/avro/receipt.avsc",          // 현재 디렉토리 기준
            "../src/main/avro/receipt.avsc",         // 상위 디렉토리 기준
            "/opt/flink/src/main/avro/receipt.avsc", // 절대 경로 (Flink 실행환경)
            System.getProperty("user.dir") + "/src/main/avro/receipt.avsc" // 시스템 작업 디렉토리
        };
        
        InputStream schemaInputStream = null;
        String foundPath = null;
        
        // 1. 먼저 classpath에서 시도
        for (String path : possiblePaths) {
            if (path.startsWith("/") || path.startsWith("./") || path.startsWith("../") || path.contains(":")) {
                continue; // 파일 시스템 경로는 나중에 시도
            }
            schemaInputStream = getClass().getClassLoader().getResourceAsStream(path);
            if (schemaInputStream != null) {
                foundPath = "classpath:" + path;
                break;
            }
        }
        
        // 2. classpath에서 못 찾으면 파일 시스템에서 시도
        if (schemaInputStream == null) {
            for (String path : possiblePaths) {
                if (!path.startsWith("/") && !path.startsWith("./") && !path.startsWith("../") && !path.contains(":")) {
                    continue; // classpath 경로는 이미 시도했음
                }
                try {
                    java.io.File file = new java.io.File(path);
                    if (file.exists() && file.isFile()) {
                        schemaInputStream = new java.io.FileInputStream(file);
                        foundPath = file.getAbsolutePath();
                        break;
                    }
                } catch (Exception e) {
                    LOG.debug("Failed to load schema from: {}", path);
                }
            }
        }
        
        if (schemaInputStream == null) {
            // 현재 작업 디렉토리 정보 로깅
            LOG.error("Current working directory: {}", System.getProperty("user.dir"));
            LOG.error("Tried paths: {}", java.util.Arrays.toString(possiblePaths));
            throw new IOException("Avro schema file not found in any of the expected locations");
        }
        
        try {
            this.schema = new Schema.Parser().parse(schemaInputStream);
            this.datumReader = new GenericDatumReader<>(schema);
            LOG.info("Successfully loaded Avro schema from: {}", foundPath);
        } finally {
            schemaInputStream.close();
        }
    }

    @Override
    public T deserialize(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        try {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
            
            // Check if data includes Confluent Schema Registry wire format
            if (bytes.length > 5 && bytes[0] == MAGIC_BYTE) {
                // Skip magic byte and schema ID (5 bytes total)
                inputStream.skip(5);
            }
            
            Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
            GenericRecord record = datumReader.read(null, decoder);
            
            // Convert GenericRecord to ReceiptData
            return (T) convertToReceiptData(record);
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to deserialize Avro message: {}", e.getMessage());
            }
            throw new IOException("Failed to deserialize Avro message", e);
        }
    }

    private ReceiptData convertToReceiptData(GenericRecord record) {
        ReceiptData receipt = new ReceiptData();
        
        receipt.setFranchise_id((Integer) record.get("franchise_id"));
        receipt.setStore_brand(record.get("store_brand").toString());
        receipt.setStore_id((Integer) record.get("store_id"));
        receipt.setStore_name(record.get("store_name").toString());
        receipt.setRegion(record.get("region").toString());
        receipt.setStore_address(record.get("store_address").toString());
        receipt.setTotal_price((Integer) record.get("total_price"));
        receipt.setUser_id((Integer) record.get("user_id"));
        receipt.setTime(record.get("time").toString());
        receipt.setUser_name(record.get("user_name").toString());
        receipt.setUser_gender(record.get("user_gender").toString());
        receipt.setUser_age((Integer) record.get("user_age"));
        
        // Convert menu items
        List<ReceiptData.MenuItem> menuItems = new ArrayList<>();
        List<GenericRecord> items = (List<GenericRecord>) record.get("menu_items");
        for (GenericRecord item : items) {
            ReceiptData.MenuItem menuItem = new ReceiptData.MenuItem();
            menuItem.setMenu_id((Integer) item.get("menu_id"));
            menuItem.setMenu_name(item.get("menu_name").toString());
            menuItem.setUnit_price((Integer) item.get("unit_price"));
            menuItem.setQuantity((Integer) item.get("quantity"));
            menuItems.add(menuItem);
        }
        receipt.setMenu_items(menuItems);
        
        return receipt;
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(targetType);
    }
}
