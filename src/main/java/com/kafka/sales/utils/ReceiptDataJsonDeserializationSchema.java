package com.kafka.sales.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.sales.model.ReceiptData;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ReceiptDataJsonDeserializationSchema implements DeserializationSchema<ReceiptData> {
    private static final Logger LOG = LoggerFactory.getLogger(ReceiptDataJsonDeserializationSchema.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void open(InitializationContext context) throws Exception {
        // ObjectMapper는 thread-safe하므로 추가 초기화 불필요
    }

    @Override
    public ReceiptData deserialize(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        try {
            // 원시 바이트 데이터 로깅
            LOG.info("Raw bytes (first 10): {}", 
                    java.util.Arrays.toString(java.util.Arrays.copyOfRange(bytes, 0, Math.min(10, bytes.length))));
            
            String jsonString = new String(bytes, "UTF-8");
            LOG.info("Attempting to deserialize JSON: {}", jsonString);
            return objectMapper.readValue(bytes, ReceiptData.class);
        } catch (Exception e) {
            LOG.error("Failed to deserialize JSON message: {}", new String(bytes, "UTF-8"), e);
            throw new IOException("Failed to deserialize JSON message", e);
        }
    }

    @Override
    public boolean isEndOfStream(ReceiptData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<ReceiptData> getProducedType() {
        return TypeInformation.of(ReceiptData.class);
    }
}
