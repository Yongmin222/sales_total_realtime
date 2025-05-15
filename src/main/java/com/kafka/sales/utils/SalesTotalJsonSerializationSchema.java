package com.kafka.sales.utils;

import com.kafka.sales.model.SalesTotalData;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class SalesTotalJsonSerializationSchema implements KafkaSerializationSchema<SalesTotalData> {
    private static final Logger LOG = LoggerFactory.getLogger(SalesTotalJsonSerializationSchema.class);
    private final ObjectMapper objectMapper;
    private final String topic;

    public SalesTotalJsonSerializationSchema(String topic) {
        this.topic = topic;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(SalesTotalData element, @Nullable Long timestamp) {
        try {
            // Use franchise_id and brand as key
            String key = element.getFranchise_id() + "_" + element.getStore_brand();
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = objectMapper.writeValueAsBytes(element);
            
            return new ProducerRecord<>(topic, keyBytes, valueBytes);
        } catch (Exception e) {
            LOG.error("Failed to serialize SalesTotalData", e);
            throw new RuntimeException("Failed to serialize SalesTotalData", e);
        }
    }
}
