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
    
    // Avro 스키마 파일 경로
    private static final String SCHEMA_RESOURCE_PATH = "avro/receipt.avsc";

    public SimpleAvroDeserializationSchema(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        try (InputStream schemaInputStream = getClass().getClassLoader().getResourceAsStream(SCHEMA_RESOURCE_PATH)) {
            if (schemaInputStream == null) {
                throw new IOException("Avro schema file not found: " + SCHEMA_RESOURCE_PATH);
            }
            
            // 스키마 파일에서 직접 로드
            this.schema = new Schema.Parser().parse(schemaInputStream);
            LOG.info("Successfully loaded Avro schema from resource: {}", SCHEMA_RESOURCE_PATH);
            this.datumReader = new GenericDatumReader<>(schema);
        } catch (Exception e) {
            LOG.error("Failed to load Avro schema from resource: {}", SCHEMA_RESOURCE_PATH, e);
            throw e;
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
