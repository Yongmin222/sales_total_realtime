package com.kafka.sales.utils;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ConfluentAvroDeserializationSchema<T extends SpecificRecord> 
    implements DeserializationSchema<T> {
    
    private final Class<T> avroType;
    private final String schemaRegistryUrl;
    private transient KafkaAvroDeserializer deserializer;
    
    public ConfluentAvroDeserializationSchema(Class<T> avroType, String schemaRegistryUrl) {
        this.avroType = avroType;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }
    
    @Override
    public void open(InitializationContext context) throws Exception {
        Map<String, Object> props = new HashMap<>();
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        
        deserializer = new KafkaAvroDeserializer();
        deserializer.configure(props, false);
    }
    
    @Override
    public T deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        return avroType.cast(deserializer.deserialize(null, message));
    }
    
    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }
    
    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(avroType);
    }
}
