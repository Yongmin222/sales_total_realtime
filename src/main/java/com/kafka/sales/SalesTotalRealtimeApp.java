package com.kafka.sales;

import com.kafka.sales.functions.FranchiseKeySelector;
import com.kafka.sales.functions.DailyCumulativeSalesProcessor;
import com.kafka.sales.functions.TodayReceiptFilter;
import com.kafka.sales.model.ReceiptData;
import com.kafka.sales.model.SalesTotalData;
import com.kafka.sales.utils.SalesTotalJsonSerializationSchema;
import com.kafka.sales.utils.SimpleAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class SalesTotalRealtimeApp {
    private static final Logger LOG = LoggerFactory.getLogger(SalesTotalRealtimeApp.class);
    
    public static void main(String[] args) throws Exception {
        // Load configuration
        Properties appProps = loadApplicationProperties();
        
        String bootstrapServers = appProps.getProperty("kafka.bootstrap.servers");
        String sourceTopic = appProps.getProperty("kafka.source.topic");
        String sinkTopic = appProps.getProperty("kafka.sink.topic");
        String consumerGroup = appProps.getProperty("kafka.consumer.group");
        String schemaRegistryUrl = appProps.getProperty("schema.registry.url");
        int windowSizeSeconds = Integer.parseInt(appProps.getProperty("flink.window.size.seconds", "10"));
        long checkpointInterval = Long.parseLong(appProps.getProperty("flink.checkpoint.interval", "60000"));
        
        LOG.info("Starting Sales Total Realtime Application");
        LOG.info("Bootstrap Servers: {}", bootstrapServers);
        LOG.info("Source Topic: {}", sourceTopic);
        LOG.info("Sink Topic: {}", sinkTopic);
        
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 로컬 테스트를 위해 병렬 처리를 1로 설정
        env.enableCheckpointing(checkpointInterval);
        
        // Configure Kafka consumer
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        
        // Create Kafka consumer for Avro data
        FlinkKafkaConsumer<ReceiptData> consumer = new FlinkKafkaConsumer<>(
            sourceTopic,
            new SimpleAvroDeserializationSchema<>(ReceiptData.class),
            consumerProps
        );
        
        // Configure Kafka producer
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10");
        producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        producerProps.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        
        // Create data stream from Kafka
        DataStream<ReceiptData> receiptStream = env.addSource(consumer)
            .name("Receipt Data Source")
            .map(receipt -> {
                LOG.info("Received receipt: franchise_id={}, time={}", 
                        receipt.getFranchise_id(), receipt.getTime());
                return receipt;
            });
        
        // Filter for today's receipts only
        DataStream<ReceiptData> todayReceiptStream = receiptStream
            .filter(new TodayReceiptFilter())
            .name("Today Receipt Filter");
        
        // Key by franchise_id only
        KeyedStream<ReceiptData, Integer> keyedStream = todayReceiptStream
            .keyBy(new FranchiseKeySelector());
        
        // Process with cumulative state (no windowing)
        DataStream<SalesTotalData> salesTotalStream = keyedStream
            .process(new DailyCumulativeSalesProcessor())
            .name("Daily Cumulative Sales Aggregation");
        
        // Create Kafka producer for output
        FlinkKafkaProducer<SalesTotalData> producer = new FlinkKafkaProducer<>(
            sinkTopic,
            new SalesTotalJsonSerializationSchema(sinkTopic),
            producerProps,
            FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );
        
        // Add sink to Kafka
        salesTotalStream.addSink(producer)
            .name("Sales Total Sink");
        
        // Execute the job
        env.execute("Sales Total Realtime Aggregation");
    }
    
    private static Properties loadApplicationProperties() throws Exception {
        Properties props = new Properties();
        try (InputStream inputStream = SalesTotalRealtimeApp.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (inputStream == null) {
                throw new RuntimeException("application.properties not found in classpath");
            }
            props.load(inputStream);
            return props;
        }
    }
}
