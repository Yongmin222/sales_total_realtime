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

public class SalesTotalRealtimeApp {
    private static final Logger LOG = LoggerFactory.getLogger(SalesTotalRealtimeApp.class);
    
    public static void main(String[] args) throws Exception {
        // 타임존 설정
        TimeZone.setDefault(TimeZone.getTimeZone(AppProperties.getTimezone()));
        LOG.info("Setting timezone to: {}", AppProperties.getTimezone());
        
        LOG.info("Starting Sales Total Realtime Application");
        LOG.info("Source Topic: {} → Sink Topic: {}", AppProperties.getSourceTopic(), AppProperties.getSinkTopic());
        LOG.info("Bootstrap Servers: {}", AppProperties.getBootstrapServers());
        
        // Flink 환경 설정
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(AppProperties.getParallelism());
        env.enableCheckpointing(AppProperties.getCheckpointInterval());
        
        // Kafka Consumer 설정
        Properties consumerProps = createConsumerProperties();
        FlinkKafkaConsumer<ReceiptData> consumer = new FlinkKafkaConsumer<>(
            AppProperties.getSourceTopic(),
            new SimpleAvroDeserializationSchema<>(ReceiptData.class),
            consumerProps
        );
        
        // Kafka Producer 설정
        Properties producerProps = createProducerProperties();
        FlinkKafkaProducer<SalesTotalData> producer = new FlinkKafkaProducer<>(
            AppProperties.getSinkTopic(),
            new SalesTotalJsonSerializationSchema(AppProperties.getSinkTopic()),
            producerProps,
            FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );
        
        // 데이터 파이프라인
        DataStream<SalesTotalData> result = env
            .addSource(consumer).name("Receipt Data Source")
            .filter(new TodayReceiptFilter()).name("Today Filter")
            .keyBy(new FranchiseKeySelector())
            .process(new DailyCumulativeSalesProcessor()).name("Sales Aggregation")
            .returns(SalesTotalData.class);
        
        result.addSink(producer).name("Sales Total Sink");
        
        // 실행
        env.execute(AppProperties.getJobName());
    }
    
    private static Properties createConsumerProperties() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppProperties.getBootstrapServers());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, AppProperties.getConsumerGroup());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return props;
    }
    
    private static Properties createProducerProperties() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppProperties.getBootstrapServers());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        return props;
    }
}
