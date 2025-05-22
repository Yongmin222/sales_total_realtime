package com.kafka.sales.utils;

import java.io.InputStream;
import java.util.Properties;

/**
 * application.properties 파일에서 설정값을 읽어오는 유틸리티 클래스
 */
public class AppProperties {
    private static Properties properties;
    
    static {
        loadProperties();
    }

    public static String getJobName() {
        return properties.getProperty("flink.job.name", "Sales Total Realtime Aggregation");
    }

    private static void loadProperties() {
        try (InputStream input = AppProperties.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            properties = new Properties();
            properties.load(input);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load application.properties", e);
        }
    }
    
    public static String getBootstrapServers() {
        return properties.getProperty("kafka.bootstrap.servers");
    }
    
    public static String getSourceTopic() {
        return properties.getProperty("kafka.source.topic");
    }
    
    public static String getSinkTopic() {
        return properties.getProperty("kafka.sink.topic");
    }
    
    public static String getConsumerGroup() {
        return properties.getProperty("kafka.consumer.group");
    }
    
    public static String getSchemaRegistryUrl() {
        return properties.getProperty("schema.registry.url");
    }
    
    public static int getParallelism() {
        return Integer.parseInt(properties.getProperty("flink.parallelism", "1"));
    }
    
    public static long getCheckpointInterval() {
        return Long.parseLong(properties.getProperty("flink.checkpoint.interval", "60000"));
    }
    
    public static String getTimezone() {
        return properties.getProperty("app.timezone", "Asia/Seoul");
    }
}
