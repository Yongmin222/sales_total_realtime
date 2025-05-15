# Sales Total Realtime Deployment Guide

## Prerequisites

1. Java 17 or higher
2. Apache Flink cluster running
3. Kafka cluster running
4. Schema Registry (if using Avro with Schema Registry)

## Build

```bash
cd C:\Users\r2com\Desktop\kafka\sales-total-realtime
gradlew shadowJar
```

The JAR file will be created at: `build/libs/sales-total-realtime.jar`

## Local Deployment

### 1. Start Flink Cluster

```bash
# Download and extract Flink
wget https://archive.apache.org/dist/flink/flink-1.18.0/flink-1.18.0-bin-scala_2.12.tgz
tar -xzf flink-1.18.0-bin-scala_2.12.tgz
cd flink-1.18.0

# Start cluster
./bin/start-cluster.sh
```

### 2. Create Kafka Topics

```bash
# Create input topic
kafka-topics.sh --create --topic receipt_data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Create output topic
kafka-topics.sh --create --topic sales_total_realtime --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 3. Submit Job

```bash
# Submit to Flink
flink run -c com.kafka.sales.SalesTotalRealtimeApp build/libs/sales-total-realtime.jar

# Or with parallelism
flink run -p 4 -c com.kafka.sales.SalesTotalRealtimeApp build/libs/sales-total-realtime.jar
```

## Production Deployment

### Using Kubernetes

1. Build Docker image:

```dockerfile
FROM openjdk:17-jdk-slim
WORKDIR /app
COPY build/libs/sales-total-realtime.jar /app/
CMD ["java", "-jar", "sales-total-realtime.jar"]
```

2. Deploy to Kubernetes:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sales-total-realtime
spec:
  replicas: 1
  selector:
    matchLabels:
      app: sales-total-realtime
  template:
    metadata:
      labels:
        app: sales-total-realtime
    spec:
      containers:
      - name: sales-total-realtime
        image: your-registry/sales-total-realtime:latest
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka-cluster:9092"
        - name: SCHEMA_REGISTRY_URL
          value: "http://schema-registry:8081"
```

### Configuration

Update `application.properties` for different environments:

```properties
# Production configuration
kafka.bootstrap.servers=prod-kafka-1:9092,prod-kafka-2:9092,prod-kafka-3:9092
kafka.source.topic=prod_receipt_data
kafka.sink.topic=prod_sales_total_realtime
kafka.consumer.group=prod-sales-total-group

# Schema Registry (if using)
schema.registry.url=http://prod-schema-registry:8081

# Flink Configuration
flink.window.size.seconds=60
flink.checkpoint.interval=300000
```

## Monitoring

1. **Flink Web UI**: http://localhost:8081
   - View job status
   - Check backpressure
   - Monitor metrics

2. **Kafka Manager**: Monitor topic lag and throughput

3. **Application Logs**: Check logs for errors or warnings

## Troubleshooting

### High Backpressure
- Increase parallelism
- Adjust window size
- Optimize aggregation logic

### Memory Issues
- Increase TaskManager memory
- Adjust JVM heap size
- Enable incremental checkpointing

### Kafka Consumer Lag
- Increase consumer parallelism
- Optimize deserialization
- Check network bandwidth

## Scaling

1. **Horizontal Scaling**: Increase parallelism
   ```bash
   flink modify <job-id> -p 8
   ```

2. **Vertical Scaling**: Increase TaskManager resources
   ```yaml
   taskmanager.memory.process.size: 4gb
   taskmanager.memory.flink.size: 3gb
   ```

## Maintenance

1. **Regular Health Checks**
   - Monitor job restarts
   - Check checkpoint failures
   - Review error logs

2. **Updates**
   - Use savepoints for upgrades
   - Test in staging environment
   - Roll back if needed

3. **Data Retention**
   - Configure Kafka topic retention
   - Archive old data if needed
   - Monitor disk usage
