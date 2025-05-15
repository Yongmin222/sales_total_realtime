#!/bin/bash

echo "Building Sales Total Realtime Application..."
./gradlew shadowJar

if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

echo "Build successful! JAR created at build/libs/sales-total-realtime.jar"
echo
echo "To submit to Flink:"
echo "flink run -c com.kafka.sales.SalesTotalRealtimeApp build/libs/sales-total-realtime.jar"
