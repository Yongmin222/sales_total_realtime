@echo off
echo Building Sales Total Realtime Application...
call gradlew.bat shadowJar

if %ERRORLEVEL% neq 0 (
    echo Build failed!
    exit /b %ERRORLEVEL%
)

echo Build successful! JAR created at build/libs/sales-total-realtime.jar
echo.
echo To submit to Flink:
echo flink run -c com.kafka.sales.SalesTotalRealtimeApp build/libs/sales-total-realtime.jar
