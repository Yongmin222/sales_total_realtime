package com.kafka.sales.functions;

import com.kafka.sales.model.ReceiptData;
import com.kafka.sales.model.SalesTotalData;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;

public class DailyCumulativeSalesProcessor extends KeyedProcessFunction<Integer, ReceiptData, SalesTotalData> {
    private static final Logger LOG = LoggerFactory.getLogger(DailyCumulativeSalesProcessor.class);
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    static {
        // 한국 시간으로 설정
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"));
        DATETIME_FORMAT.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"));
    }
    
    // 프랜차이즈별 상태
    private ValueState<Long> totalSalesState;          // 누적 매출
    private MapState<Integer, Boolean> storesState;    // 매장 ID 목록 (Map으로 변경)
    private ValueState<String> lastDateState;          // 마지막 처리 날짜
    private MapState<String, Integer> brandStoreCountState;  // 브랜드별 매장 수
    
    @Override
    public void open(Configuration parameters) throws Exception {
        // State 초기화
        totalSalesState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("totalSales", Long.class));
            
        storesState = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("stores", Types.INT, Types.BOOLEAN));
            
        lastDateState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("lastDate", String.class));
            
        brandStoreCountState = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("brandStoreCount", Types.STRING, Types.INT));
    }
    
    @Override
    public void processElement(ReceiptData receipt, Context ctx, Collector<SalesTotalData> out) throws Exception {
        String today = DATE_FORMAT.format(new Date());
        String lastDate = lastDateState.value();
        
        // 날짜가 바뀌면 상태 초기화
        if (lastDate == null || !today.equals(lastDate)) {
            LOG.info("New day detected. Resetting state for franchise {}", receipt.getFranchise_id());
            totalSalesState.clear();
            storesState.clear();
            brandStoreCountState.clear();
            lastDateState.update(today);
        }
        
        // 현재 상태 가져오기
        Long currentTotal = totalSalesState.value();
        if (currentTotal == null) currentTotal = 0L;
        
        // 새로운 매장인지 확인
        boolean isNewStore = storesState.get(receipt.getStore_id()) == null;
        if (isNewStore) {
            storesState.put(receipt.getStore_id(), true);
            LOG.info("New store detected: {} for franchise {}", receipt.getStore_id(), receipt.getFranchise_id());
        }
        
        // 매출 누적
        currentTotal += receipt.getTotal_price();
        
        // 상태 업데이트
        totalSalesState.update(currentTotal);
        
        // 브랜드별 매장 수 업데이트
        Integer brandStoreCount = brandStoreCountState.get(receipt.getStore_brand());
        if (brandStoreCount == null) brandStoreCount = 0;
        if (isNewStore) {
            brandStoreCount++;
            brandStoreCountState.put(receipt.getStore_brand(), brandStoreCount);
        }
        
        // 전체 매장 수 계산
        int totalStoreCount = 0;
        for (Boolean value : storesState.values()) {
            totalStoreCount++;
        }
        
        // 결과 생성 및 출력
        SalesTotalData result = new SalesTotalData(
            receipt.getFranchise_id(),
            receipt.getStore_brand(),
            totalStoreCount,    // 전체 매장 수
            currentTotal,       // 누적 매출
            DATETIME_FORMAT.format(new Date())
        );
        
        LOG.info("Cumulative sales for franchise {} brand {}: {} stores, total sales: {}", 
                receipt.getFranchise_id(), receipt.getStore_brand(), totalStoreCount, currentTotal);
        
        out.collect(result);
    }
    
    // 정기적으로 현재 상태를 출력할 수 있는 타이머 설정 (옵션)
    public void setPeriodicTimer(Context ctx) {
        // 1분마다 현재 누적 상태 출력
        long currentTime = ctx.timerService().currentProcessingTime();
        ctx.timerService().registerProcessingTimeTimer(currentTime + 60000);
    }
    
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SalesTotalData> out) throws Exception {
        // 타이머가 발동되면 현재 누적 상태 출력
        Long currentTotal = totalSalesState.value();
        
        if (currentTotal != null) {
            // 전체 매장 수 계산
            int totalStoreCount = 0;
            for (Boolean value : storesState.values()) {
                totalStoreCount++;
            }
            
            // 각 브랜드별로 출력
            for (String brand : brandStoreCountState.keys()) {
                Integer storeCount = brandStoreCountState.get(brand);
                SalesTotalData result = new SalesTotalData(
                    ctx.getCurrentKey(),  // franchise_id
                    brand,
                    storeCount,
                    currentTotal,
                    DATETIME_FORMAT.format(new Date())
                );
                out.collect(result);
            }
        }
        
        // 다음 타이머 설정
        setPeriodicTimer(ctx);
    }
}
