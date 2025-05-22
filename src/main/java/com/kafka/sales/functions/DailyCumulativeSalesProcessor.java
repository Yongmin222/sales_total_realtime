package com.kafka.sales.functions;

import com.kafka.sales.model.ReceiptData;
import com.kafka.sales.model.SalesTotalData;
import com.kafka.sales.utils.AppProperties;
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
import java.util.TimeZone;

public class DailyCumulativeSalesProcessor extends KeyedProcessFunction<Integer, ReceiptData, SalesTotalData> {
    private static final Logger LOG = LoggerFactory.getLogger(DailyCumulativeSalesProcessor.class);
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    static {
        TimeZone timezone = TimeZone.getTimeZone(AppProperties.getTimezone());
        DATE_FORMAT.setTimeZone(timezone);
        DATETIME_FORMAT.setTimeZone(timezone);
    }
    
    private ValueState<Long> totalSalesState;
    private MapState<Integer, Boolean> storesState;
    private ValueState<String> lastDateState;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        totalSalesState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("totalSales", Long.class));
        storesState = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("stores", Types.INT, Types.BOOLEAN));
        lastDateState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("lastDate", String.class));
    }
    
    @Override
    public void processElement(ReceiptData receipt, Context ctx, Collector<SalesTotalData> out) throws Exception {
        String today = DATE_FORMAT.format(new Date());
        String lastDate = lastDateState.value();
        
        // 날짜가 바뀌면 상태 초기화
        if (lastDate == null || !today.equals(lastDate)) {
            totalSalesState.clear();
            storesState.clear();
            lastDateState.update(today);
        }
        
        // 현재 상태 가져오기
        Long currentTotal = totalSalesState.value();
        if (currentTotal == null) currentTotal = 0L;
        
        // 새로운 매장인지 확인
        if (storesState.get(receipt.getStore_id()) == null) {
            storesState.put(receipt.getStore_id(), true);
        }
        
        // 매출 누적
        currentTotal += receipt.getTotal_price();
        totalSalesState.update(currentTotal);
        
        // 매장 수 계산
        int storeCount = 0;
        for (Boolean value : storesState.values()) {
            storeCount++;
        }
        
        // 결과 생성 및 출력
        SalesTotalData result = new SalesTotalData(
            receipt.getFranchise_id(),
            receipt.getStore_brand(),
            storeCount,
            currentTotal,
            DATETIME_FORMAT.format(new Date())
        );
        
        out.collect(result);
    }
}
