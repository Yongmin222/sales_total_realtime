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

/**
 * =======================================================
 * 프랜차이즈별 일일 누적 매출 처리 클래스
 * =======================================================
 * 
 * 📋 주요 기능:
 * - 프랜차이즈별로 하루 동안의 매출을 실시간 누적 계산
 * - 매장 수 자동 카운팅 (중복 제거)
 * - 날짜 변경 시 자동 상태 초기화
 * - 매 영수증마다 최신 누적 결과 출력
 * 
 * 🗄️ 상태 관리:
 * - totalSalesState: 프랜차이즈별 누적 매출액
 * - storesState: 방문한 매장 ID 목록 (중복 제거용)
 * - lastDateState: 마지막 처리 날짜 (날짜 변경 감지용)
 * - lastBrandState: 마지막 브랜드명 (결과 생성용)
 * 
 * 🔄 처리 과정:
 * 1. 날짜 변경 체크 → 필요시 상태 초기화
 * 2. 새로운 매장 감지 → 매장 수 업데이트
 * 3. 매출액 누적 → 총 매출에 추가
 * 4. 결과 생성 → SalesTotalData 출력
 * 
 * 💡 실시간 특성:
 * - 매 영수증 처리마다 즉시 결과 출력
 * - 윈도우 없이 연속적인 상태 업데이트
 * - 장애 복구 시 체크포인트에서 상태 복원
 */
public class DailyCumulativeSalesProcessor extends KeyedProcessFunction<Integer, ReceiptData, SalesTotalData> {
    private static final Logger LOG = LoggerFactory.getLogger(DailyCumulativeSalesProcessor.class);
    
    // =======================================================
    // 날짜/시간 포맷터 설정
    // =======================================================
    
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    // 🌏 타임존 설정 (한국 시간)
    static {
        TimeZone timezone = TimeZone.getTimeZone(AppProperties.getTimezone());
        DATE_FORMAT.setTimeZone(timezone);
        DATETIME_FORMAT.setTimeZone(timezone);
    }
    
    // =======================================================
    // 상태 변수 선언 (Flink 관리 상태)
    // =======================================================
    
    /** 프랜차이즈별 누적 매출액 */
    private ValueState<Long> totalSalesState;
    
    /** 방문한 매장 ID 목록 (중복 제거용) */
    private MapState<Integer, Boolean> storesState;
    
    /** 마지막 처리 날짜 (날짜 변경 감지용) */
    private ValueState<String> lastDateState;
    
    /** 마지막 브랜드명 (결과 생성용) */
    private ValueState<String> lastBrandState;
    
    /**
     * =======================================================
     * 상태 초기화 메서드
     * =======================================================
     * 
     * 🔧 수행 작업:
     * - Flink 관리 상태 객체들 초기화
     * - 상태는 키(프랜차이즈 ID)별로 독립적으로 관리됨
     * - 장애 복구 시 체크포인트에서 자동 복원
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 💰 누적 매출액 상태 (Long 타입)
        totalSalesState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("totalSales", Long.class));
            
        // 🏪 매장 목록 상태 (Map<매장ID, Boolean>)
        storesState = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("stores", Types.INT, Types.BOOLEAN));
            
        // 📅 마지막 날짜 상태 (String 타입)
        lastDateState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("lastDate", String.class));
            
        // 🏷️ 마지막 브랜드 상태 (String 타입)
        lastBrandState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("lastBrand", String.class));
    }
    
    /**
     * =======================================================
     * 메인 처리 메서드 (영수증 하나씩 처리)
     * =======================================================
     * 
     * 🔄 처리 단계:
     * 1. 날짜 변경 체크 및 상태 초기화
     * 2. 현재 상태 값들 가져오기
     * 3. 새로운 매장 감지 및 등록
     * 4. 매출액 누적 계산
     * 5. 상태 업데이트
     * 6. 결과 생성 및 출력
     * 
     * @param receipt 처리할 영수증 데이터
     * @param ctx 처리 컨텍스트 (시간, 키 정보 등)
     * @param out 결과 출력 컬렉터
     */
    @Override
    public void processElement(ReceiptData receipt, Context ctx, Collector<SalesTotalData> out) throws Exception {
        
        // =======================================================
        // 1. 날짜 변경 체크 및 상태 초기화
        // =======================================================
        
        String today = DATE_FORMAT.format(new Date());
        String lastDate = lastDateState.value();
        
        // 🔄 새로운 날이 시작되면 모든 상태 초기화
        if (lastDate == null || !today.equals(lastDate)) {
            LOG.info("New day detected. Resetting state for franchise {}", receipt.getFranchise_id());
            resetDailyState();
            lastDateState.update(today);
        }
        
        // =======================================================
        // 2. 현재 상태 값들 가져오기 및 업데이트
        // =======================================================
        
        // 💰 현재 누적 매출액 가져오기 (없으면 0)
        Long currentTotal = getCurrentTotal();
        
        // 🏪 새로운 매장인지 확인하고 등록
        boolean isNewStore = addStoreIfNew(receipt.getStore_id());
        
        // 💰 매출액 누적 (현재 영수증 금액 추가)
        currentTotal += receipt.getTotal_price();
        totalSalesState.update(currentTotal);
        
        // 🏷️ 브랜드 정보 업데이트 (결과 생성용)
        lastBrandState.update(receipt.getStore_brand());
        
        // =======================================================
        // 3. 결과 생성 및 출력
        // =======================================================
        
        // 📊 현재 매장 수 계산
        int storeCount = getStoreCount();
        
        // 📋 결과 객체 생성
        SalesTotalData result = createResult(receipt, currentTotal, storeCount);
        
        // 🔍 디버깅 로그 (필요시)
        logProcessingInfo(receipt, currentTotal, storeCount);
        
        // 📤 결과 출력 (다음 단계로 전송)
        out.collect(result);
    }
    
    // =======================================================
    // 헬퍼 메서드들 (코드 가독성 향상)
    // =======================================================
    
    /**
     * 일일 상태 초기화 (새로운 날 시작시)
     */
    private void resetDailyState() throws Exception {
        totalSalesState.clear();
        storesState.clear();
        lastBrandState.clear();
    }
    
    /**
     * 현재 누적 매출액 안전하게 가져오기
     * @return 누적 매출액 (없으면 0L)
     */
    private Long getCurrentTotal() throws Exception {
        Long currentTotal = totalSalesState.value();
        return currentTotal != null ? currentTotal : 0L;
    }
    
    /**
     * 새로운 매장이면 등록하고 결과 반환
     * @param storeId 매장 ID
     * @return true=새 매장, false=기존 매장
     */
    private boolean addStoreIfNew(int storeId) throws Exception {
        boolean isNewStore = storesState.get(storeId) == null;
        if (isNewStore) {
            storesState.put(storeId, true);
            LOG.debug("New store detected: {}", storeId);
        }
        return isNewStore;
    }
    
    /**
     * 현재 등록된 매장 수 계산
     * @return 총 매장 수
     */
    private int getStoreCount() throws Exception {
        int count = 0;
        for (Boolean value : storesState.values()) {
            count++;
        }
        return count;
    }
    
    /**
     * 결과 객체 생성
     * @param receipt 원본 영수증 데이터
     * @param totalSales 누적 매출액
     * @param storeCount 매장 수
     * @return 생성된 결과 객체
     */
    private SalesTotalData createResult(ReceiptData receipt, Long totalSales, int storeCount) {
        return new SalesTotalData(
            receipt.getFranchise_id(),    // 프랜차이즈 ID
            receipt.getStore_brand(),     // 브랜드명
            storeCount,                   // 총 매장 수
            totalSales,                   // 누적 매출액
            DATETIME_FORMAT.format(new Date())  // 현재 시간
        );
    }
    
    /**
     * 처리 정보 디버깅 로그 출력
     */
    private void logProcessingInfo(ReceiptData receipt, Long totalSales, int storeCount) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Cumulative sales for franchise {} brand {}: {} stores, total sales: {}", 
                receipt.getFranchise_id(), receipt.getStore_brand(), storeCount, totalSales);
        }
    }
}
