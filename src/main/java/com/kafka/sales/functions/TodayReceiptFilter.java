package com.kafka.sales.functions;

import com.kafka.sales.model.ReceiptData;
import com.kafka.sales.utils.AppProperties;
import org.apache.flink.api.common.functions.FilterFunction;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * =======================================================
 * 오늘 날짜 영수증 필터링 클래스
 * =======================================================
 * 
 * 📋 기능:
 * - 모든 영수증 데이터 중에서 오늘 날짜 것만 통과
 * - 과거 데이터나 미래 데이터 제외
 * - 타임존 기반 정확한 날짜 비교
 * 
 * 🎯 필터링 조건:
 * - receipt.time이 "YYYY-MM-DD"로 시작하는 것만 통과
 * - 현재 날짜 기준 (Asia/Seoul 타임존)
 * 
 * 💡 사용 예시:
 * - 입력: "2025-05-22 14:30:15" → 통과 (오늘)
 * - 입력: "2025-05-21 14:30:15" → 차단 (어제)
 */
public class TodayReceiptFilter implements FilterFunction<ReceiptData> {
    
    // 📅 날짜 포맷터 (YYYY-MM-DD 형식)
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    
    // 🌏 타임존 설정 (애플리케이션 설정에서 가져옴)
    static {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone(AppProperties.getTimezone()));
    }

    /**
     * =======================================================
     * 필터링 로직 실행
     * =======================================================
     * 
     * 🔍 판단 과정:
     * 1. 현재 날짜를 "YYYY-MM-DD" 형식으로 가져오기
     * 2. 영수증의 time 필드가 같은 날짜로 시작하는지 확인
     * 3. 일치하면 true (통과), 불일치하면 false (차단)
     * 
     * @param receipt 확인할 영수증 데이터
     * @return true=오늘 데이터(통과), false=다른 날 데이터(차단)
     */
    @Override
    public boolean filter(ReceiptData receipt) throws Exception {
        // 🚫 Null 데이터 체크
        if (receipt == null || receipt.getTime() == null) {
            return false;
        }
        
        // 📅 현재 날짜 가져오기 (YYYY-MM-DD 형식)
        String today = DATE_FORMAT.format(new Date());
        
        // 🎯 영수증 시간이 오늘 날짜로 시작하는지 확인
        boolean isToday = receipt.getTime().startsWith(today);
        
        return isToday;
    }
}
