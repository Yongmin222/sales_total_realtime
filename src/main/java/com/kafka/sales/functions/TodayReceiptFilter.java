package com.kafka.sales.functions;

import com.kafka.sales.model.ReceiptData;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class TodayReceiptFilter implements FilterFunction<ReceiptData> {
    private static final Logger LOG = LoggerFactory.getLogger(TodayReceiptFilter.class);
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    
    static {
        // 한국 시간으로 설정
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"));
    }

    @Override
    public boolean filter(ReceiptData receipt) throws Exception {
        String today = DATE_FORMAT.format(new Date());
        boolean isToday = receipt.getTime().startsWith(today);
        
        // DEBUG 레벨에서만 로깅하여 성능 개선
        if (LOG.isDebugEnabled()) {
            LOG.debug("Receipt time: {}, Today: {}, Filter result: {}", 
                 receipt.getTime(), today, isToday);
        }
        
        // 실제 필터링 활성화
        return isToday;
    }
}
