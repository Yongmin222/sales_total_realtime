package com.kafka.sales.functions;

import com.kafka.sales.model.ReceiptData;
import com.kafka.sales.utils.AppProperties;
import org.apache.flink.api.common.functions.FilterFunction;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class TodayReceiptFilter implements FilterFunction<ReceiptData> {
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    
    static {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone(AppProperties.getTimezone()));
    }

    @Override
    public boolean filter(ReceiptData receipt) throws Exception {
        String today = DATE_FORMAT.format(new Date());
        return receipt.getTime().startsWith(today);
    }
}
