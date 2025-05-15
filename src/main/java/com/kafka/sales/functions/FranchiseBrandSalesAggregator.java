package com.kafka.sales.functions;

import com.kafka.sales.model.ReceiptData;
import com.kafka.sales.model.SalesTotalData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class FranchiseBrandSalesAggregator extends ProcessWindowFunction<ReceiptData, SalesTotalData, Tuple2<Integer, String>, TimeWindow> {
    private static final Logger LOG = LoggerFactory.getLogger(FranchiseBrandSalesAggregator.class);
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void process(Tuple2<Integer, String> key, Context context, Iterable<ReceiptData> receipts, Collector<SalesTotalData> out) throws Exception {
        Set<Integer> uniqueStores = new HashSet<>();
        long totalSales = 0;
        int franchiseId = key.f0;
        String brand = key.f1;
        int receiptCount = 0;

        // Aggregate data from receipts
        for (ReceiptData receipt : receipts) {
            uniqueStores.add(receipt.getStore_id());
            totalSales += receipt.getTotal_price();
            receiptCount++;
        }

        // Create result
        String currentTime = DATE_FORMAT.format(new Date());
        SalesTotalData result = new SalesTotalData(
            franchiseId,
            brand,
            uniqueStores.size(),
            totalSales,
            currentTime
        );

        LOG.debug("Aggregated {} receipts for franchise {} brand {} - {} stores, total sales: {}",
            receiptCount, franchiseId, brand, uniqueStores.size(), totalSales);

        out.collect(result);
    }
}
