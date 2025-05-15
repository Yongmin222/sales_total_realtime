package com.kafka.sales.functions;

import com.kafka.sales.model.ReceiptData;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class FranchiseBrandKeySelector implements KeySelector<ReceiptData, Integer> {
    @Override
    public Integer getKey(ReceiptData receipt) throws Exception {
        return receipt.getFranchise_id();
    }
}
