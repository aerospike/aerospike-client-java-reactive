package com.aerospike.client.reactor.listeners;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchResults;
import com.aerospike.client.listener.BatchRecordArrayListener;
import reactor.core.publisher.MonoSink;

public class ReactorBatchRecordArrayListener implements BatchRecordArrayListener {

    private final MonoSink<BatchResults> sink;

    public ReactorBatchRecordArrayListener(MonoSink<BatchResults> sink) {
        this.sink = sink;
    }

    @Override
    public void onSuccess(BatchRecord[] batchRecords, boolean b) {
        sink.success(new BatchResults(batchRecords, b));
    }

    @Override
    public void onFailure(BatchRecord[] batchRecords, AerospikeException e) {
        sink.error(e);
    }
}
