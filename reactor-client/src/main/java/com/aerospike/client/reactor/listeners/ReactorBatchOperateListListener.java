package com.aerospike.client.reactor.listeners;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.listener.BatchOperateListListener;
import reactor.core.publisher.MonoSink;

import java.util.List;

public class ReactorBatchOperateListListener implements BatchOperateListListener {

    private final MonoSink<Boolean> sink;

    public ReactorBatchOperateListListener(MonoSink<Boolean> sink) {
        this.sink = sink;
    }

    @Override
    public void onSuccess(List<BatchRecord> list, boolean b) {
        sink.success(b);
    }

    @Override
    public void onFailure(AerospikeException e) {
        sink.error(e);
    }
}
