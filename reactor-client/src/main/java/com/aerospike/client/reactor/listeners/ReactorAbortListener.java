package com.aerospike.client.reactor.listeners;

import com.aerospike.client.AbortStatus;
import com.aerospike.client.listener.AbortListener;
import reactor.core.publisher.MonoSink;

public class ReactorAbortListener implements AbortListener {

    private final MonoSink<AbortStatus> sink;

    public ReactorAbortListener(MonoSink<AbortStatus> sink) {
        this.sink = sink;
    }

    @Override
    public void onSuccess(AbortStatus abortStatus) {
        sink.success(abortStatus);
    }
}
