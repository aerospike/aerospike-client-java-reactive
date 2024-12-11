package com.aerospike.client.reactor.listeners;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.CommitStatus;
import com.aerospike.client.listener.CommitListener;
import reactor.core.publisher.MonoSink;

public class ReactorCommitListener implements CommitListener {

    private final MonoSink<CommitStatus> sink;

    public ReactorCommitListener(MonoSink<CommitStatus> sink) {
        this.sink = sink;
    }

    @Override
    public void onSuccess(CommitStatus commitStatus) {
        sink.success(commitStatus);
    }

    @Override
    public void onFailure(AerospikeException.Commit commit) {
        sink.error(commit);
    }
}
