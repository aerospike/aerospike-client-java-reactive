package com.aerospike.client.reactor.retry;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.Statement;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.aerospike.client.reactor.retry.RetryFactories.retryOnNoMoreConnections;
import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RetryTest {

    public static final Key KEY = new Key("a", "b", "c");
    public static final Key KEY2 = new Key("a", "b", "c2");
    public static final Key[] KEYS = {KEY, KEY2};
    public static final String[] BIN_NAMES = new String[]{"1", "2"};
    public static final String[] BIN_NAMES2 = new String[]{"3", "4"};

    public static final Bin BIN = new Bin("1", "1");
    public static final Bin BIN2 = new Bin("2", "2");

    public static final List<BatchRead> BATCH = asList(new BatchRead(KEY, BIN_NAMES), new BatchRead(KEY2, BIN_NAMES2));

    private static final String LIST_BIN = "listBin";
    public static final Operation[] OPS = new Operation[]{ListOperation.size(LIST_BIN), ListOperation.getByIndex(LIST_BIN, -1, ListReturnType.VALUE)};

    public static final AerospikeException.Connection NO_CONNECTION = new AerospikeException.Connection(ResultCode.NO_MORE_CONNECTIONS, "1");
    public static final AerospikeException.Timeout TIMEOUT = new AerospikeException.Timeout(new Policy(), false);

    private final IAerospikeReactorClient reactorClient = mock(IAerospikeReactorClient.class);

    private final IAerospikeReactorClient retryClient = new AerospikeReactorRetryClient(reactorClient,
            retryOnNoMoreConnections());

    @Test
    public void shouldRetryGet(){

        when(reactorClient.get(any(), any(Key.class)))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.get(KEY))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryGetWithBinNames(){

        when(reactorClient.get(any(), any(), any(String[].class)))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.get(null, KEY, BIN_NAMES))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryBatchGet(){

        when(reactorClient.get(any(), any(Key[].class)))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.get(KEYS))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryBatch(){

        when(reactorClient.get(any(), ArgumentMatchers.<List<BatchRead>>any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.get(BATCH))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryBatchOperations(){

        when(reactorClient.get(ArgumentMatchers.<BatchPolicy>any(), any(), ArgumentMatchers.<Operation>any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.get(KEYS, OPS))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryGetFlux(){

        when(reactorClient.getFlux(any(), any(Key[].class)))
                .thenReturn(mockFluxErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.getFlux(KEYS))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryBatchFlux(){

        when(reactorClient.getFlux(any(), ArgumentMatchers.<List<BatchRead>>any()))
                .thenReturn(mockFluxErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.getFlux(BATCH))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryBatchFluxOperations(){

        when(reactorClient.getFlux(ArgumentMatchers.<BatchPolicy>any(), any(), ArgumentMatchers.<Operation>any()))
                .thenReturn(mockFluxErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.getFlux(KEYS, OPS))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryGetHeader(){

        when(reactorClient.getHeader(any(), any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.getHeader(KEY))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryGetHeaders(){

        when(reactorClient.getHeaders(any(), any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.getHeaders(KEYS))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryTouch(){

        when(reactorClient.touch(any(), any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.touch(KEY))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryExists(){

        when(reactorClient.exists(any(), any(Key.class)))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.exists(KEY))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryBatchExists(){

        when(reactorClient.exists(any(), any(Key[].class)))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.exists(KEYS))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryExistsFlux(){

        when(reactorClient.existsFlux(any(), any()))
                .thenReturn(mockFluxErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.existsFlux(KEYS))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryPut(){

        when(reactorClient.put(any(), any(), any(Bin[].class)))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.put(KEY, BIN, BIN2))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryAppend(){

        when(reactorClient.append(any(), any(), any(Bin[].class)))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.append(KEY, BIN, BIN2))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryPrepend(){

        when(reactorClient.prepend(any(), any(), any(Bin[].class)))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.prepend(KEY, BIN, BIN2))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryAdd(){

        when(reactorClient.add(any(), any(), any(Bin[].class)))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.add(KEY, BIN, BIN2))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryDelete(){

        when(reactorClient.delete(any(), any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.delete(KEY))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryOperate(){

        when(reactorClient.operate(any(), any(), any(Operation[].class)))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.operate(KEY, Operation.touch(), Operation.delete()))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryQuery(){

        when(reactorClient.query(any(), any()))
                .thenReturn(mockFluxErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.query(new Statement()))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryScanAll(){

        when(reactorClient.scanAll(any(), any(), any(), any()))
                .thenReturn(mockFluxErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.scanAll("namespace", "setname", BIN_NAMES))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryExecute(){

        when(reactorClient.execute(any(), any(), any(), any(),  any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.execute(KEY, "packageName", "functionName"))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryInfo(){

        when(reactorClient.info(any(), any(), anyString()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.info(null, null, "functionName"))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryInfoList(){

        when(reactorClient.info(any(), any(), ArgumentMatchers.<List<String>>any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.info(null, null, asList("functionName", "functionName2")))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryCreateIndex(){

        when(reactorClient.createIndex(any(), anyString(), anyString(), anyString(), anyString(),
                any(IndexType.class), any(IndexCollectionType.class), any(CTX[].class)))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.createIndex(null, "ns", "st", "ind", "bb",
                IndexType.NUMERIC, IndexCollectionType.DEFAULT))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryCreateExpressionIndex(){

        when(reactorClient.createIndex(any(), anyString(), anyString(), anyString(),
                any(IndexType.class), any(IndexCollectionType.class), any(Expression.class)))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.createIndex(null, "ns", "st", "ind",
                        IndexType.NUMERIC, IndexCollectionType.DEFAULT, Expression.fromBase64("")))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryDropIndex(){

        when(reactorClient.dropIndex(any(), any(), any(), any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.dropIndex(null, "ns", "st", "ind"))
                .verifyError(AerospikeException.Timeout.class);
    }

    private <T> Mono<T> mockMonoErrors(Throwable... errors){
        AtomicInteger subscribeCount = new AtomicInteger();
        return Mono.defer(() ->  Mono.error(errors[subscribeCount.getAndIncrement()]));
    }

    private <T> Flux<T> mockFluxErrors(Throwable... errors){
        AtomicInteger subscribeCount = new AtomicInteger();
        return Flux.defer(() ->  Mono.error(errors[subscribeCount.getAndIncrement()]));
    }
}
