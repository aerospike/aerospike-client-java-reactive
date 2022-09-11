package com.aerospike.client.reactor;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.reactor.util.Args;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class ReactorIndexTest extends ReactorTest {

    public ReactorIndexTest(Args args) {
        super(args);
    }

    private static final String indexName = "rintindxtsts";
    private static final String binName = "rintbin";

    @Before
    public void before() {
        Mono<Void> dropped = reactorClient.dropIndex(null, args.namespace, args.set, indexName)
                .onErrorResume(throwable -> true, throwable -> Mono.empty());

        StepVerifier.create(dropped)
                .verifyComplete();
    }

    @Test
    public void shouldCreateAndDropIndex() {
        Mono<Void> created = reactorClient.createIndex(null, args.namespace, args.set, indexName,
                args.getBinName(binName), IndexType.NUMERIC, IndexCollectionType.DEFAULT);

        StepVerifier.create(created)
                .verifyComplete();

        Mono<Void> dropped = reactorClient.dropIndex(null, args.namespace, args.set, indexName);
        StepVerifier.create(dropped)
                .verifyComplete();
    }

    /**
     * Starting at Aerospike server version 6.1.0.1:
     * Attempting to create a secondary index which already exists now returns success/OK instead of an error.
     */
    @Test
    public void shouldNotFailCreateIndexIfAlreadyExists() {
        Mono<Void> created = reactorClient.createIndex(null, args.namespace, args.set, indexName,
                args.getBinName(binName), IndexType.NUMERIC, IndexCollectionType.DEFAULT);

        StepVerifier.create(created)
                .verifyComplete();

        created = reactorClient.createIndex(null, args.namespace, args.set, indexName,
                args.getBinName(binName), IndexType.NUMERIC, IndexCollectionType.DEFAULT);

        StepVerifier.create(created)
                .verifyComplete();

        reactorClient.dropIndex(null, args.namespace, args.set, indexName).subscribe();
    }

    @Test
    public void shouldNotFailDropIndexIfNotExists() {
        Mono<Void> created = reactorClient.dropIndex(null, args.namespace, args.set, indexName);

        StepVerifier.create(created)
                .verifyComplete();
    }
}
