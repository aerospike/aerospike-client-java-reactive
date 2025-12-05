package com.aerospike.client.reactor;

import com.aerospike.client.cluster.Node;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.reactor.util.Args;
import com.aerospike.client.util.Version;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assume.assumeTrue;

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

    @Test
    public void shouldCreateAndDropExpressionBasedIndex() {
        Node node = client.getNodes()[0];
        Version version= node.getVersion();
        assumeTrue(version.isGreaterOrEqual(Version.SERVER_VERSION_8_1));

        List<String> countries = Arrays.asList("Australia", "Canada", "USA");
        Expression exp = Exp.build(
            // IF (age >= 18 AND country IN ["Australia, "Canada", "USA"])
            Exp.cond(
                Exp.and(
                    Exp.ge(
                        Exp.intBin("age"),
                        Exp.val(18)
                    ),
                    Exp.or(
                        Exp.eq(Exp.stringBin("country"), Exp.val(countries.get(0))),
                        Exp.eq(Exp.stringBin("country"), Exp.val(countries.get(1))),
                        Exp.eq(Exp.stringBin("country"), Exp.val(countries.get(2)))
                    )
                ),
                Exp.val(1),
                Exp.unknown()
            )
        );

        Mono<Void> created = reactorClient.createIndex(null, args.namespace, args.set, indexName,
                IndexType.NUMERIC, IndexCollectionType.DEFAULT, exp);

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
