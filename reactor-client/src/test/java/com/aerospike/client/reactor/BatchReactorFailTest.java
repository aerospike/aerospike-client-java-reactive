/*
 * Copyright 2012-2018 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.reactor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListReturnType;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.reactor.dto.KeysExists;
import com.aerospike.client.reactor.dto.KeysRecords;
import com.aerospike.client.reactor.util.Args;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class BatchReactorFailTest extends ReactorFailTest {
	private static final String LIST_BIN = "listbin";
	private static final String KEY_PREFIX = "batchkey";
	private static final String VALUE_PREFIX = "batchvalue";
	private final String binName = args.getBinName("batchbin");
	private static final int SIZE = 8;
	private Key[] sendKeys;

	public BatchReactorFailTest(Args args) {
		super(args);
	}

	@Before
	public void fillData() {
		sendKeys = new Key[SIZE];

		WritePolicy policy = new WritePolicy();
		policy.expiration = 2592000;

		Mono.zip(
				IntStream.range(0, SIZE)
						.mapToObj(i -> {
							final Key key = new Key(args.namespace, args.set, KEY_PREFIX + (i + 1));
							sendKeys[i] = key;
							Bin bin = new Bin(binName, VALUE_PREFIX + (i + 1));

							List<Integer> list = new ArrayList<>();

							for (int j = 0; j < (i + 1); j++) {
								list.add(j * (i + 1));
							}

							Bin listBin = new Bin(LIST_BIN, list);

							if (i != 6) {
								return reactorClient.put(policy, key, bin, listBin);
							} else {
								return reactorClient.put(policy, key, new Bin(binName, i + 1), listBin);
							}
						}).collect(Collectors.toList()),
				objects -> objects).block();
	}

	@Test
	public void shouldFailOnBatchExistsArray() {
		Mono<KeysExists> mono = proxyReactorClient.exists(strictBatchPolicy(), sendKeys);

		StepVerifier.create(mono)
				.expectError(AerospikeException.Timeout.class)
				.verify();
	}

	@Test
	public void shouldFailOnBatchExistsSequence() {

		Flux<KeyRecord> flux = proxyReactorClient.getFlux(strictBatchPolicy(), sendKeys);

		StepVerifier.create(flux)
				.expectError(AerospikeException.Timeout.class)
				.verify();
	}

	@Test
	public void shouldFailOnBatchGetArray() {
		Mono<KeysRecords> mono = proxyReactorClient.get(strictBatchPolicy(), sendKeys);

		StepVerifier.create(mono)
				.expectError(AerospikeException.Timeout.class)
				.verify();
	}

	@Test
	public void shouldFailOnBatchGetSequence() {
		Flux<KeyRecord> flux = proxyReactorClient.getFlux(strictBatchPolicy(), sendKeys);

		StepVerifier.create(flux)
				.expectError(AerospikeException.Timeout.class)
				.verify();
	}

	@Test
	public void shouldFailOnBatchGetHeaders() {
		Mono<KeysRecords> mono = proxyReactorClient.getHeaders(strictBatchPolicy(), sendKeys);

		StepVerifier.create(mono)
				.expectError(AerospikeException.Timeout.class)
				.verify();
	}

	@Test
	public void shouldFailOnBatchReadComplex() {
		// Batch gets into one call.
		// Batch allows multiple namespaces in one call, but example test environment may only have one namespace.
		String[] bins = new String[] {binName};
		List<BatchRead> records = new ArrayList<>();
		records.add(new BatchRead(new Key(args.namespace, args.set, KEY_PREFIX + 1), bins));
		records.add(new BatchRead(new Key(args.namespace, args.set, KEY_PREFIX + 2), true));
		records.add(new BatchRead(new Key(args.namespace, args.set, KEY_PREFIX + 3), true));
		records.add(new BatchRead(new Key(args.namespace, args.set, KEY_PREFIX + 4), false));
		records.add(new BatchRead(new Key(args.namespace, args.set, KEY_PREFIX + 5), true));
		records.add(new BatchRead(new Key(args.namespace, args.set, KEY_PREFIX + 6), true));
		records.add(new BatchRead(new Key(args.namespace, args.set, KEY_PREFIX + 7), bins));

		// This record should be found, but the requested bin will not be found.
		records.add(new BatchRead(new Key(args.namespace, args.set, KEY_PREFIX + 8), new String[] {"binnotfound"}));

		// This record should not be found.
		records.add(new BatchRead(new Key(args.namespace, args.set, "keynotfound"), bins));

		// Execute batch.
		Mono<List<BatchRead>> mono = proxyReactorClient.get(strictBatchPolicy(), records);

		StepVerifier.create(mono)
				.expectError(AerospikeException.Timeout.class)
				.verify();

	}

	@Test
	public void shouldFailOnBatchListOperate() {
		Mono<KeysRecords> mono = proxyReactorClient.get(strictBatchPolicy(), sendKeys, ListOperation.size(LIST_BIN), ListOperation.getByIndex(LIST_BIN, -1, ListReturnType.VALUE));

		StepVerifier.create(mono)
				.expectError(AerospikeException.Timeout.class)
				.verify();
	}
}
