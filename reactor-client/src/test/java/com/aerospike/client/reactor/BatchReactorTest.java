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

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.reactor.dto.KeyExists;
import com.aerospike.client.reactor.dto.KeysExists;
import com.aerospike.client.reactor.dto.KeysRecords;
import com.aerospike.client.reactor.util.Args;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class BatchReactorTest extends ReactorTest {
	private static final String LIST_BIN = "listbin";
	private static final String KEY_PREFIX = "batchkey";
	private static final String VALUE_PREFIX = "batchvalue";
	private final String binName = args.getBinName("batchbin");
	private static final int SIZE = 8;
	private Key[] sendKeys;
	private Key[] notSendKeys;

	public BatchReactorTest(Args args) {
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

							if ((i + 1) != 6) {
								return reactorClient.put(policy, key, bin, listBin);
							} else {
								return reactorClient.put(policy, key, new Bin(binName, (i + 1)), listBin);
							}
						}).collect(Collectors.toList()),
				objects -> objects).block();

		notSendKeys = new Key[]{
				new Key(args.namespace, args.set, KEY_PREFIX +"absent1"),
				new Key(args.namespace, args.set, KEY_PREFIX +"absent2")
		};
	}

	@Test
	public void batchExistsArray() {
		Mono<KeysExists> mono = reactorClient.exists(sendKeys);

		StepVerifier.create(mono)
				.expectNextMatches(keysExists -> {
					assertThat(keysExists.exists).hasSameSizeAs(sendKeys).containsOnly(true);
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void batchNotExistsArray() {

		Mono<KeysExists> mono = reactorClient.exists(notSendKeys);

		StepVerifier.create(mono)
				.expectNextMatches(keysExists -> {
					assertThat(keysExists.exists).hasSameSizeAs(notSendKeys).containsOnly(false);
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void batchExistsSequence() {
		Flux<KeyExists> flux = reactorClient.existsFlux(sendKeys);

		StepVerifier.create(flux)
				.recordWith(ArrayList::new)
				.expectNextCount(sendKeys.length)
				.consumeRecordedWith(results -> assertThat(results)
						.extracting(keyExists -> keyExists.exists)
						.containsOnly(true))
				.verifyComplete();
	}

	@Test
	public void batchNotExistsSequence() {
		Flux<KeyExists> flux = reactorClient.existsFlux(notSendKeys);

		StepVerifier.create(flux)
				.recordWith(ArrayList::new)
				.expectNextCount(notSendKeys.length)
				.consumeRecordedWith(results -> assertThat(results)
						.extracting(keyExists -> keyExists.exists)
						.containsOnly(false))
				.verifyComplete();
	}

	@Test
	public void batchGetArray() {
		Mono<KeysRecords> mono = reactorClient.get(sendKeys);

		StepVerifier.create(mono)
				.expectNextMatches(keysRecords -> {
					for (int i = 0; i < keysRecords.records.length; i++) {
						if (i != 5) {
							assertBinEqual(keysRecords.keys[i], keysRecords.records[i], binName, VALUE_PREFIX + (i + 1));
						} else {
							assertBinEqual(keysRecords.keys[i], keysRecords.records[i], binName, i + 1L);
						}
					}
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void batchGetSequence() {
		Flux<KeyRecord> flux = reactorClient.getFlux(sendKeys);

		StepVerifier.create(flux)
				.recordWith(ArrayList::new)
				.expectNextCount(sendKeys.length)
				.consumeRecordedWith(results -> assertThat(results)
						.extracting(keyRecord -> {
							assertRecordFound(keyRecord.key, keyRecord.record);
							assertThat(keyRecord.record.getValue(binName)).isNotNull();
							return true;
						})
						.containsOnly(true))
				.verifyComplete();
	}

	@Test
	public void batchGetNothingSequence() {
		Flux<KeyRecord> flux = reactorClient.getFlux(notSendKeys);

		StepVerifier.create(flux)
				.recordWith(ArrayList::new)
				.expectNextCount(notSendKeys.length)
				.consumeRecordedWith(results -> assertThat(results)
						.extracting(keyRecord -> keyRecord.record)
						.containsOnlyNulls())
				.verifyComplete();
	}

	@Test
	public void batchGetHeaders() {
		Mono<KeysRecords> mono = reactorClient.getHeaders(sendKeys);

		StepVerifier.create(mono)
				.expectNextMatches(keysRecords -> {
					assertThat(keysRecords.records).hasSize(SIZE);
					for (int i = 0; i < keysRecords.records.length; i++) {
						Record record = keysRecords.records[i];
						assertRecordFound(keysRecords.keys[i], record);
						assertThat(record.generation).isGreaterThan(0);
						assertThat(record.expiration).isGreaterThan(0);
					}
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void batchReadComplex() {
		// Batch gets into one call.
		// Batch allows multiple namespaces in one call, but example test environment may only have one namespace.

		// bin * 8
		//Expression exp = Exp.build(Exp.mul(Exp.intBin(binName), Exp.val(8)));
		//Operation[] ops = Operation.array(ExpOperation.read(binName, exp, ExpReadFlags.DEFAULT));

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
		Mono<List<BatchRead>> mono = reactorClient.get(records);

		StepVerifier.create(mono)
				.expectNextMatches(batchReads -> {
					List<BatchRead> recordsFound = batchReads.stream()
							.filter(record -> record.record != null)
							.collect(Collectors.toList());
					assertThat(recordsFound).hasSize(8);
					assertThat(recordsFound).extracting(record -> record.record.getValue(binName))
							.containsExactly(
									"batchvalue1",
									"batchvalue2",
									"batchvalue3",
									//readAllBeans == false
									null,
									"batchvalue5",
									6L,
									"batchvalue7",
									//no bean with name "binnotfound"
									null);

					return true;
				})
				.verifyComplete();
	}

	@Test
	public void batchListOperate() {
		Mono<KeysRecords> mono = reactorClient.get(sendKeys, ListOperation.size(LIST_BIN), ListOperation.getByIndex(LIST_BIN, -1, ListReturnType.VALUE));

		StepVerifier.create(mono)
				.expectNextMatches(keysRecords -> {
					List<Record> recordsFound = Arrays.stream(keysRecords.records)
							.filter(Objects::nonNull)
							.collect(Collectors.toList());
					assertThat(recordsFound).hasSize(8);
					assertThat(recordsFound).extracting(record -> record.getValue(LIST_BIN).toString())
							.containsExactly(
									"[1, 0]",
									"[2, 2]",
									"[3, 6]",
									"[4, 12]",
									"[5, 20]",
									"[6, 30]",
									"[7, 42]",
									"[8, 56]");

					return true;
				})
				.verifyComplete();
	}
}
