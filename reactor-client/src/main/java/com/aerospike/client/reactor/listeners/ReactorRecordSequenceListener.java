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
package com.aerospike.client.reactor.listeners;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.query.KeyRecord;
import reactor.core.publisher.FluxSink;

public class ReactorRecordSequenceListener implements RecordSequenceListener {

	private final FluxSink<KeyRecord> sink;

	public ReactorRecordSequenceListener(FluxSink<KeyRecord> sink) {
		this.sink = sink;
	}

	@Override
	public void onRecord(Key key, Record record) throws AerospikeException {
		sink.next(new KeyRecord(key, record));
	}

	@Override
	public void onSuccess() {
		sink.complete();
	}

	@Override
	public void onFailure(AerospikeException exception) {
		sink.error(exception);
	}
}
