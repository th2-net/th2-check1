/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.check1;

import com.exactpro.th2.check1.utils.TimeUtilsKt;
import com.exactpro.th2.common.grpc.Checkpoint;
import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.util.Objects;

public class CheckpointData {
	private final Long sequence;
	private final Timestamp timestamp;

	public CheckpointData(Long sequence, Timestamp timestamp) {
		this.sequence = sequence;
		this.timestamp = timestamp;
	}

	public CheckpointData(Long sequence) {
		this(sequence, null);
	}

	public Long getSequence() {
		return sequence;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public Instant getTimestampAsInstant() {
		return TimeUtilsKt.toInstant(timestamp);
	}

	public static CheckpointData convert(com.exactpro.th2.common.grpc.Checkpoint.CheckpointData checkpointData) {
		return new CheckpointData(checkpointData.getSequence(), checkpointData.getTimestamp());
	}

	public static com.exactpro.th2.common.grpc.Checkpoint.CheckpointData convert(CheckpointData checkpointData) {
		Checkpoint.CheckpointData.Builder builder = Checkpoint.CheckpointData.newBuilder()
				.setSequence(checkpointData.getSequence());
		if (checkpointData.getTimestamp() != null)
			builder.setTimestamp(checkpointData.getTimestamp());

		return builder.build();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		CheckpointData that = (CheckpointData) o;
		return Objects.equals(sequence, that.sequence) && Objects.equals(timestamp, that.timestamp);
	}

	@Override
	public int hashCode() {
		return Objects.hash(sequence, timestamp);
	}
}
