/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;

import java.io.IOException;

/**
 *
 * 为了使得记录以及事件能够被写入Buffer随后在消费时再从Buffer中读出，
 * Flink提供了记录序列化器（RecordSerializer）与反序列化器（RecordDeserializer）以及事件序列化器（EventSerializer）。
 *
 * Interface for turning records into sequences of memory segments.
 */
public interface RecordSerializer<T extends IOReadableWritable> {

	/**
	 * Status of the serialization result.
	 */
	enum SerializationResult {
		PARTIAL_RECORD_MEMORY_SEGMENT_FULL(false, true),
		FULL_RECORD_MEMORY_SEGMENT_FULL(true, true),
		FULL_RECORD(true, false);

		private final boolean isFullRecord;

		private final boolean isFullBuffer;

		SerializationResult(boolean isFullRecord, boolean isFullBuffer) {
			this.isFullRecord = isFullRecord;
			this.isFullBuffer = isFullBuffer;
		}

		/**
		 * Whether the full record was serialized and completely written to
		 * a target buffer.
		 *
		 * @return <tt>true</tt> if the complete record was written
		 */
		public boolean isFullRecord() {
			return this.isFullRecord;
		}

		/**
		 * Whether the target buffer is full after the serialization process.
		 *
		 * @return <tt>true</tt> if the target buffer is full
		 */
		public boolean isFullBuffer() {
			return this.isFullBuffer;
		}
	}

	/**
	 * 向序列化器中加入记录，加入的记录会被序列化并存入到序列化器内部的Buffer中
	 *
	 * Starts serializing and copying the given record to the target buffer
	 * (if available).
	 *
	 * @param record the record to serialize
	 * @return how much information was written to the target buffer and
	 *         whether this buffer is full
	 */
	SerializationResult addRecord(T record) throws IOException;

	/**
	 * Sets a (next) target buffer to use and continues writing remaining data
	 * to it until it is full.
	 *
	 * @param bufferBuilder the new target buffer to use
	 * @return how much information was written to the target buffer and
	 *         whether this buffer is full
	 */
	SerializationResult continueWritingWithNextBufferBuilder(BufferBuilder bufferBuilder) throws IOException;

	/**
	 * Clear and release internal state.
	 */
	void clear();

	/**
	 * @return <tt>true</tt> if has some serialized data pending copying to the result {@link BufferBuilder}.
	 */
	boolean hasSerializedData();
}
