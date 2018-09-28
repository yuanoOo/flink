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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import java.io.IOException;

/**
 * 封装了工作状态的存储逻辑，包括内存和 RocksDB 两种形式
 *
 * 在Flink中状态按照不同的主题划分，状态也可以分为不同的状态：
 *   1、end-to-end：内部状态、外部状态（sink中的外部数据）
 *   2、state backend：working state（就是应用程序实时交互的状态），state checkpoint
 *
 * A <b>State Backend</b> defines how the state of a streaming application is stored and
 * checkpointed. Different State Backends store their state in different fashions, and use
 * different data structures to hold the state of a running application.
 *
 *
 * 对于MemoryStateBackend来说：
 *   1、working state：TaskManager memory
 *   2、state checkpoint：JobManager memory
 * <p>For example, the {@link org.apache.flink.runtime.state.memory.MemoryStateBackend memory state backend}
 * keeps working state in the memory of the TaskManager and stores checkpoints in the memory of the
 * JobManager. The backend is lightweight and without additional dependencies, but not highly available
 * and supports only small state.
 *
 * 对于FsStateBackend来说：
 *   1、working state：TaskManager memory
 *   2、state checkpoint：filesystem，通常是DFS
 * <p>The {@link org.apache.flink.runtime.state.filesystem.FsStateBackend file system state backend}
 * keeps working state in the memory of the TaskManager and stores state checkpoints in a filesystem
 * (typically a replicated highly-available filesystem, like <a href="https://hadoop.apache.org/">HDFS</a>,
 * <a href="https://ceph.com/">Ceph</a>, <a href="https://aws.amazon.com/documentation/s3/">S3</a>,
 * <a href="https://cloud.google.com/storage/">GCS</a>, etc).
 *
 * 对于RocksDBStateBackend来说：
 *   1、working state：rocksDB
 *   2、state checkpoint：filesystem，通常是DFS
 * <p>The {@code RocksDBStateBackend} stores working state in <a href="http://rocksdb.org/">RocksDB</a>,
 * and checkpoints the state by default to a filesystem (similar to the {@code FsStateBackend}).
 * 
 * <h2>Raw Bytes Storage and Backends</h2>
 * 
 * The {@code StateBackend} creates services for <i>raw bytes storage</i> and for <i>keyed state</i>
 * and <i>operator state</i>.
 *
 * <i>原始字节存储</ i>（通过{@link CheckpointStreamFactory}）是简单地以容错方式存储字节的基本服务。
 * JobManager使用此服务来存储检查点和恢复元数据，并且通常还由键控和操作员状态后端用于存储检查点状态。
 * <p>The <i>raw bytes storage</i> (through the {@link CheckpointStreamFactory}) is the fundamental
 * service that simply stores bytes in a fault tolerant fashion. This service is used by the JobManager
 * to store checkpoint and recovery metadata and is typically also used by the keyed- and operator state
 * backends to store checkpointed state.
 *
 * 由此状态后端创建的{@link AbstractKeyedStateBackend}和{@link OperatorStateBackend}定义了如何保持密钥和运算符的工作状态。
 * 它们还定义了如何检查该状态，经常使用原始字节存储（通过{@code CheckpointStreamFactory}）。
 * 但是，例如，键控状态后端也可以简单地实现到键/值存储的桥接，并且它不需要在检查点上将任何内容存储在原始字节存储器中。
 * <p>The {@link AbstractKeyedStateBackend} and {@link OperatorStateBackend} created by this state
 * backend define how to hold the working state for keys and operators. They also define how to checkpoint
 * that state, frequently using the raw bytes storage (via the {@code CheckpointStreamFactory}).
 * However, it is also possible that for example a keyed state backend simply implements the bridge to
 * a key/value store, and that it does not need to store anything in the raw byte storage upon a
 * checkpoint.
 * 
 * <h2>Serializability</h2>
 * 
 * State Backends need to be {@link java.io.Serializable serializable}, because they distributed
 * across parallel processes (for distributed execution) together with the streaming application code. 
 *
 * 因此，{@code StateBackend}实现（通常是{@link AbstractStateBackend}的子类）意味着像<i> factories </ i>，
 * 它们创建了正确的状态存储，提供对持久存储的访问并保持键控, 和运营商状态数据结构。
 * 这样，State后端可以非常轻量级（仅包含配置），这样可以更容易地进行序列化。
 * <p>Because of that, {@code StateBackend} implementations (typically subclasses
 * of {@link AbstractStateBackend}) are meant to be like <i>factories</i> that create the proper
 * states stores that provide access to the persistent storage and hold the keyed- and operator
 * state data structures. That way, the State Backend can be very lightweight (contain only
 * configurations) which makes it easier to be serializable.
 *
 * <h2>Thread Safety</h2>
 * 状态后端实现必须是线程安全的。, 多个线程可能同时创建流和键控，运营商状态后端。
 * State backend implementations have to be thread-safe. Multiple threads may be creating
 * streams and keyed-/operator state backends concurrently.
 */
@PublicEvolving
public interface StateBackend extends java.io.Serializable {

	// ------------------------------------------------------------------------
	//  Checkpoint storage - the durable persistence of checkpoint data
	// ------------------------------------------------------------------------

	/**
	 * Resolves the given pointer to a checkpoint/savepoint into a checkpoint location. The location
	 * supports reading the checkpoint metadata, or disposing the checkpoint storage location.
	 *
	 * <p>If the state backend cannot understand the format of the pointer (for example because it
	 * was created by a different state backend) this method should throw an {@code IOException}.
	 *
	 * @param externalPointer The external checkpoint pointer to resolve.
	 * @return The checkpoint location handle.
	 *
	 * @throws IOException Thrown, if the state backend does not understand the pointer, or if
	 *                     the pointer could not be resolved due to an I/O error.
	 */
	CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException;

	/**
	 * Creates a storage for checkpoints for the given job. The checkpoint storage is
	 * used to write checkpoint data and metadata.
	 *
	 * @param jobId The job to store checkpoint data for.
	 * @return A checkpoint storage for the given job.
	 *
	 * @throws IOException Thrown if the checkpoint storage cannot be initialized.
	 */
	CheckpointStorage createCheckpointStorage(JobID jobId) throws IOException;

	// ------------------------------------------------------------------------
	//  Structure Backends 
	// ------------------------------------------------------------------------

	/**
	 * Creates a new {@link AbstractKeyedStateBackend} that is responsible for holding <b>keyed state</b>
	 * and checkpointing it. Uses default TTL time provider.
	 *
	 * <p><i>Keyed State</i> is state where each value is bound to a key.
	 *
	 * @param <K> The type of the keys by which the state is organized.
	 *
	 * @return The Keyed State Backend for the given job, operator, and key group range.
	 *
	 * @throws Exception This method may forward all exceptions that occur while instantiating the backend.
	 */
	default <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
			Environment env,
			JobID jobID,
			String operatorIdentifier,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			TaskKvStateRegistry kvStateRegistry) throws Exception {
		return createKeyedStateBackend(
			env,
			jobID,
			operatorIdentifier,
			keySerializer,
			numberOfKeyGroups,
			keyGroupRange,
			kvStateRegistry,
			TtlTimeProvider.DEFAULT);
	}

	/**
	 * Creates a new {@link AbstractKeyedStateBackend} that is responsible for holding <b>keyed state</b>
	 * and checkpointing it.
	 *
	 * <p><i>Keyed State</i> is state where each value is bound to a key.
	 *
	 * @param <K> The type of the keys by which the state is organized.
	 *
	 * @return The Keyed State Backend for the given job, operator, and key group range.
	 *
	 * @throws Exception This method may forward all exceptions that occur while instantiating the backend.
	 */
	<K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
		Environment env,
		JobID jobID,
		String operatorIdentifier,
		TypeSerializer<K> keySerializer,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		TaskKvStateRegistry kvStateRegistry,
		TtlTimeProvider ttlTimeProvider) throws Exception;
	
	/**
	 * Creates a new {@link OperatorStateBackend} that can be used for storing operator state.
	 *
	 * <p>Operator state is state that is associated with parallel operator (or function) instances,
	 * rather than with keys.
	 *
	 * @param env The runtime environment of the executing task.
	 * @param operatorIdentifier The identifier of the operator whose state should be stored.
	 *
	 * @return The OperatorStateBackend for operator identified by the job and operator identifier.
	 *
	 * @throws Exception This method may forward all exceptions that occur while instantiating the backend.
	 */
	OperatorStateBackend createOperatorStateBackend(Environment env, String operatorIdentifier) throws Exception;
}
