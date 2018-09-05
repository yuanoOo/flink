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

package org.apache.flink.runtime.io.network;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManager.IOMode;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.query.KvStateClientProxy;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.KvStateServer;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 *
 * 网络环境（NetworkEnvironment）是TaskManager进行网络通信的主对象，主要用于跟踪中间结果并负责所有的数据交换。
 * 每个TaskManager的实例都包含一个网络环境对象，在TaskManager启动时创建。
 * NetworkEnvironment管理着多个协助通信的关键部件，它们是：
 *
 *    NetworkBufferPool：网络缓冲池，负责申请一个TaskManager的所有的内存段用作缓冲池；
 *    ConnectionManager：连接管理器，用于管理本地（远程）通信连接；
 *    ResultPartitionManager：结果分区管理器，用于跟踪一个TaskManager上所有生产/消费相关的ResultPartition；
 *    TaskEventDispatcher：任务事件分发器，从消费者任务分发事件给生产者任务；
 *    ResultPartitionConsumableNotifier：结果分区可消费通知器，用于通知消费者生产者生产的结果分区可消费；
 *    PartitionStateChecker：分区状态检查器，用于检查分区状态；
 *
 * NetworkEnvironment被初始化时，它首先根据配置创建网络缓冲池（NetworkBufferPool）。
 *  创建NetworkBufferPool时需要指定Buffer数目、单个Buffer的大小以及Buffer所基于的内存类型，
 *  这些信息都是可配置的并封装在配置对象NetworkEnvironmentConfiguration中。
 *
 * NetworkEnvironment对象包含了上面列举的网络I/O相关的各种部件，这些对象并不随着NetworkEnvironment对象实例化而被立即实例化，
 * 它们的实例化会被延后到NetworkEnvironment对象跟TaskManager以及JobManager**关联**（associate）上之后。
 * TaskManager在启动后会向JobManager注册，随后NetworkEnvironment的associateWithTaskManagerAndJobManager方法会得到调用，
 * 在其中所有的辅助部件都会得到实例化：
 *
 *
 * Network I/O components of each {@link TaskManager} instance. The network environment contains
 * the data structures that keep track of all intermediate results and all data exchanges.
 */
public class NetworkEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(NetworkEnvironment.class);

	private final Object lock = new Object();

	private final NetworkBufferPool networkBufferPool;

	/**
	 * Netty连接管理器（NettyConnectionManager）是连接管理器接口（ConnectionManager）针对
	 * 基于Netty的远程连接管理的实现者。它是TaskManager中负责网络通信的网络环境对象（NetworkEnvironment）
	 * 的核心部件之一。
	 */
	private final ConnectionManager connectionManager;

	private final ResultPartitionManager resultPartitionManager;

	private final TaskEventDispatcher taskEventDispatcher;

	/** Server for {@link InternalKvState} requests. */
	private KvStateServer kvStateServer;

	/** Proxy for the queryable state client. */
	private KvStateClientProxy kvStateProxy;

	/** Registry for {@link InternalKvState} instances. */
	private final KvStateRegistry kvStateRegistry;

	private final IOManager.IOMode defaultIOMode;

	private final int partitionRequestInitialBackoff;

	private final int partitionRequestMaxBackoff;

	/** Number of network buffers to use for each outgoing/incoming channel (subpartition/input channel). */
	private final int networkBuffersPerChannel;

	/** Number of extra network buffers to use for each outgoing/incoming gate (result partition/input gate). */
	private final int extraNetworkBuffersPerGate;

	private final boolean enableCreditBased;

	private boolean isShutdown;

	public NetworkEnvironment(
			NetworkBufferPool networkBufferPool,
			ConnectionManager connectionManager,
			ResultPartitionManager resultPartitionManager,
			TaskEventDispatcher taskEventDispatcher,
			KvStateRegistry kvStateRegistry,
			KvStateServer kvStateServer,
			KvStateClientProxy kvStateClientProxy,
			IOMode defaultIOMode,
			int partitionRequestInitialBackoff,
			int partitionRequestMaxBackoff,
			int networkBuffersPerChannel,
			int extraNetworkBuffersPerGate,
			boolean enableCreditBased) {

		this.networkBufferPool = checkNotNull(networkBufferPool);
		this.connectionManager = checkNotNull(connectionManager);
		this.resultPartitionManager = checkNotNull(resultPartitionManager);
		this.taskEventDispatcher = checkNotNull(taskEventDispatcher);
		this.kvStateRegistry = checkNotNull(kvStateRegistry);

		this.kvStateServer = kvStateServer;
		this.kvStateProxy = kvStateClientProxy;

		this.defaultIOMode = defaultIOMode;

		this.partitionRequestInitialBackoff = partitionRequestInitialBackoff;
		this.partitionRequestMaxBackoff = partitionRequestMaxBackoff;

		isShutdown = false;
		this.networkBuffersPerChannel = networkBuffersPerChannel;
		this.extraNetworkBuffersPerGate = extraNetworkBuffersPerGate;

		this.enableCreditBased = enableCreditBased;
	}

	// --------------------------------------------------------------------------------------------
	//  Properties
	// --------------------------------------------------------------------------------------------

	public ResultPartitionManager getResultPartitionManager() {
		return resultPartitionManager;
	}

	public TaskEventDispatcher getTaskEventDispatcher() {
		return taskEventDispatcher;
	}

	public ConnectionManager getConnectionManager() {
		return connectionManager;
	}

	public NetworkBufferPool getNetworkBufferPool() {
		return networkBufferPool;
	}

	public IOMode getDefaultIOMode() {
		return defaultIOMode;
	}

	public int getPartitionRequestInitialBackoff() {
		return partitionRequestInitialBackoff;
	}

	public int getPartitionRequestMaxBackoff() {
		return partitionRequestMaxBackoff;
	}

	public boolean isCreditBased() {
		return enableCreditBased;
	}

	public KvStateRegistry getKvStateRegistry() {
		return kvStateRegistry;
	}

	public KvStateServer getKvStateServer() {
		return kvStateServer;
	}

	public KvStateClientProxy getKvStateProxy() {
		return kvStateProxy;
	}

	public TaskKvStateRegistry createKvStateTaskRegistry(JobID jobId, JobVertexID jobVertexId) {
		return kvStateRegistry.createTaskRegistry(jobId, jobVertexId);
	}

	// --------------------------------------------------------------------------------------------
	//  Task operations
	// --------------------------------------------------------------------------------------------

	/**
	 * 在任务执行的核心逻辑中，有一个步骤是需要将自身（Task）注册到网络栈（也就是这里的NetworkEnvironment）。
	 * 该步骤会调用NetworkEnvironment的实例方法registerTask进行注册，注册之后NetworkEnvironment会对任务的通信进行管理：
	 *
	 *
	 * NetworkEnvironment对象会为当前任务生产端的每个ResultPartition都创建本地缓冲池，缓冲池中的Buffer数为结果分区的子分区数，
	 * 同时为当前任务消费端的InputGate创建本地缓冲池，缓冲池的Buffer数为InputGate所包含的输入信道数。这些缓冲池都是非固定大小的，
	 * 也就是说他们会按照网络缓冲池内存段的使用情况进行重平衡。
	 * @param task
	 * @throws IOException
	 */
	public void registerTask(Task task) throws IOException {
		// 获得当前任务对象所生产的结果分区集合
		final ResultPartition[] producedPartitions = task.getProducedPartitions();

		synchronized (lock) {
			if (isShutdown) {
				throw new IllegalStateException("NetworkEnvironment is shut down");
			}

			// 遍历任务的每个结果分区，依次进行初始化
			for (final ResultPartition partition : producedPartitions) {
				setupPartition(partition);
			}

			// Setup the buffer pool for each buffer reader
			final SingleInputGate[] inputGates = task.getAllInputGates();
			for (SingleInputGate gate : inputGates) {
				setupInputGate(gate);
			}
		}
	}

	@VisibleForTesting
	public void setupPartition(ResultPartition partition) throws IOException {
		BufferPool bufferPool = null;

		try {
			int maxNumberOfMemorySegments = partition.getPartitionType().isBounded() ?
				partition.getNumberOfSubpartitions() * networkBuffersPerChannel +
					extraNetworkBuffersPerGate : Integer.MAX_VALUE;

			// 用网络缓冲池创建本地缓冲池，该缓冲池是非固定大小的且请求的缓冲个数是结果分区的子分区个数
			bufferPool = networkBufferPool.createBufferPool(partition.getNumberOfSubpartitions(),
				maxNumberOfMemorySegments);

			// 将本地缓冲池注册到结果分区
			partition.registerBufferPool(bufferPool);

			// 结果分区会被注册到结果分区管理器
			resultPartitionManager.registerResultPartition(partition);
		} catch (Throwable t) {
			if (bufferPool != null) {
				bufferPool.lazyDestroy();
			}

			if (t instanceof IOException) {
				throw (IOException) t;
			} else {
				throw new IOException(t.getMessage(), t);
			}
		}

		// 向任务事件分发器注册结果分区写入器
		taskEventDispatcher.registerPartition(partition.getPartitionId());
	}

	@VisibleForTesting
	public void setupInputGate(SingleInputGate gate) throws IOException {
		BufferPool bufferPool = null;
		int maxNumberOfMemorySegments;
		try {
			if (enableCreditBased) {
				maxNumberOfMemorySegments = gate.getConsumedPartitionType().isBounded() ?
					extraNetworkBuffersPerGate : Integer.MAX_VALUE;

				// assign exclusive buffers to input channels directly and use the rest for floating buffers
				gate.assignExclusiveSegments(networkBufferPool, networkBuffersPerChannel);
				bufferPool = networkBufferPool.createBufferPool(0, maxNumberOfMemorySegments);
			} else {
				maxNumberOfMemorySegments = gate.getConsumedPartitionType().isBounded() ?
					gate.getNumberOfInputChannels() * networkBuffersPerChannel +
						extraNetworkBuffersPerGate : Integer.MAX_VALUE;

				bufferPool = networkBufferPool.createBufferPool(gate.getNumberOfInputChannels(),
					maxNumberOfMemorySegments);
			}
			gate.setBufferPool(bufferPool);
		} catch (Throwable t) {
			if (bufferPool != null) {
				bufferPool.lazyDestroy();
			}

			ExceptionUtils.rethrowIOException(t);
		}
	}

	public void unregisterTask(Task task) {
		LOG.debug("Unregister task {} from network environment (state: {}).",
				task.getTaskInfo().getTaskNameWithSubtasks(), task.getExecutionState());

		final ExecutionAttemptID executionId = task.getExecutionId();

		synchronized (lock) {
			if (isShutdown) {
				// no need to do anything when we are not operational
				return;
			}

			if (task.isCanceledOrFailed()) {
				resultPartitionManager.releasePartitionsProducedBy(executionId, task.getFailureCause());
			}

			for (ResultPartition partition : task.getProducedPartitions()) {
				taskEventDispatcher.unregisterPartition(partition.getPartitionId());
				partition.destroyBufferPool();
			}

			final SingleInputGate[] inputGates = task.getAllInputGates();

			if (inputGates != null) {
				for (SingleInputGate gate : inputGates) {
					try {
						if (gate != null) {
							gate.releaseAllResources();
						}
					}
					catch (IOException e) {
						LOG.error("Error during release of reader resources: " + e.getMessage(), e);
					}
				}
			}
		}
	}

	// 由TaskManagerServices进行启动
	public void start() throws IOException {
		synchronized (lock) {
			Preconditions.checkState(!isShutdown, "The NetworkEnvironment has already been shut down.");

			LOG.info("Starting the network environment and its components.");

			try {
				LOG.debug("Starting network connection manager");
				connectionManager.start(resultPartitionManager, taskEventDispatcher);
			} catch (IOException t) {
				throw new IOException("Failed to instantiate network connection manager.", t);
			}

			if (kvStateServer != null) {
				try {
					kvStateServer.start();
				} catch (Throwable ie) {
					kvStateServer.shutdown();
					kvStateServer = null;
					throw new IOException("Failed to start the Queryable State Data Server.", ie);
				}
			}

			if (kvStateProxy != null) {
				try {
					kvStateProxy.start();
				} catch (Throwable ie) {
					kvStateProxy.shutdown();
					kvStateProxy = null;
					throw new IOException("Failed to start the Queryable State Client Proxy.", ie);
				}
			}
		}
	}

	/**
	 * Tries to shut down all network I/O components.
	 */
	public void shutdown() {
		synchronized (lock) {
			if (isShutdown) {
				return;
			}

			LOG.info("Shutting down the network environment and its components.");

			if (kvStateProxy != null) {
				try {
					LOG.debug("Shutting down Queryable State Client Proxy.");
					kvStateProxy.shutdown();
				} catch (Throwable t) {
					LOG.warn("Cannot shut down Queryable State Client Proxy.", t);
				}
			}

			if (kvStateServer != null) {
				try {
					LOG.debug("Shutting down Queryable State Data Server.");
					kvStateServer.shutdown();
				} catch (Throwable t) {
					LOG.warn("Cannot shut down Queryable State Data Server.", t);
				}
			}

			// terminate all network connections
			try {
				LOG.debug("Shutting down network connection manager");
				connectionManager.shutdown();
			}
			catch (Throwable t) {
				LOG.warn("Cannot shut down the network connection manager.", t);
			}

			// shutdown all intermediate results
			try {
				LOG.debug("Shutting down intermediate result partition manager");
				resultPartitionManager.shutdown();
			}
			catch (Throwable t) {
				LOG.warn("Cannot shut down the result partition manager.", t);
			}

			taskEventDispatcher.clearAll();

			// make sure that the global buffer pool re-acquires all buffers
			networkBufferPool.destroyAllBufferPools();

			// destroy the buffer pool
			try {
				networkBufferPool.destroy();
			}
			catch (Throwable t) {
				LOG.warn("Network buffer pool did not shut down properly.", t);
			}

			isShutdown = true;
		}
	}

	public boolean isShutdown() {
		synchronized (lock) {
			return isShutdown;
		}
	}
}
