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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

import java.io.IOException;

/**
 * 一个TaskManager中可能同时运行着很多任务实例，有时某些任务需要消费某远程任务所生产的结果分区，
 * 有时某些任务可能会生产结果分区供其他任务消费。所以对一个TaskManager来说，其职责并非单一的，
 * 它既可能充当客户端的角色也可能充当服务端角色。因此，一个NettyConnectionManager会同时管理着
 * 一个Netty客户端（NettyClient）和一个Netty服务器（NettyServer）实例。
 *
 * 当然除此之外还有一个Netty缓冲池（NettyBufferPool）以及一个分区请求客户端工厂
 * （PartitionRequestClientFactory，用于创建分区请求客户端PartitionRequestClient），
 * 这些对象都在NettyConnectionManager构造器中被初始化。
 *
 * Netty客户端和服务器对象的启动和停止都是由NettyConnectionManager统一控制的。
 *
 * NettyConnectionManager启动的时机是当TaskManager跟JobManager关联上之后调用
 * NetworkEnvironment的associateWithTaskManagerAndJobManager方法时。而当
 * TaskManager跟JobManager解除关联时停止。
 */
public class NettyConnectionManager implements ConnectionManager {

	private final NettyServer server;

	private final NettyClient client;

	/**
	 * NettyClient和NettyServer在实例化Netty通信的核心对象。
	 * 都需要配置各自的“字节缓冲分配器”用于为Netty读写数据分配内存单元。
	 * Netty自身提供了一个池化的字节缓冲分配器（PooledByteBufAllocator），但Flink又在此基础上进行了包装并提供了Netty缓冲池（NettyBufferPool）。
	 * 此举的目的是严格控制所创建的分配器（Arena）的个数，转而依赖TaskManager的相关配置指定。
	 */
	private final NettyBufferPool bufferPool;

	private final PartitionRequestClientFactory partitionRequestClientFactory;

	public NettyConnectionManager(NettyConfig nettyConfig) {
		this.server = new NettyServer(nettyConfig);
		this.client = new NettyClient(nettyConfig);
		this.bufferPool = new NettyBufferPool(nettyConfig.getNumberOfArenas());

		this.partitionRequestClientFactory = new PartitionRequestClientFactory(client);
	}

	/**
	 * NettyConnectionManager启动的时机是当TaskManager跟JobManager关联上之后调用
	 * NetworkEnvironment的associateWithTaskManagerAndJobManager方法时。
	 *
	 * @param partitionProvider
	 * @param taskEventDispatcher
	 * @throws IOException
	 */
	@Override
	public void start(ResultPartitionProvider partitionProvider, TaskEventDispatcher taskEventDispatcher) throws IOException {
		NettyProtocol partitionRequestProtocol = new NettyProtocol(
			partitionProvider,
			taskEventDispatcher,
			client.getConfig().isCreditBasedEnabled());

		client.init(partitionRequestProtocol, bufferPool);
		server.init(partitionRequestProtocol, bufferPool);
	}

	@Override
	public PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId)
			throws IOException, InterruptedException {
		return partitionRequestClientFactory.createPartitionRequestClient(connectionId);
	}

	@Override
	public void closeOpenChannelConnections(ConnectionID connectionId) {
		partitionRequestClientFactory.closeOpenChannelConnections(connectionId);
	}

	@Override
	public int getNumberOfActiveConnections() {
		return partitionRequestClientFactory.getNumberOfActiveClients();
	}

	@Override
	public int getDataPort() {
		if (server != null && server.getLocalAddress() != null) {
			return server.getLocalAddress().getPort();
		} else {
			return -1;
		}
	}

	@Override
	public void shutdown() {
		client.shutdown();
		server.shutdown();
	}

	NettyClient getClient() {
		return client;
	}

	NettyServer getServer() {
		return server;
	}

	NettyBufferPool getBufferPool() {
		return bufferPool;
	}
}
