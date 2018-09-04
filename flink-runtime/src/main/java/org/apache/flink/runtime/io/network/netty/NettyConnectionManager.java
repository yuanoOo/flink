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

	/**
	 * 跟NettyClient一样，NettyServer也会初始化Netty服务端的核心对象，
	 * 除此之外它会启动对特定端口的侦听并准备接收客户端发起的请求。
	 * 下面是NettyServer的初始化与启动步骤：
	 *     1、创建ServerBootstrap实例来引导绑定和启动服务器： bootstrap = new ServerBootstrap();
	 *     2、根据配置创建NioEventLoopGroup或EpollEventLoopGroup对象来处理事件，如接收新连接、接收数据、写数据等等：
	 *     3、指定InetSocketAddress，服务器监听此端口：
	 *        	bootstrap.localAddress(config.getServerAddress(), config.getServerPort());
	 *     4、进行各种参数配置，设置childHandler执行所有的连接请求：
	 *     5、都设置完毕了，最后调用ServerBootstrap.bind()方法来绑定服务器：
	 *          bindFuture = bootstrap.bind().syncUninterruptibly();
	 */
	private final NettyServer server;

	/**
	 * NettyClient的主要职责是初始化Netty客户端的核心对象，并根据NettyProtocol配置用于客户端事件处理的ChannelPipeline。
	 *
	 * 一个Netty引导客户端的创建步骤如下：
	 *    1、创建Bootstrap对象用来引导启动客户端：bootstrap = new Bootstrap();
	 *    2、创建NioEventLoopGroup或EpollEventLoopGroup对象并设置到Bootstrap中，
	 *       EventLoopGroup可以理解为是一个线程池，用来处理连接、接收数据、发送数据：
	 *	  3、进行一系列配置，并设置ChannelHandler用来处理逻辑：
	 *	  4、调用Bootstrap.connect()来连接服务器：
	 *	       return bootstrap.connect(serverSocketAddress);
	 *
	 *
	 * 需要注意的是，一个TaskManager根本上只会存在一个NettyClient对象（对应的也只有一个Bootstrap实例）。
	 * 但一个TaskManager中的子任务实例很有可能会跟多个不同的远程TaskManager通信，所以同一个Bootstrap
	 * 实例可能会跟多个目标服务器建立连接，所以它是复用的，这一点不存在问题因为无论跟哪个目标服务器通信，
	 * Bootstrap的配置都是不变的。至于不同的RemoteChannel如何跟某个连接建立对应关系，这一点由
	 * PartitionRequestClientFactory来保证。
	 */
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
