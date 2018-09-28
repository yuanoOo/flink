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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Public;

import java.io.IOException;
import java.io.OutputStream;

/**
 * 通过{@link FileSystem}创建的文件的输出流。
 * An output stream to a file that is created via a {@link FileSystem}.
 * This class extends the base {@link java.io.OutputStream} with some additional important methods.
 *
 * 数据持久性保证
 * <h2>Data Persistence Guarantees</h2>
 *
 * 这些流用于持久存储数据，既用于流应用程序的结果，也用于容错和恢复。因此，必须明确定义这些流的持久性语义。
 * <p>These streams are used to persistently store data, both for results of streaming applications
 * and for fault tolerance and recovery. It is therefore crucial that the persistence semantics
 * of these streams are well defined.
 *
 * 请参阅{@link FileSystem}的类级文档，了解通过Flink的FileSystem抽象和{@code FSDataOutputStream}定义数据持久性。
 * <p>Please refer to the class-level docs of {@link FileSystem} for the definition of data persistence
 * via Flink's FileSystem abstraction and the {@code FSDataOutputStream}.
 *
 * 线程安全
 * <h2>Thread Safety</h2>
 *
 * 通常不认为{@code FSDataOutputStream}的实现是线程安全的。
 * 不应在线程之间传递{@code FSDataOutputStream}的实例，因为无法保证跨线程的操作可见性顺序。
 * <p>Implementations of the {@code FSDataOutputStream} are generally not assumed to be thread safe.
 * Instances of {@code FSDataOutputStream} should not be passed between threads, because there
 * are no guarantees about the order of visibility of operations across threads.
 *
 * @see FileSystem
 * @see FSDataInputStream
 */
@Public
public abstract class FSDataOutputStream extends OutputStream {

	/**
	 * Gets the position of the stream (non-negative), defined as the number of bytes
	 * from the beginning of the file to the current writing position. The position
	 * corresponds to the zero-based index of the next byte that will be written.
	 *
	 * <p>This method must report accurately report the current position of the stream.
	 * Various components of the high-availability and recovery logic rely on the accurate
	 *
	 * @return The current position in the stream, defined as the number of bytes
	 *         from the beginning of the file to the current writing position.
	 *
	 * @throws IOException Thrown if an I/O error occurs while obtaining the position from
	 *                     the stream implementation.
	 */
	public abstract long getPos() throws IOException;

	/**
	 * Flushes the stream, writing any data currently buffered in stream implementation
	 * to the proper output stream. After this method has been called, the stream implementation
	 * must not hold onto any buffered data any more.
	 *
	 * <p>A completed flush does not mean that the data is necessarily persistent. Data
	 * persistence can is only assumed after calls to {@link #close()} or {@link #sync()}.
	 *
	 * <p>Implementation note: This overrides the method defined in {@link OutputStream}
	 * as abstract to force implementations of the {@code FSDataOutputStream} to implement
	 * this method directly.
	 *
	 * @throws IOException Thrown if an I/O error occurs while flushing the stream.
	 */
	public abstract void flush() throws IOException;

	/**
	 * Flushes the data all the way to the persistent non-volatile storage (for example disks).
	 * The method behaves similar to the <i>fsync</i> function, forcing all data to
	 * be persistent on the devices.
	 *
	 * @throws IOException Thrown if an I/O error occurs
	 */
	public abstract void sync() throws IOException;

	/**
	 * Closes the output stream. After this method returns, the implementation must guarantee
	 * that all data written to the stream is persistent/visible, as defined in the
	 * {@link FileSystem class-level docs}.
	 *
	 * <p>The above implies that the method must block until persistence can be guaranteed.
	 * For example for distributed replicated file systems, the method must block until the
	 * replication quorum has been reached. If the calling thread is interrupted in the
	 * process, it must fail with an {@code IOException} to indicate that persistence cannot
	 * be guaranteed.
	 *
	 * <p>If this method throws an exception, the data in the stream cannot be assumed to be
	 * persistent.
	 *
	 * <p>Implementation note: This overrides the method defined in {@link OutputStream}
	 * as abstract to force implementations of the {@code FSDataOutputStream} to implement
	 * this method directly.
	 *
	 * @throws IOException Thrown, if an error occurred while closing the stream or guaranteeing
	 *                     that the data is persistent.
	 */
	public abstract void close() throws IOException;
}
