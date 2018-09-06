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


package org.apache.flink.types;

import java.io.Serializable;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.io.IOReadableWritable;

/**
 * Value位于所有类型的继承链的最顶端，可以说是所有类型的根。它代指所有可被序列化为Flink二进制表示的类型。
 * 该接口本身并不提供任何接口方法，但它继承自两个接口。
 *
 * Basic value interface for types that act as serializable values.
 * <p>
 * This interface extends {@link IOReadableWritable} and requires to implement
 * the serialization of its value.
 * 
 * @see org.apache.flink.core.io.IOReadableWritable
 */
@Public
public interface Value extends IOReadableWritable, Serializable {
}
