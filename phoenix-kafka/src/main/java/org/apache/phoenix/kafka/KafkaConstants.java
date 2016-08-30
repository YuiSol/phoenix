/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.kafka;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class KafkaConstants {

    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    
    public static final String KEY_SERIALIZER = "key.serializer";

    public static final String VALUE_SERIALIZER = "value.serializer";

    public static final String DEFAULT_KEY_SERIALIZER = StringSerializer.class.getName();

    public static final String DEFAULT_VALUE_SERIALIZER = StringSerializer.class.getName();
    
    public static final String KEY_DESERIALIZER = "key.deserializer";

    public static final String VALUE_DESERIALIZER = "value.deserializer";

    public static final String DEFAULT_KEY_DESERIALIZER = StringDeserializer.class.getName();

    public static final String DEFAULT_VALUE_DESERIALIZER = StringDeserializer.class.getName();

    public static final String TOPICS = "topics";

    public static final String GROUP_ID = "group.id";
    
    public static final String TIMEOUT = "poll.timeout.ms";
    
    public static final long DEFAULT_TIMEOUT = 100;
}
