/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.check1.utils

import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.MessageID
import com.google.protobuf.Timestamp

fun SessionKey.toMessageID(timestamp: Timestamp, sequence: Long): MessageID = MessageID.newBuilder()
    .setConnectionId(
        ConnectionID.newBuilder()
        .setSessionAlias(sessionAlias)
        .build())
    .setBookName(bookName)
    .setTimestamp(timestamp)
    .setSequence(sequence)
    .setDirection(direction)
    .build()