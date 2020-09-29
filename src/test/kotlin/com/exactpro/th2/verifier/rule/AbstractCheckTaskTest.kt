/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.verifier.rule

import com.exactpro.th2.eventstore.grpc.AsyncEventStoreServiceService
import com.exactpro.th2.eventstore.grpc.EventStoreServiceGrpc
import com.exactpro.th2.eventstore.grpc.EventStoreServiceGrpc.EventStoreServiceFutureStub
import com.exactpro.th2.eventstore.grpc.Response
import com.exactpro.th2.eventstore.grpc.StoreEventBatchRequest
import com.exactpro.th2.eventstore.grpc.StoreEventRequest
import com.exactpro.th2.proto.service.generator.core.antlr.ServiceClassGenerator
import com.google.protobuf.StringValue
import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.stub.StreamObserver
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.slf4j.LoggerFactory
import kotlin.test.assertNotNull

abstract class AbstractCheckTaskTest {
    private lateinit var server: Server
    private lateinit var channel: ManagedChannel

    private lateinit var serverStub: TestEventStorageImpl
    protected lateinit var clientStub: AsyncEventStoreServiceService

    @BeforeEach
    internal fun setUp() {
        val serverName = InProcessServerBuilder.generateName()
        serverStub = TestEventStorageImpl()

        server = InProcessServerBuilder.forName(serverName).directExecutor().addService(serverStub).build().start()

        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build()
        clientStub = object : AsyncEventStoreServiceService {

            private val stub = EventStoreServiceGrpc.newStub(channel)

            override fun storeEvent(p0: StoreEventRequest?, p1: StreamObserver<Response>?) {
                stub.storeEvent(p0, p1)
            }

            override fun storeEventBatch(p0: StoreEventBatchRequest?, p1: StreamObserver<Response>?) {
                stub.storeEventBatch(p0, p1)
            }

        }
    }

    @AfterEach
    internal fun tearDown() {
        try {
            channel.shutdownNow()
        } catch (ex: Exception) {
            LOGGER.error("Cannot shutdown channel", ex)
        }

        try {
            server.shutdownNow()
        } catch (ex: Exception) {
            LOGGER.error("Cannot shutdown server", ex)
        }
    }

    fun awaitEventBatchRequest(timeout: Long = 1000L): StoreEventBatchRequest {
        val currentTime = System.currentTimeMillis()
        while (System.currentTimeMillis() < currentTime + timeout) {
            if (serverStub.eventBatchRequest != null) {
                break
            }
            Thread.sleep(100L)
        }
        return assertNotNull(serverStub.eventBatchRequest, "Request was not received during $timeout millis")
    }

    protected class TestEventStorageImpl : EventStoreServiceGrpc.EventStoreServiceImplBase() {
        private var _eventBatchRequest: StoreEventBatchRequest? = null
        val eventBatchRequest: StoreEventBatchRequest?
            get() = _eventBatchRequest

        override fun storeEventBatch(request: StoreEventBatchRequest, responseObserver: StreamObserver<Response>) {
            _eventBatchRequest = request
            responseObserver.onNext(Response.newBuilder().setId(StringValue.of("id")).build())
            responseObserver.onCompleted()
        }
    }

    companion object {
        protected val LOGGER = LoggerFactory.getLogger(AbstractCheckTaskTest::class.java)
    }
}