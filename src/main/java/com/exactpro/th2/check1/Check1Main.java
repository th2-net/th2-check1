/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.check1;

import java.util.ArrayDeque;
import java.util.Deque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.check1.configuration.Check1Configuration;

public class Check1Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Check1Main.class);

    public static void main(String[] args) {
        try {
            Deque<AutoCloseable> toDispose = new ArrayDeque<>();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> closeResources(toDispose)));

            CommonFactory commonFactory = CommonFactory.createFromArguments(args);
            toDispose.add(commonFactory);

            MessageRouter<MessageBatch> messageRouter = commonFactory.getMessageRouterParsedBatch();
            GrpcRouter grpcRouter = commonFactory.getGrpcRouter();
            Check1Configuration configuration = commonFactory.getCustomConfiguration(Check1Configuration.class);

            CollectorService collectorService = new CollectorService(messageRouter, commonFactory.getEventBatchRouter(), configuration);
            toDispose.add(collectorService::close);

            Check1Handler check1Handler = new Check1Handler(collectorService);
            Check1Server check1Server = new Check1Server(grpcRouter.startServer(check1Handler));
            check1Server.start();
            LOGGER.info("verify started");
            check1Server.blockUntilShutdown();
        } catch (Throwable e) {
            LOGGER.error("Fatal error: {}", e.getMessage(), e);
            System.exit(-1);
        }
    }

    /**
     * Close resources in LIFO order
     * @param resources
     */
    private static void closeResources(Deque<AutoCloseable> resources) {
        resources.descendingIterator().forEachRemaining(resource -> {
            try {
                resource.close();
            } catch (Exception e) {
                LOGGER.error("Cannot close resource", e);
            }
        });
    }
}