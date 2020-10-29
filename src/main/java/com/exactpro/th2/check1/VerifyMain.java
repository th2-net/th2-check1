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

import java.io.Closeable;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.check1.configuration.VerifierConfiguration;

public class VerifyMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(VerifyMain.class);

    public static void main(String[] args) {
        try {
            CommonFactory commonFactory = CommonFactory.createFromArguments(args);
            MessageRouter<MessageBatch> messageRouter = commonFactory.getMessageRouterParsedBatch();
            GrpcRouter grpcRouter = commonFactory.getGrpcRouter();
            VerifierConfiguration configuration = commonFactory.getCustomConfiguration(VerifierConfiguration.class);

            CollectorService collectorService = new CollectorService(messageRouter, commonFactory.getEventBatchRouter(), configuration);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> closeResources(collectorService::close, commonFactory)));
            VerifierHandler verifierHandler = new VerifierHandler(collectorService);

            VerifierServer verifierServer = new VerifierServer(grpcRouter.startServer(verifierHandler));
            verifierServer.start();
            LOGGER.info("verify started");
            verifierServer.blockUntilShutdown();
        } catch (Throwable e) {
            LOGGER.error("Fatal error: {}", e.getMessage(), e);
            System.exit(-1);
        }
    }

    private static void closeResources(AutoCloseable... resources) {
        for (AutoCloseable resource : resources) {
            try {
                resource.close();
            } catch (Exception e) {
                LOGGER.error("Cannot close resource", e);
            }
        }
    }
}
