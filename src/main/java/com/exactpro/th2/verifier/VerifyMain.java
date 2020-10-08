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
package com.exactpro.th2.verifier;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.configuration.RabbitMQConfiguration;
import com.exactpro.th2.infra.grpc.MessageBatch;
import com.exactpro.th2.schema.factory.CommonFactory;
import com.exactpro.th2.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.schema.message.MessageRouter;
import com.exactpro.th2.verifier.cfg.CollectorServiceConfiguration;
import com.exactpro.th2.verifier.configuration.VerifierConfiguration;

public class VerifyMain {
    private final static Logger LOGGER = LoggerFactory.getLogger(VerifyMain.class);

    /**
     * Environment variables:
     *  {@link RabbitMQConfiguration#ENV_RABBITMQ_HOST}
     *  {@link RabbitMQConfiguration#ENV_RABBITMQ_PORT}
     *  {@link RabbitMQConfiguration#ENV_RABBITMQ_USER}
     *  {@link RabbitMQConfiguration#ENV_RABBITMQ_PASS}
     *  {@link RabbitMQConfiguration#ENV_RABBITMQ_VHOST}
     */
    public static void main(String[] args) {
        try {
            CommonFactory commonFactory = CommonFactory.createFromArguments(args);
            MessageRouter<MessageBatch> messageRouter = (MessageRouter<MessageBatch>) commonFactory.getMessageRouterParsedBatch();
            GrpcRouter grpcRouter = commonFactory.getGrpcRouter();
            VerifierConfiguration configuration = commonFactory.getCustomConfiguration(VerifierConfiguration.class);

            CollectorService collectorService = new CollectorService(messageRouter, commonFactory.getEventBatchRouter(), new CollectorServiceConfiguration(configuration));
            ExecutorService executorService = Executors.newFixedThreadPool(configuration.getCountExecutorThreads());
            Runtime.getRuntime().addShutdownHook(new Thread(collectorService::close));
            Runtime.getRuntime().addShutdownHook(new Thread(executorService::shutdown));//TODO fix
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
}
