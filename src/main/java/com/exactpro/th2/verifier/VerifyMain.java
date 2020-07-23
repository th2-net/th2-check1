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
package com.exactpro.th2.verifier;

import com.exactpro.th2.configuration.RabbitMQConfiguration;
import com.exactpro.th2.verifier.cfg.CollectorServiceConfiguration;
import com.exactpro.th2.verifier.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.exactpro.th2.ConfigurationUtils.safeLoad;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.exactpro.th2.configuration.MicroserviceConfiguration;

public class VerifyMain {
    private final static Logger LOGGER = LoggerFactory.getLogger(VerifyMain.class);

    /**
     * Environment variables:
     *  {@link Configuration#ENV_GRPC_PORT}
     *  {@link RabbitMQConfiguration#ENV_RABBITMQ_HOST}
     *  {@link RabbitMQConfiguration#ENV_RABBITMQ_PORT}
     *  {@link RabbitMQConfiguration#ENV_RABBITMQ_USER}
     *  {@link RabbitMQConfiguration#ENV_RABBITMQ_PASS}
     *  {@link RabbitMQConfiguration#ENV_RABBITMQ_VHOST}
     *  {@link Configuration#MESSAGE_CACHE_SIZE}
     */
    public static void main(String[] args) {
        try {
            MicroserviceConfiguration microserviceConfiguration = readConfiguration(args);
            CollectorServiceConfiguration configuration = new CollectorServiceConfiguration(microserviceConfiguration);
            CollectorServiceA collectorService = new CollectorServiceA(configuration);
            ExecutorService executorService = Executors.newFixedThreadPool(10);//TODO config in future
            Runtime.getRuntime().addShutdownHook(new Thread(collectorService::close));
            Runtime.getRuntime().addShutdownHook(new Thread(executorService::shutdown));//TODO fix
            VerifierHandler verifierHandler = new VerifierHandler(collectorService, executorService);
            VerifierServer verifierServer = new VerifierServer(microserviceConfiguration.getPort(), verifierHandler);
            verifierServer.start();
            LOGGER.info("verify started on {} port", microserviceConfiguration.getPort());
            verifierServer.blockUntilShutdown();
        } catch (Throwable e) {
            LOGGER.error("Fatal error: {}", e.getMessage(), e);
            System.exit(-1);
        }
    }

    private static Configuration readConfiguration(String[] args) {
        Configuration configuration = args.length > 0
                ? safeLoad(Configuration::load, Configuration::new, args[0])
                : new Configuration();
        LOGGER.info("Loading verify with configuration: {}", configuration);
        return configuration;
    }
}
