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

import static com.exactpro.th2.infra.grpc.RequestStatus.Status.ERROR;
import static com.exactpro.th2.infra.grpc.RequestStatus.Status.SUCCESS;
import static com.google.protobuf.TextFormat.shortDebugString;

import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.infra.grpc.RequestStatus;
import com.exactpro.th2.verifier.grpc.CheckRuleRequest;
import com.exactpro.th2.verifier.grpc.CheckRuleResponse;
import com.exactpro.th2.verifier.grpc.CheckSequenceRuleRequest;
import com.exactpro.th2.verifier.grpc.CheckSequenceRuleResponse;
import com.exactpro.th2.verifier.grpc.CheckpointRequest;
import com.exactpro.th2.verifier.grpc.CheckpointResponse;
import com.exactpro.th2.verifier.grpc.VerifierGrpc.VerifierImplBase;

import io.grpc.stub.StreamObserver;

public class VerifierHandler extends VerifierImplBase {
    private final Logger logger = LoggerFactory.getLogger(getClass().getName() + "@" + hashCode());

    private final CollectorServiceA collectorService;

    public VerifierHandler(CollectorServiceA collectorService, ExecutorService executorService) {
        this.collectorService = collectorService;
    }

    @Override
    public void createCheckpoint(CheckpointRequest request, StreamObserver<CheckpointResponse> responseObserver) {
        try {
            var internalCheckpoint = collectorService.createCheckpoint(request);
            CheckpointResponse checkpointResponse = CheckpointResponse.newBuilder()
                    .setCheckpoint(internalCheckpoint.convert())
                    .build();
            if (logger.isDebugEnabled()) {
                logger.debug("Created checkpoint internal '{}' proto '{}", internalCheckpoint, shortDebugString(checkpointResponse));
            }
            responseObserver.onNext(checkpointResponse);
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            if (logger.isErrorEnabled()) {
                logger.error("CheckRule failed. Request " + shortDebugString(request), e);
            }
            responseObserver.onNext(CheckpointResponse.newBuilder()
                    .setStatus(RequestStatus.newBuilder().setStatus(ERROR).setMessage("Create checkpoint failed. See the logs.").build())
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void submitCheckRule(CheckRuleRequest request, StreamObserver<CheckRuleResponse> responseObserver) {
        try {
            if (logger.isInfoEnabled()) {
                logger.info("SubmitCheckRule request: " + shortDebugString(request));
            }
            RequestStatus status = RequestStatus.newBuilder()
                    .setStatus(SUCCESS)
                    .build();
            try {
                collectorService.verifyCheckRule(request);
            } catch (Exception e) {
                if (logger.isErrorEnabled()) {
                    logger.error("CheckRule failed in CollectorService. Request " + shortDebugString(request), e);
                }
                status = RequestStatus.newBuilder()
                        .setStatus(ERROR)
                        .setMessage("submitCheckRule interrupted by internal process")
                        .build();
            }
            responseObserver.onNext(CheckRuleResponse.newBuilder()
                    .setStatus(status)
                    .build());
            responseObserver.onCompleted();
        } catch (Throwable e) {
            if (logger.isErrorEnabled()) {
                logger.error("CheckRule failed. Request " + shortDebugString(request), e);
            }
            responseObserver.onNext(CheckRuleResponse.newBuilder()
                    .setStatus(RequestStatus.newBuilder().setStatus(ERROR).setMessage("CheckRule failed. See the logs.").build())
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void submitCheckSequenceRule(CheckSequenceRuleRequest request, StreamObserver<CheckSequenceRuleResponse> responseObserver) {
        try {
            RequestStatus status = RequestStatus.newBuilder()//TODO determine status in future
                    .setStatus(SUCCESS)
                    .build();
            try {
                if (logger.isInfoEnabled()) {
                    logger.info("Sequence rule for request '" + shortDebugString(request) + "' started");
                }
                collectorService.verifyCheckSequenceRule(request);
                if (logger.isInfoEnabled()) {
                    logger.info("Sequence rule for request '" + shortDebugString(request) + "' finished");
                }
            } catch (Exception e) {
                if (logger.isErrorEnabled()) {
                    logger.error("Sequence rule for request '" + shortDebugString(request) + "' failed", e);
                }
                status = RequestStatus.newBuilder()
                        .setStatus(ERROR)
                        .setMessage("Sequence rule rejected by internal process: " + e.getMessage())
                        .build();
            }
            responseObserver.onNext(CheckSequenceRuleResponse.newBuilder()
                    .setStatus(status)
                    .build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            if (logger.isErrorEnabled()) {
                logger.error("Sequence rule task for request '" + shortDebugString(request) + "' isn't submited", e);
            }
            responseObserver.onNext(CheckSequenceRuleResponse.newBuilder()
                    .setStatus(RequestStatus.newBuilder().setStatus(ERROR)
                            .setMessage("Sequence rule rejected by internal process: " + e.getMessage())
                            .build())
                    .build());
            responseObserver.onCompleted();
        }
    }
}