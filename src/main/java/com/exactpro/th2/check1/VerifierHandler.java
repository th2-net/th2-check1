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

import static com.exactpro.th2.infra.grpc.RequestStatus.Status.ERROR;
import static com.exactpro.th2.infra.grpc.RequestStatus.Status.SUCCESS;
import static com.google.protobuf.TextFormat.shortDebugString;

import java.util.concurrent.ExecutorService;

import com.exactpro.th2.check1.CollectorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.infra.grpc.RequestStatus;
import com.exactpro.th2.verifier.grpc.ChainID;
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

    private final CollectorService collectorService;

    public VerifierHandler(CollectorService collectorService) {
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
                logger.info("Submit CheckRule request: " + shortDebugString(request));
            }

            CheckRuleResponse.Builder response = CheckRuleResponse.newBuilder();
            try {
                ChainID chainID = collectorService.verifyCheckRule(request);
                response.setChainId(chainID)
                        .setStatus(RequestStatus.newBuilder().setStatus(SUCCESS));
            } catch (Exception e) {
                if (logger.isErrorEnabled()) {
                    logger.error("CheckRule failed in CollectorService. Request " + shortDebugString(request), e);
                }
                RequestStatus status = RequestStatus.newBuilder()
                        .setStatus(ERROR)
                        .setMessage("submitCheckRule interrupted by internal process")
                        .build();
                response.setStatus(status);
            }
            responseObserver.onNext(response.build());
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
            CheckSequenceRuleResponse.Builder response = CheckSequenceRuleResponse.newBuilder();
            try {
                if (logger.isInfoEnabled()) {
                    logger.info("Submitting sequence rule for request '" + shortDebugString(request) + "' started");
                }
                ChainID chainID = collectorService.verifyCheckSequenceRule(request);
                if (logger.isInfoEnabled()) {
                    logger.info("Submitting sequence rule for request '" + request.getDescription() + "' finished");
                }
                response.setChainId(chainID)
                        .setStatus(RequestStatus.newBuilder().setStatus(SUCCESS));
            } catch (Exception e) {
                if (logger.isErrorEnabled()) {
                    logger.error("Submitting sequence rule for request '" + shortDebugString(request) + "' failed", e);
                }
                RequestStatus status = RequestStatus.newBuilder()
                        .setStatus(ERROR)
                        .setMessage("Sequence rule rejected by internal process: " + e.getMessage())
                        .build();
                response.setStatus(status);
            }
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            if (logger.isErrorEnabled()) {
                logger.error("Sequence rule task for request '" + shortDebugString(request) + "' isn't submitted", e);
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