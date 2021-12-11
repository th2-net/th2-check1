/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.check1;

import static com.exactpro.th2.common.grpc.RequestStatus.Status.ERROR;
import static com.exactpro.th2.common.grpc.RequestStatus.Status.SUCCESS;

import com.exactpro.th2.check1.utils.ProtoMessageUtilsKt;
import com.exactpro.th2.common.message.MessageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.check1.grpc.ChainID;
import com.exactpro.th2.check1.grpc.CheckpointResponse;
import com.exactpro.th2.check1.grpc.Check1Grpc.Check1ImplBase;
import com.exactpro.th2.check1.grpc.CheckRuleRequest;
import com.exactpro.th2.check1.grpc.CheckRuleResponse;
import com.exactpro.th2.check1.grpc.CheckSequenceRuleRequest;
import com.exactpro.th2.check1.grpc.CheckSequenceRuleResponse;
import com.exactpro.th2.check1.grpc.CheckpointRequest;
import com.exactpro.th2.check1.grpc.NoMessageCheckResponse;
import com.exactpro.th2.check1.grpc.NoMessageCheckRequest;
import com.exactpro.th2.common.grpc.RequestStatus;

import io.grpc.stub.StreamObserver;

public class Check1Handler extends Check1ImplBase {
    private final Logger logger = LoggerFactory.getLogger(getClass().getName() + "@" + hashCode());

    private final CollectorService collectorService;

    public Check1Handler(CollectorService collectorService) {
        this.collectorService = collectorService;
    }

    @Override
    public void createCheckpoint(CheckpointRequest request, StreamObserver<CheckpointResponse> responseObserver) {
        try {
            var internalCheckpoint = collectorService.createCheckpoint(request);
            CheckpointResponse checkpointResponse = CheckpointResponse.newBuilder()
                    .setCheckpoint(ProtoMessageUtilsKt.convert(internalCheckpoint))
                    .build();
            if (logger.isDebugEnabled()) {
                logger.debug("Created checkpoint internal '{}' proto '{}", internalCheckpoint, MessageUtils.toJson(checkpointResponse));
            }
            responseObserver.onNext(checkpointResponse);
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("CheckRule failed. Request " + MessageUtils.toJson(request), e);
            }
            responseObserver.onNext(CheckpointResponse.newBuilder()
                    .setStatus(RequestStatus.newBuilder().setStatus(ERROR).setMessage("Create checkpoint failed. See the logs.").build())
                    .build());
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void submitCheckRule(CheckRuleRequest request, StreamObserver<CheckRuleResponse> responseObserver) {
        try {
            if (logger.isInfoEnabled()) {
                logger.info("Submit CheckRule request: " + MessageUtils.toJson(request));
            }

            CheckRuleResponse.Builder response = CheckRuleResponse.newBuilder();
            try {
                ChainID chainID = collectorService.verifyCheckRule(request);
                response.setChainId(chainID)
                        .setStatus(RequestStatus.newBuilder().setStatus(SUCCESS));
            } catch (Exception e) {
                if (logger.isErrorEnabled()) {
                    logger.error("CheckRule failed in CollectorService. Request " + MessageUtils.toJson(request), e);
                }
                RequestStatus status = RequestStatus.newBuilder()
                        .setStatus(ERROR)
                        .setMessage("submitCheckRule interrupted by internal process")
                        .build();
                response.setStatus(status);
            }
            responseObserver.onNext(response.build());
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("CheckRule failed. Request " + MessageUtils.toJson(request), e);
            }
            responseObserver.onNext(CheckRuleResponse.newBuilder()
                    .setStatus(RequestStatus.newBuilder().setStatus(ERROR).setMessage("CheckRule failed. See the logs.").build())
                    .build());
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void submitCheckSequenceRule(CheckSequenceRuleRequest request, StreamObserver<CheckSequenceRuleResponse> responseObserver) {
        try {
            CheckSequenceRuleResponse.Builder response = CheckSequenceRuleResponse.newBuilder();
            try {
                if (logger.isInfoEnabled()) {
                    logger.info("Submitting sequence rule for request '" + MessageUtils.toJson(request) + "' started");
                }
                ChainID chainID = collectorService.verifyCheckSequenceRule(request);
                if (logger.isInfoEnabled()) {
                    logger.info("Submitting sequence rule for request '" + request.getDescription() + "' finished");
                }
                response.setChainId(chainID)
                        .setStatus(RequestStatus.newBuilder().setStatus(SUCCESS));
            } catch (Exception e) {
                if (logger.isErrorEnabled()) {
                    logger.error("Submitting sequence rule for request '" + MessageUtils.toJson(request) + "' failed", e);
                }
                RequestStatus status = RequestStatus.newBuilder()
                        .setStatus(ERROR)
                        .setMessage("Sequence rule rejected by internal process: " + e.getMessage())
                        .build();
                response.setStatus(status);
            }
            responseObserver.onNext(response.build());
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("Sequence rule task for request '" + MessageUtils.toJson(request) + "' isn't submitted", e);
            }
            responseObserver.onNext(CheckSequenceRuleResponse.newBuilder()
                    .setStatus(RequestStatus.newBuilder().setStatus(ERROR)
                            .setMessage("Sequence rule rejected by internal process: " + e.getMessage())
                            .build())
                    .build());
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void submitNoMessageCheck(NoMessageCheckRequest request, StreamObserver<NoMessageCheckResponse> responseObserver) {
        try {
            if (logger.isInfoEnabled()) {
                logger.info("Submitting no message check rule for request '{}' started", MessageUtils.toJson(request));
            }

            NoMessageCheckResponse.Builder response = NoMessageCheckResponse.newBuilder();
            try {
                ChainID chainID = collectorService.verifyNoMessageCheck(request);
                response.setChainId(chainID)
                        .setStatus(RequestStatus.newBuilder().setStatus(SUCCESS));
            } catch (Exception e) {
                if (logger.isErrorEnabled()) {
                    logger.error("No message check rule task for request '{}' isn't submitted", MessageUtils.toJson(request), e);
                }
                RequestStatus status = RequestStatus.newBuilder()
                        .setStatus(ERROR)
                        .setMessage("No message check rule rejected by internal process: " + e.getMessage())
                        .build();
                response.setStatus(status);
            }
            responseObserver.onNext(response.build());
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("No message check rule failed. Request " + MessageUtils.toJson(request), e);
            }
            responseObserver.onNext(NoMessageCheckResponse.newBuilder()
                    .setStatus(RequestStatus.newBuilder().setStatus(ERROR).setMessage("No message check rule failed. See the logs.").build())
                    .build());
        } finally {
            responseObserver.onCompleted();
        }
    }
}