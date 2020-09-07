# Overview

The component is responsible for verifying decoded messages.

Communication with the script takes place via grpc, messages are received via rabbit mq.

The component subscribes to queues specified in the configuration and accumulates messages from them in a FIFO buffer. 

The buffer size is configurable and is 1000 by default.

When the component starts, the grpc server starts and then the component waits for incoming grpc requests for verification.

# Verification requests

Available requests described in [proto file](grpc-verifier/src/main/proto/th2/verifier.proto)

- CheckSequenceRuleRequest - prefilters messages and verify all of them by filter. Order checking configured from request.
- CheckRuleRequest - get message filter from request and check it with messages in the cache or await specified time in case of empty cache or message absence.

# Configuration

## Environment variables
- RABBITMQ_PASS=some_pass (***required***)
- RABBITMQ_HOST=some-host-name-or-ip (***required***)
- RABBITMQ_PORT=7777 (***required***)
- RABBITMQ_VHOST=someVhost (***required***)
- RABBITMQ_USER=some_user (***required***)
- GRPC_PORT=7878 (***required***)
- TH2_CONNECTIVITY_QUEUE_NAMES={"fix_client": {"exchangeName":"demo_exchange", "toSendQueueName":"client_to_send", "toSendRawQueueName":"client_to_send_raw", "inQueueName": "fix_codec_out_client", "inRawQueueName": "client_in_raw", "outQueueName": "client_out" , "outRawQueueName": "client_out_raw"  }, "fix_server": {"exchangeName":"demo_exchange", "toSendQueueName":"server_to_send", "toSendRawQueueName":"server_to_send_raw", "inQueueName": "fix_codec_out_server", "inRawQueueName": "server_in_raw", "outQueueName": "server_out" , "outRawQueueName": "server_out_raw"  }} (***required***)
- TH2_EVENT_STORAGE_GRPC_HOST=event-store-host-name-or-ip; (***required***)
- TH2_EVENT_STORAGE_GRPC_PORT=9999; (***required***)
- MESSAGE_CACHE_SIZE=1000; (***optional***)