Example of Verifier component env variables:
RABBITMQ_PASS=some_pass
RABBITMQ_HOST=some-host-name-or-ip
RABBITMQ_PORT=7777
RABBITMQ_VHOST=someVhost
RABBITMQ_USER=some_user
GRPC_PORT=7878
TH2_CONNECTIVITY_QUEUE_NAMES={"fix_client": {"exchangeName":"demo_exchange", "toSendQueueName":"client_to_send", "toSendRawQueueName":"client_to_send_raw", "inQueueName": "fix_codec_out_client", "inRawQueueName": "client_in_raw", "outQueueName": "client_out" , "outRawQueueName": "client_out_raw"  }, "fix_server": {"exchangeName":"demo_exchange", "toSendQueueName":"server_to_send", "toSendRawQueueName":"server_to_send_raw", "inQueueName": "fix_codec_out_server", "inRawQueueName": "server_in_raw", "outQueueName": "server_out" , "outRawQueueName": "server_out_raw"  }}
TH2_EVENT_STORAGE_GRPC_HOST=event-store-host-name-or-ip;
TH2_EVENT_STORAGE_GRPC_PORT=9999;
MESSAGE_CACHE_SIZE=1000; // size limit of message cache per queue.