# Overview

The component is responsible for verifying decoded messages.

Communication with the script takes place via grpc, messages are received via rabbit mq.

The component subscribes to queues specified in the configuration and accumulates messages from them in a FIFO buffer. 

The buffer size is configurable, and it is set to 1000 by default.

When the component starts, the grpc server also starts and then the component waits for incoming grpc requests for verification.

# Verification requests

Available requests are described in [this repository](https://gitlab.exactpro.com/vivarium/th2/th2-core-open-source/th2-grpc-check1)

- CheckSequenceRuleRequest - prefilters the messages and verify all of them by filter. Order checking configured from request.
- CheckRuleRequest - get message filter from request and check it with messages in the cache or await specified time in case of empty cache or message absence.

## Quick start
General view of the component will look like this:

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: check1
spec:
  image-name: ghcr.io/th2-net/th2-check1
  image-version: <image version>
  custom-config:
    message-cache-size: '1000'
    cleanup-older-than: '60'
    cleanup-time-unit: 'SECONDS'
    max-event-batch-content-size: '1048576'
  type: th2-check1
  pins:
    - name: server
      connection-type: grpc
    - name: from_codec
      connection-type: mq
      attributes: ['subscribe', 'parsed']
  extended-settings:
    service:
      enabled: true
      nodePort: '<port>'
    envVariables:
      JAVA_TOOL_OPTIONS: "-XX:+ExitOnOutOfMemoryError"
    resources:
      limits:
        memory: 200Mi
        cpu: 200m
      requests:
        memory: 100Mi
        cpu: 50m
```

# Configuration

This block describes the configuration for check1.

### Configuration example
```json
{
  "message-cache-size": 1000,
  "cleanup-older-than": 60,
  "cleanup-time-unit": "SECONDS",
  "max-event-batch-content-size": "1048576"
}
```

### Properties description

#### message-cache-size
The number of messages for each stream (alias + direction) that will be buffered.

#### cleanup-older-than
The time before the verification chain (from a task that is complete) will be removed.
The value will be interpreted as time unit defined in _cleanup-time-unit_ setting. _The default value is 60_

#### cleanup-time-unit
The time unit for _cleanup-older-than_ setting. The available values are MILLIS, SECONDS, MINUTES, HOURS. _The default value is set to SECONDS_

#### max-event-batch-content-size
The max size in bytes of summary events content in a batch defined in _max-event-batch-content-size_ setting. _The default value is 1048576_

## Required pins

The Check1 component has two types of pin:
* gRPC server pin to allow other components to connect via `com.exactpro.th2.check1.grpc.Check1Service` class.
* MQ pin to listen to parsed messages. You can link several sources with different directions and session alises to it.

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: check1
spec:
  pins:
    - name: server
      connection-type: grpc
    - name: in_parsed_message
      connection-type: mq
      attributes:
        - "subscribe"
        - "parsed"
```
