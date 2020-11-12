# Overview

The component is responsible for verifying decoded messages.

Communication with the script takes place via grpc, messages are received via rabbit mq.

The component subscribes to queues specified in the configuration and accumulates messages from them in a FIFO buffer. 

The buffer size is configurable and is 1000 by default.

When the component starts, the grpc server starts and then the component waits for incoming grpc requests for verification.

# Verification requests

Available requests described in [this repository](https://gitlab.exactpro.com/vivarium/th2/th2-core-open-source/th2-grpc-check1)

- CheckSequenceRuleRequest - prefilters messages and verify all of them by filter. Order checking configured from request.
- CheckRuleRequest - get message filter from request and check it with messages in the cache or await specified time in case of empty cache or message absence.

# Configuration

This block describes the configuration for check1.

### Configuration example
```json
{
  "message-cache-size": 1000,
  "cleanup-older-than": 60,
  "cleanup-time-unit": "SECONDS"
}
```

### Properties description

#### message-cache-size
The number of messages for each stream (alias + direction) that will be buffered.

#### cleanup-older-than
The time before the verification chain (from a task that is complete) will be removed.
The value will be interpreted as time unit defined in _cleanup-time-unit_ setting. _The default value is 60_

#### cleanup-time-unit
The time unit for _cleanup-older-than_ setting. Available values are MILLIS, SECONDS, MINUTES, HOURS. _The default value is SECONDS_

## Requried pins

The Check1 component has got tow types of pin
* gRPC server pin to allow other components to connect via `com.exactpro.th2.check1.grpc.Check1Service` class.
* MQ pin to listen to parsed messages. You can link several sources with different directions and session alises to it.

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2GenericBox
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
