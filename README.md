# th2 check1 (3.3.0)

## Overview

The component is responsible for verifying decoded messages.

Communication with the script takes place via grpc, messages are received via rabbit mq.

The component subscribes to queues specified in the configuration and accumulates messages from them in a FIFO buffer. 

The buffer size is configurable and is 1000 by default.

When the component starts, the grpc server starts and then the component waits for incoming grpc requests for verification.

# Verification requests

Available requests described in [this repository](https://gitlab.exactpro.com/vivarium/th2/th2-core-open-source/th2-grpc-check1)

- CheckSequenceRuleRequest - prefilters messages and verify all of them by filter. Order checking configured from request.
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

## Release Notes

### 3.3.0

+ Fix problem with missing key field markers in verification entry

### 3.2.1

+ removed gRPC event loop handling
+ fixed dictionary reading

### 3.2.0

+ reads dictionaries from the /var/th2/config/dictionary folder.
+ uses mq_router, grpc_router, cradle_manager optional JSON configs from the /var/th2/config folder
+ tries to load log4j.properties files from sources in order: '/var/th2/config', '/home/etc', configured path via cmd, default configuration
+ update Cradle version. Introduce async API for storing events
