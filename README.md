# th2 check1 (3.9.0)

## Overview

The component is responsible for verifying decoded messages.

Communication with the script takes place via grpc, messages are received via rabbit mq.

The component subscribes to queues specified in the configuration and accumulates messages from them in a FIFO buffer. 

The buffer size is configurable, and it is set to 1000 by default.

When the component starts, the grpc server also starts and then the component waits for incoming grpc requests for verification.

# Verification requests

Available requests are described in [this repository](https://gitlab.exactpro.com/vivarium/th2/th2-core-open-source/th2-grpc-check1)

- CheckSequenceRuleRequest - prefilters the messages and verify all of them by filter. Order checking configured from request.
- CheckRuleRequest - get message filter from request and check it with messages in the cache or await specified time in case of empty cache or message absence.
- NoMessageCheckRequest - prefilters messages and verifies that no other messages have been received.

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
    auto-sequence-rule-silence-check: false
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
  "max-event-batch-content-size": "1048576",
  "rule-execution-timeout": 5000,
  "auto-sequence-rule-silence-check": false
}
```

### Properties description

#### message-cache-size
The number of messages for each stream (alias + direction) that will be buffered.

#### cleanup-older-than
The time before the verification chain (from a task that is complete) will be removed.
The value will be interpreted as time unit defined in _cleanup-time-unit_ setting. _The default value is set to 60_

#### cleanup-time-unit
The time unit for _cleanup-older-than_ setting. The available values are MILLIS, SECONDS, MINUTES, HOURS. _The default value is set to SECONDS_

#### max-event-batch-content-size
The max size in bytes of summary events content in a batch defined in _max-event-batch-content-size_ setting. _The default value is set to 1048576_

#### auto-sequence-rule-silence-check
Defines a default behavior for creating CheckSequenceRule if `silence_check` parameter is not specified in the request. The default value is `false`

## Required pins

The Check1 component has two types of pin:
* gRPC server pin: it allows other components to connect via `com.exactpro.th2.check1.grpc.Check1Service` class.
* MQ pin: it is used for listening to parsed messages. You can link several sources with different directions and session alises to it.

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

## Prometheus metrics
The Check1 component publishes Prometheus metrics to observe the actual state of it
* `th2_check1_actual_cache_number` - actual number of messages in caches
* `th2_check1_active_tasks_number` - actual number of currently working rules

The `th2_check1_actual_cache_number` metric separate messages with two labels:
* `session_alias` - session alias of received message
* `direction` - direction of received message

The `th2_check1_active_tasks_number` metric separate rules with label `rule_type`

## Release Notes

### 3.9.0

#### Added:
+ Implemented NoMessageCheck rule task. Updated CheckRule and CheckSequence rule tasks
+ New configuration parameter `rule-execution-timeout` witch is used if the user has not specified a timeout for the rule execution
+ Auto silence check after the CheckSequenceRule.
  It verifies that there were not any messages matches the pre-filter in original request for CheckSequenceRule.
  It awaits for realtime timeout that is equal to clean-up timeout.
  Report about unexpected messages only after the timeout exceeds. Reports nothing if any task is added to the chain.

### 3.8.0

#### Added:
+ Added check for positive timeout
+ Added mechanism for handling exceptions when creating and executing rules which publishes events about an error that has occurred
+ Added metric for monitoring active rules and messages count
+ Added check for required message type in the message filter
+ Provided more detailed logging in comparable messages
+ Provided the ability to attach verification description to event
+ Provided the ability to verify repeating groups according to defined filters via `check_repeating_group_order` parameter in the `RootComparisonSettings` message

#### Changed:
+ Migrated `common` version from `3.25.0` to `3.26.4`
  + Added support for converting SimpleList to readable payload body
  + Added the new `description` parameter to `RootMessageFilter` message
+ Migrated `grpc-check1` version from `3.2.0` to `3.4.2`
+ Migrated sailfish-utils from `3.7.0` to `3.8.1`
  + Now Check1 keep the order of repeating result groups by default
  + Fix IN, NOT_IN FilterOperation interaction

### 3.7.2

#### Changed:
+ The root event name is now shorter. The additional information about session alias and direction is moved to the event body.
  The user's description should be displayed more clearly in the report.

### 3.7.1

+ Migrated common version from `3.23.0` to `3.25.0`
+ Improved message filter table view

### 3.7.0

+ Added functional for 'IN', 'LIKE', 'MORE', 'LESS', 'WILDCARD' FilterOperations and their negative versions

### 3.6.1

+ Fixed a problem where rule completes check before the timer for execution has been scheduled

### 3.6.0

+ Fixed configuration for gRPC server
    + Added the property `workers`, which changes the count of gRPC server's threads
+ Disable waiting for connection recovery when closing the `SubscribeMonitor`    

### 3.5.1

+ fixed a problem while using full verification instead of verification by key fields in matching-filter of sequence rule when order check is disabled

### 3.5.0

+ moves the message cursor in the message stream when a filter is matched by the key fields
+ creates events with a message filter
+ fixed an event layout problem for the sequence check rule

### 3.4.0

+ Added the max-event-batch-content-size option

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