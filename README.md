# th2 check1 (4.7.0)

## Overview

The component is responsible for verifying decoded messages.

Communication with the script takes place via grpc, messages are received via rabbit mq.

The component subscribes to the queues specified in the configuration and accumulates messages from them in a FIFO
buffer.

The buffer size is configurable, and it is set to 1000 by default.

When the component starts, the grpc server also starts and then the component waits for incoming grpc requests for
verification.

# Verification requests

Available requests are described
in [this repository](https://github.com/th2-net/th2-grpc-check1/blob/dev-version-4/src/main/proto/th2_grpc_check1/check1.proto)

- CheckSequenceRuleRequest - prefilters the messages and verify all of them by filter. Order checking configured from
  request. Depending on the request and check1 configuration **SilenceCheckRule** can be added after the
  CheckSequenceRule. It verifies that there were not any messages matching the pre-filter in the original request. It
  awaits for realtime timeout that is equal to clean-up timeout. Reports about unexpected messages only after the
  timeout is exceeded. Reports nothing if any task is added to the chain.
- CheckRuleRequest - get message filter from request and check it with messages in the cache or await specified time in
  case of empty cache or message absence.
- NoMessageCheckRequest - prefilters messages and verifies that no other messages have been received.
- WaitForResult - synchronous request waiting for the result of specified rule during specified timeout

## Request parameters

### Common

#### Required

* **parent_event_id** - all events generated by the rule will be attached to that event
* **connectivity_id** (the `session_alias` inside `connectivity_id` must not be empty)

#### Optional

* **direction** - the direction of messages to be checked by rule. By default, it is _FIRST_
* **chain_id** - the id to connect rules (rule starts checking after the previous one in the chain). Considers *
  *connectivity_id**
* **description** - the description that will be added to the root event produced by the rule
* **timeout** - defines the allowed timeout for messages matching by real time. If not set the default value from check1
  settings will be taken
* **store_result** - `true` indicates that the rule result should be stored for later request using `WaitForResult` method
* **message_timeout** - defines the allowed timeout for messages matching by the time they were received
* **checkpoint** (must be set if `message_timeout` is used and no valid `chain_id` has been provided)

### CheckRuleRequest

#### Required

* **root_filter** or **filter** (please note, that the `filter` parameter is deprecated and will be removed in the
  future releases)

### CheckSequenceRuleRequest

#### Required

* **root_message_filters** or **message_filters** with at least one filter
  (please note, that the `message_filters` parameter is deprecated and will be removed in the future releases)

#### Optional

* **pre_filter** - pre-filtering for messages. Only messages that passed the filter will be checked by the main filters.
* **check_order** - enables order validation in message's collections
* **silence_check** - enables auto-check for messages that match the `pre_filter` after the rule has finished

### NoMessageCheckRequest

#### Optional

* **pre_filter** pre-filtering for messages that should not be received.

### WaitForResult

#### Required

* **rule_id** - the id of rule
* **timeout** - timeout for waiting for result

### SubmitMultipleRules

#### Required

* **rules** - list of rules to submit

#### Optional

* **default_parent_event_id** - default value for `parent_event_id` of submitted rules.
* **default_chain** - default value for `parent_event_id` of submitted rules.

### PostMultipleRules

the same as `SubmitMultipleRules` but returns immediately with `Empty` response

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
    rule-execution-timeout: '5000'
    auto-silence-check-after-sequence-rule: false
    time-precision: 'PT0.000000001S'
    decimal-precision: '0.00001'
    enable-checkpoint-events-publication: true
    rules-execution-threads: 1
  type: th2-check1
  pins:
    - name: server
      connection-type: grpc
    - name: from_codec_proto
      connection-type: mq
      attributes: [ 'subscribe', 'parsed' ]
    - name: from_codec_transport
      connection-type: mq
      attributes: [ 'subscribe', 'transport-group' ]
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
  "auto-silence-check-after-sequence-rule": false,
  "time-precision": "PT0.000000001S",
  "decimal-precision": 0.00001,
  "check-null-value-as-empty": false,
  "enable-checkpoint-events-publication": true,
  "rules-execution-threads": 1
}
```

### Properties description

#### message-cache-size

The number of messages for each stream (alias + direction) that will be buffered.

#### cleanup-older-than

The time before the verification chain (from a task that is complete) will be removed. The value will be interpreted as
time unit defined in _cleanup-time-unit_ setting. _The default value is set to 60_

#### cleanup-time-unit

The time unit for _cleanup-older-than_ setting. The available values are MILLIS, SECONDS, MINUTES, HOURS. _The default
value is set to SECONDS_

#### min-cleanup-interval-ms

Minimum interval between cleanups in milliseconds. Default value is 1000 milliseconds.

#### max-event-batch-content-size

The max size in bytes of summary events content in a batch defined in _max-event-batch-content-size_ setting. _The
default value is set to 1048576_

#### rule-execution-timeout

The default rule execution timeout is used if no rule timeout is specified. Measured in milliseconds

#### auto-silence-check-after-sequence-rule

Defines a default behavior for creating CheckSequenceRule if `silence_check` parameter is not specified in the request.
The default value is `false`

#### time-precision

The time precision is used to compare two time values. It is based on the `ISO-8601` duration format `PnDTnHnMn.nS` with
days considered to be exactly 24 hours. Additional information can be
found [here](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/Duration.html#parse(java.lang.CharSequence))

#### decimal-precision

The decimal precision is used to compare the value of two numbers. Can be specified in number or string format. For
example `0.0001`, `0.125`, `125E-3`

#### check-null-value-as-empty

`check-null-value-as-empty` is used for `EMPTY` and `NOT_EMPTY` operations to check if `NULL_VALUE` value is empty. By
default, this parameter is set to `false`. For example, if the `checkNullValueAsEmpty` parameter is:

+ `true`, then `NULL_VALUE` is equal to `EMPTY`, otherwise `NULL_VALUE` is equal to `NOT_EMPTY`

#### enable-checkpoint-events-publication

Enables event publication for each stream that was added into checkpoint.
Otherwise, only top events with attached messages will be published.
_Enabled by default._

#### rules-execution-threads

Configures number of threads for rules execution. 
This option can help to increase performance when you are going to calculate heavy rules, and you haven't got strict CPU limitation,
otherwise use `1` thread for rule execution.
The default value is `1`

## Required pins

The Check1 component has two types of pin:

* gRPC server pin: it allows other components to connect via `com.exactpro.th2.check1.grpc.Check1Service` class.
* MQ pins: they are used for listening to protobuf and th2 transport parsed messages. You can link several sources with
  different directions and session aliases to it.

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: check1
spec:
  pins:
    - name: server
      connection-type: grpc
    - name: from_codec_proto
      connection-type: mq
      attributes:
        - 'subscribe'
        - 'parsed'
    - name: from_codec_transport
      connection-type: mq
      attributes:
        - 'subscribe'
        - 'transport-group'
```

## Prometheus metrics

The Check1 component publishes Prometheus metrics to observe the actual state of it

* `th2_check1_actual_cache_number` - actual number of messages in caches
* `th2_check1_active_tasks_number` - actual number of currently working rules

The `th2_check1_actual_cache_number` metric separate messages with three labels:

* `book_name` - book name of received message
* `session_alias` - session alias of received message
* `direction` - direction of received message

The `th2_check1_active_tasks_number` metric separate rules with label `rule_type`

## Release Notes

### 4.7.0
+ Updated th2 gradle plugin: `0.2.4` (bom: `4.11.0`)

### 4.6.1
+ `NoMessageCheck` rule publishes `noMessageCheckExecutionStop` event with status `SUCCESS` 
  when user requests check with `message_timeout` = 0 or without it and the rule is completed by `TIMEOUT` reason.
+ Updated th2 gradle plugin: `0.1.3` (bom: `4.8.0`)

### 4.6.0
+ Migrated to th2 gradle plugin `0.1.1` (bom: `4.6.1`)
+ Updated grpc-check1: `4.4.1-dev`
+ Updated common: `5.14.0-dev`
+ Updated common-utils `2.3.0-dev`
+ workflows update

### 4.5.0

#### Added:
+ `multiSubmitRules` grpc method (grpc-check1: `4.4.0-dev`)
+ `min-cleanup-interval-ms` configuration parameter to specify minimal interval between rules cleanup

### 4.4.0

#### Added:
+ Support for disabling of order verification for simple collection
+ Switch for events publication in checkpoint request. Parameter `enable-checkpoint-events-publication` should be used for that
+ `WaitForResult` method added

### 4.3.0

#### Added:
+ Configure number of threads for rules execution. Parameter `rules-execution-threads`

#### Updated:
+ common: `5.6.0-dev`
+ grpc-check1: `4.3.0-dev`
+ rxjava: `2.2.21`

#### Merged:
+ Changes from 3.10.0 version

### 4.2.1

#### Changed:

* verification event now displays the field as failed if it has nested failed field(s)

#### Fixed:

* `sailfish-utils` could not work with `BigInteger` that is decoded in th2 transport parsed messages
  (when value does not fit into `Long`)

### 4.2.0

#### Updated:

* bom: `4.5.0-dev`
* common: `5.4.0-dev`
* common-utils: `2.2.0-dev`
* sailfish-utils: `4.1.0-dev`
* kotlin: `1.8.22`

### 4.1.0

#### Added:

+ Support for listening to messages using th2 transport protocol from a queue

#### Changed:

+ Used `saifish-common` instead of `sailfish-core`

+ Migrated to bom `4.4.0`
+ Migrated to common `5.3.1-dev`
+ Migrated to common-utils `2.1.0-dev`
+ Migrated to sailfish-utils `4.0.0-dev`

#### Gradle plugins:
+ Updated org.owasp.dependencycheck: `8.3.1`

### 4.0.1

#### Added:

+ Vulnerability scanning
+ Dev release workflow

#### Changed:

+ Excluded vulnerable transitive dependencies from sailfish
+ Migrated to common `5.2.0-dev`

### 4.0.0

#### Added:

+ `book_name` entity has been added to the `SessionKey` class as a property and into
  the `th2_check1_actual_cache_number` metric as a label

#### Changed:

+ Migrated to books/pages cradle 4.0.0
    + Migrated `common` version from `3.31.3` to `4.0.0`
    + Migrated `grpc-check1` version from `3.5.1` to `4.0.0`

### 3.10.3

#### Changed:

+ Excluded `apache-mina-core` from dependencies list

### 3.10.2

#### Changed:

+ Excluded `junit` from dependencies list

### 3.10.1

#### Changed:

+ Migrated `common` version from `3.44.0` to `3.44.1`
+ Updated bom to `4.2.0`

### 3.10.3

#### Changed:
+ Excluded `apache-mina-core` from dependencies list

### 3.10.2

#### Changed:
+ Excluded `junit` from dependencies list

### 3.10.1

#### Changed:
+ Migrated `common` version from `3.44.0` to `3.44.1`
+ Updated bom to `4.2.0`

### 3.10.0

#### Added:

+ Support for disabling of order verification for simple collection
+ Switch for events publication in checkpoint request. Parameter `enable-checkpoint-events-publication` should be used for that.

#### Changed:

+ Migrated `common` version from `3.31.3` to `3.44.0`
+ Migrated `sailfish-utils` version from `3.12.2` to `3.14.0`
    + sailfish updated to 3.3.54 
    + Improved condition output format for `EQ_PRECISION`, `WILDCARD`, `LIKE`, `IN`, `MORE`, `LESS` operations and their
      negative versions
+ Changed the way the check1 works with threads internally.
  Now it uses a common executor for running check rules instead of creating an executor per each rule

### 3.9.0

#### Added:

+ Implemented NoMessageCheck rule task. Updated CheckRule and CheckSequence rule tasks
+ New configuration parameter `rule-execution-timeout` which is used if the user has not specified a timeout for the
  rule execution
+ Auto silence check after the CheckSequenceRule.
+ `auto-silence-check-after-sequence-rule` to setup a default behavior for CheckSequenceRule
+ New configuration parameter `time-precision` which is used if the user has not specified a time precision
+ New configuration parameter `decimal-precision` which is used if the user has not specified a number precision
+ New parameter `hint` for verification event which is used to display the reason for the failed field comparison. For
  example the type mismatch of the compared values
+ New configuration parameter `check-null-value-as-empty` witch us used to configure the `EMPTY` and `NOT_EMPTY`
  operations

#### Changed:

+ Migrated `common` version from `3.26.4` to `3.31.3`
+ Migrated `grpc-check1` version from `3.4.2` to `3.5.1`
+ Migrated `sailfish-utils` version from `3.9.1` to `3.12.2`
    + Fixed conversion of `null` values
    + Add marker for `null` values to determine whether the field was set with `null` value or was not set at all
    + Allow checking for exact `null` value in message
    + Added new parameter `checkNullValueAsEmpty` in the `FilterSettings`
+ Corrected verification entry when the `null` value and string `"null"` looked the same for the expected value
+ Fixed setting of the `failUnexpected` parameter while converting a message filter
+ Migrated `sailfish-core` version to `3.2.1752`
    + Fix incorrect matching in repeating groups with reordered messages

### 3.8.0

#### Added:

+ Added check for positive timeout
+ Added mechanism for handling exceptions when creating and executing rules which publishes events about an error that
  has occurred
+ Added metric for monitoring active rules and messages count
+ Added check for required message type in the message filter
+ Provided more detailed logging in comparable messages
+ Provided the ability to attach verification description to event
+ Provided the ability to verify repeating groups according to defined filters via `check_repeating_group_order`
  parameter in the `RootComparisonSettings` message

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

+ The root event name is now shorter. The additional information about session alias and direction is moved to the event
  body. The user's description should be displayed more clearly in the report.

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

+ fixed a problem while using full verification instead of verification by key fields in matching-filter of sequence
  rule when order check is disabled

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
+ tries to load log4j.properties files from sources in order: '/var/th2/config', '/home/etc', configured path via cmd,
  default configuration
+ update Cradle version. Introduce async API for storing events