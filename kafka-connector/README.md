### Introduction

The CloudPubSubConnector is a connector to be used with
[Kafka Connect](http://kafka.apache.org/documentation.html#connect) to publish
messages from [Kafka](http://kafka.apache.org) to
[Google Cloud Pub/Sub](https://cloud.google.com/pubsub/) or
[Pub/Sub Lite](https://cloud.google.com/pubsub/lite) and vice versa.

CloudPubSubSinkConnector provides a sink connector to copy messages from Kafka
to Google Cloud Pub/Sub.
CloudPubSubSourceConnector provides a source connector to copy messages from
Google Cloud Pub/Sub to Kafka.
PubSubLiteSinkConnector provides a sink connector to copy messages from Kafka
to Pub/Sub Lite.
PubSubLiteSourceConnector provides a source connector to copy messages from
Pub/Sub Lite to Kafka.

### Pre-Running Steps

1.  Regardless of whether you are running on Google Cloud Platform or not, you
    need to create a project and create service key that allows you access to
    the Cloud Pub/Sub API's and default quotas.

2.  Create project on Google Cloud Platform. By default, this project will have
    multiple service accounts associated with it (see "IAM & Admin" within GCP
    console). Within this section, find the tab for "Service Accounts". Create a
    new service account and make sure to select "Furnish a new private key".
    Doing this will create the service account and download a private key file
    to your local machine.

3.  Go to the "IAM" tab, find the service account you just created and click on
    the dropdown menu named "Role(s)". Under the "Pub/Sub" submenu, select
    "Pub/Sub Admin". Finally, the key file that was downloaded to your machine
    needs to be placed on the machine running the framework. An environment
    variable named GOOGLE_APPLICATION_CREDENTIALS must point to this file. (Tip:
    export this environment variable as part of your shell startup file).

    `export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key/file`
    
### Quickstart: copy_tool.py

You can download `copy_tool.py`, a single-file python script which downloads,
sets up and runs the kafka connector in a single-machine configuration. This
script requires:

1. python >= 3.5
1. [requests](https://requests.readthedocs.io/en/master/user/install/#python-m-pip-install-requests)
   installed
1. `JAVA_HOME` configured properly
1. `GOOGLE_APPLICATION_CREDENTIALS` set
1. A [properties file](#cloudpubsubconnector-configs) with connector.class and
   other properties set
   
It can be invoked on mac/linux with:

```bash
python3 path/to/copy_tool.py --bootstrap_servers=MY_KAFKA_SERVER,OTHER_SERVER --connector_properties_file=path/to/connector.properties
```

or windows with:

```bash
python3 path\to\copy_tool.py --bootstrap_servers=MY_KAFKA_SERVER,OTHER_SERVER --connector_properties_file=path\to\connector.properties
```

### Acquiring the connector

A pre-built uber-jar is available for download with the
[latest release](https://github.com/GoogleCloudPlatform/pubsub/releases).

You can also build the connector from head, as described [below](#building).

### Running a Connector

1.  Copy the pubsub-kafka-connector.jar to the place where you will run your Kafka
    connector.

2.  Create a configuration file for the Pub/Sub connector and copy it to the
    place where you will run Kafka connect. The configuration should set up the
    proper Kafka topics, Pub/Sub topic, and Pub/Sub project. For Pub/Sub Lite,
    this should also set the correct location (google cloud zone).
    Sample configuration files for the source and sink connectors are provided
    at configs/.

3.  Create an appropriate configuration for your Kafka connect instance. More
    information on the configuration for Kafka connect can be found in the
    [Kafka Users Guide](http://kafka.apache.org/documentation.html#connect_running).
    
4.  If running the Kafka Connector behind a proxy, you need to export the
    KAFKA_OPTS variable with options for connecting around the proxy. You can
    export this variable as part of a shell script in order ot make it easier.
    Here is an example:
 
   `export KAFKA_OPTS="-Dhttp.proxyHost=<host> -Dhttp.proxyPort=<port> -Dhttps.proxyHost=<host> -Dhttps.proxyPort=<port>"`

### CloudPubSubConnector Configs

In addition to the configs supplied by the Kafka Connect API, the Cloud Pub/Sub
Connector supports the following configs:

#### Source Connector

| Config | Value Range | Default | Description |
|------------------------|-----------------------------------|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| cps.subscription | String | REQUIRED (No default) | The name of the subscription to Cloud Pub/Sub, e.g. "sub" for subscription "/projects/bar/subscriptions/sub". |
| cps.project | String | REQUIRED (No default) | The project containing the topic from which to pull messages, e.g. "bar" from above. |
| cps.endpoint | String | "pubsub.googleapis.com:443" | The [Cloud Pub/Sub endpoint](https://cloud.google.com/pubsub/docs/reference/service_apis_overview#service_endpoints) to use. |
| kafka.topic | String | REQUIRED (No default) | The topic in Kafka which will receive messages that were pulled from Cloud Pub/Sub. |
| cps.maxBatchSize | Integer | 100 | The maximum number of messages to batch per pull request to Cloud Pub/Sub. |
| cps.makeOrderingKeyAttribute | Boolean | false | When true, copy the ordering key to the set of attributes set in the Kafka message. |
| kafka.key.attribute | String | null | The Cloud Pub/Sub message attribute to use as a key for messages published to Kafka. If set to "orderingKey", use the message's ordering key. |
| kafka.partition.count | Integer | 1 | The number of Kafka partitions for the Kafka topic in which messages will be published to. NOTE: this parameter is ignored if partition scheme is "kafka_partitioner".|
| kafka.partition.scheme | round_robin, hash_key, hash_value, kafka_partitioner, ordering_key | round_robin | The scheme for assigning a message to a partition in Kafka. The scheme "round_robin" assigns partitions in a round robin fashion, while the schemes "hash_key" and "hash_value" find the partition by hashing the message key and message value respectively. "kafka_partitioner" scheme delegates partitioning logic to kafka producer, which by default detects number of partitions automatically and performs either murmur hash based partition mapping or round robin depending on whether message key is provided or not. "ordering_key" uses the hash code of a message's ordering key. If no ordering key is present, uses "round_robin".|
| gcp.credentials.file.path | String | Optional | The file path, which stores GCP credentials.| If not defined, GOOGLE_APPLICATION_CREDENTIALS env is used. |
| gcp.credentials.json | String | Optional | GCP credentials JSON blob | If specified, use the explicitly handed credentials. Consider using the externalized secrets feature in Kafka Connect for passing the value. |
| kafka.record.headers | Boolean | false | Use Kafka record headers to store Pub/Sub message attributes |

#### Sink Connector

| Config | Value Range | Default | Description |
|---------------|-------------|-----------------------|------------------------------------------------------------------------------------------------------------------------------------|
| cps.topic | String | REQUIRED (No default) | The topic in Cloud Pub/Sub to publish to, e.g. "foo" for topic "/projects/bar/topics/foo". |
| cps.project | String | REQUIRED (No default) | The project in Cloud Pub/Sub containing the topic, e.g. "bar" from above. |
| cps.endpoint | String | "pubsub.googleapis.com:443" | The [Cloud Pub/Sub endpoint](https://cloud.google.com/pubsub/docs/reference/service_apis_overview#service_endpoints) to use. |
| maxBufferSize | Integer | 100 | The maximum number of messages that can be received for the messages on a topic partition before publishing them to Cloud Pub/Sub. |
| maxBufferBytes | Long | 10000000 | The maximum number of bytes that can be received for the messages on a topic partition before publishing them to Cloud Pub/Sub. |
| maxOutstandingRequestBytes | Long | Long.MAX_VALUE | The maximum number of outstanding bytes for incomplete and unsent messages before the publisher will block further publishing. |
| maxOutstandingElementCount | Long | Long.MAX_VALUE | The maximum number of outstanding elements for incomplete and unsent messages before the publisher will block further publishing. |
| maxDelayThresholdMs | Integer | 100 | The maximum amount of time to wait to reach maxBufferSize or maxBufferBytes before publishing outstanding messages to Cloud Pub/Sub. |
| maxRequestTimeoutMs | Integer | 10000 | The timeout for individual publish requests to Cloud Pub/Sub. |
| maxTotalTimeoutMs | Integer | 60000| The total timeout for a call to publish (including retries) to Cloud Pub/Sub. |
| gcp.credentials.file.path | String | Optional | The file path, which stores GCP credentials.| If not defined, GOOGLE_APPLICATION_CREDENTIALS env is used. |
| gcp.credentials.json | String | Optional | GCP credentials JSON blob | If specified, use the explicitly handed credentials. Consider using the externalized secrets feature in Kafka Connect for passing the value. |
| metadata.publish | Boolean | false | When true, include the Kafka topic, partition, offset, and timestamp as message attributes when a message is published to Cloud Pub/Sub. |
| headers.publish | Boolean | false | When true, include any headers as attributes when a message is published to Cloud Pub/Sub. |
| orderingKeySource | String (none, key, partition) | none | When set to "none", do not set the ordering key. When set to "key", uses a message's key as the ordering key. If set to "partition", converts the partition number to a String and uses that as the ordering key. Note that using "partition" should only be used for low-throughput topics or topics with thousands of partitions. |

### PubSubLiteConnector Configs

In addition to the configs supplied by the Kafka Connect API, the Pub/Sub Lite
Connector supports the following configs:

#### Source Connector

| Config | Value Range | Default | Description |
|---------------|-------------|-----------------------|------------------------------------------------------------------------------------------------------------------------------------|
| pubsublite.subscription | String | REQUIRED (No default) | The name of the subscription to Pub/Sub Lite, e.g. "sub" for subscription "/projects/bar/locations/europe-south7-q/subscriptions/sub". |
| pubsublite.project | String | REQUIRED (No default) | The project in Pub/Sub Lite containing the subscription, e.g. "bar" from above. |
| pubsublite.location | String | REQUIRED (No default) | The location in Pub/Sub Lite containing the subscription, e.g. "europe-south7-q" from above. |
| kafka.topic | String | REQUIRED (No default) | The topic in Kafka which will receive messages that were pulled from Pub/Sub Lite. |
| pubsublite.partition_flow_control.messages | Long | Long.MAX_VALUE | The maximum number of outstanding messages per Pub/Sub Lite partition. |
| pubsublite.partition_flow_control.bytes | Long | 20,000,000 | The maximum number of outstanding bytes per Pub/Sub Lite partition. |

#### Sink Connector

| Config | Value Range | Default | Description |
|---------------|-------------|-----------------------|------------------------------------------------------------------------------------------------------------------------------------|
| pubsublite.topic | String | REQUIRED (No default) | The topic in Pub/Sub Lite to publish to, e.g. "foo" for topic "/projects/bar/locations/europe-south7-q/topics/foo". |
| pubsublite.project | String | REQUIRED (No default) | The project in Pub/Sub Lite containing the topic, e.g. "bar" from above. |
| pubsublite.location | String | REQUIRED (No default) | The location in Pub/Sub Lite containing the topic, e.g. "europe-south7-q" from above. |

### Schema Support and Data Model

#### Cloud Pub/Sub Connector

A pubsub message has two main parts: the message body and attributes. The
message body is a [ByteString](https://developers.google.com/protocol-buffers/docs/reference/java/com/google/protobuf/ByteString)
object that translates well to and from the byte[]
bodies of Kafka messages. For this reason, we recommend using a converter that
produces primitive data types (i.e. integer, float, string, or bytes schema)
where possible to prevent deserializing and reserializing the same message body.

Additionally, a Pubsub message size cannot exceed 10MB, so please check
your broker's message.max.bytes configuration to prevent possible errors.

The sink connector handles the conversion in the following way:

*   For integer, float, string, and bytes schemas, the bytes of the Kafka
    message's value are passed directly into the Pubsub message body.
*   For map and struct types, the values are stored in attributes. Pubsub only
    supports string to string mapping in attributes. To make the connector as
    versatile as possible, the toString() method will be called on whatever
    object passed in as the key or value for a map and the value for a struct.
    *   One additional feature is we allow a specification of a particular
        field or key to be placed in the Pubsub message body. To do so, set the
        messageBodyName configuration with the struct field or map key.
        This value is stored as a ByteString, and any integer, byte, float, or
        array type is included in the message body as if it were the sole value.
*   For arrays, we only support primitive array types due to potential
    collisions of field names or keys of a struct/map array. The connector
    handles arrays in a fairly predictable fashion, each value is concatenated
    together into a ByteString object.
*   In all cases, the Kafka key value is stored in the Pubsub message's
    attributes as a string, currently "key".
    
> **IMPORTANT NOTICE:** There are three limitations to keep in mind when using Pubsub
message attributes as stated on its [documentation](https://cloud.google.com/pubsub/quotas#resource_limits)
>* *"Attributes per message: 100"*
>* *"Attribute key size: 256 bytes"*
>* *"Attribute value size: 1024 bytes"*
>
>If you enable copy of Kafka headers as Pubsub message attribute (it is disabled by default), the connector will copy 
>only those headers meeting these limitations and will skip those that do not.


The source connector takes a similar approach in handling the conversion
from a Pubsub message into a SourceRecord with a relevant Schema.

*   The connector searches for the given kafka.key.attribute in the
    attributes of the Pubsub message. If found, this will be used as the Kafka
    key with a string schema type. Otherwise, it will be set to null.
*   If the Pubsub message doesn't have any other attributes, the message body
    is stored as a byte[] for the Kafka message's value.
*   However, if there are attributes beyond the Kafka key, the value is assigned
    a struct schema. Each key in the Pubsub message's attributes map becomes a
    field name, with the values set accordingly with string schemas. In this
    case, the Pubsub message body is identified by the field name set in
    "message", and has the schema types bytes.
    *   In these cases, to carry forward the structure of data stored in
        attributes, we recommend using a converter that can represent a struct
        schema type in a useful way, e.g. JsonConverter.
        
        
#### Pub/Sub Lite Connector

Pub/Sub Lite's messages have the following structure:

```java
class Message {
  ByteString key;
  ByteString data;
  ListMultimap<String, ByteString> attributes;
  Optional<Timestamp> eventTime;
}
```
 
This maps quite closely to the SinkRecord class, except for serialization. The
table below shows how each field in SinkRecord will be mapped to the underlying
message:

| SinkRecord | Message |
|---|---|
| key{Schema} | key |
| value{Schema} | data |
| headers | attributes |
| topic | attributes["x-goog-pubsublite-source-kafka-topic"] |
| kafkaPartition | attributes["x-goog-pubsublite-source-kafka-partition"] |
| kafkaOffset | attributes["x-goog-pubsublite-source-kafka-offset"] |
| timestamp | eventTime |
| timestampType | attributes["x-goog-pubsublite-source-kafka-event-time-type"] |

When a key, value or header value with a schema is encoded as a ByteString, the
following logic will be used:

- null schemas are treated as Schema.STRING_SCHEMA
- Top level BYTES payloads are unmodified.
- Top level STRING payloads are encoded using copyFromUtf8.
- Top level Integral payloads are converted using
copyFromUtf8(Long.toString(x.longValue()))
- Top level Floating point payloads are converted using
copyFromUtf8(Double.toString(x.doubleValue()))
- All other payloads are encoded into a protobuf Value, then converted to a ByteString.
  - Nested STRING fields are encoded into a protobuf Value.
  - Nested BYTES fields are encoded to a protobuf Value holding the base64 encoded bytes.
  - Nested Numeric fields are encoded as a double into a protobuf Value.
  - Maps with Array, Map, or Struct keys are not supported.
    - BYTES keys in maps are base64 encoded.
    - Integral keys are converted using Long.toString(x.longValue())
    - Floating point keys are converted using Double.toString(x.doubleValue())
    
The source connector will perform a one to one mapping from SequencedMessage
fields to their SourceRecord counterparts.

In addition, empty `message.key` fields will be converted to `null` and assigned
round-robin to kafka partitions. Messages with identical, non-empty keys will be
routed to the same kafka partition.

| SequencedMessage | SourceRecord field | SourceRecord schema |
|---|---|---|
| message.key | key | BYTES |
| message.data | value | BYTES |
| message.attributes | headers | BYTES |
| <source topic> | sourcePartition["topic"] | String field in map |
| <source partition> | sourcePartition["partition"] | Integer field in map |
| cursor.offset | sourceOffset["offset"] | Long field in map |
| message.event_time | timestamp | long milliseconds since unix epoch if present |
| publish_time | timestamp | long milliseconds since unix epoch if no event_time exists |


### Building

These instructions assume you are using [Maven](https://maven.apache.org/).

1.  If you want to build the connector from head, clone the repository, ensuring
    to do so recursively to pick up submodules:

    `git clone --recursive https://github.com/GoogleCloudPlatform/pubsub`

    If you wish to build from a released version of the connector, download it
    from the [Releases section](https://github.com/GoogleCloudPlatform/pubsub/releases)
    in GitHub.

2.  Unzip the source code if downloaded from the release version.

3.  Go into the kafka-connector directory in the cloned repo or downloaded
    release.

4.  Make the jar that contains the connector:

    `mvn package`

The resulting jar is at target/pubsub-kafka-connector.jar.
