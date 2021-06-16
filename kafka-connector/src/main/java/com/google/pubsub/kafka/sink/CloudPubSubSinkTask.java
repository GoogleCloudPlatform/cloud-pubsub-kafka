// Copyright 2016 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////
package com.google.pubsub.kafka.sink;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.kafka.common.ConnectorCredentialsProvider;
import com.google.pubsub.kafka.sink.CloudPubSubSinkConnector.OrderingKeySource;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/**
 * A {@link SinkTask} used by a {@link CloudPubSubSinkConnector} to write messages to <a
 * href="https://cloud.google.com/pubsub">Google Cloud Pub/Sub</a>.
 */
public class CloudPubSubSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(CloudPubSubSinkTask.class);

  // Maps a topic to another map which contains the outstanding futures per partition
  private Map<String, Map<Integer, OutstandingFuturesForPartition>> allOutstandingFutures =
      new HashMap<>();
  private String cpsProject;
  private String cpsTopic;
  private String cpsEndpoint;
  private String messageBodyName;
  private long maxBufferSize;
  private long maxBufferBytes;
  private long maxOutstandingRequestBytes;
  private long maxOutstandingElementCount;
  private int maxDelayThresholdMs;
  private int maxRequestTimeoutMs;
  private int maxTotalTimeoutMs;
  private int maxShutdownTimeoutMs;
  private boolean includeMetadata;
  private boolean includeHeaders;
  private OrderingKeySource orderingKeySource;
  private ConnectorCredentialsProvider gcpCredentialsProvider;
  private com.google.cloud.pubsub.v1.Publisher publisher;



  /** Holds a list of the publishing futures that have not been processed for a single partition. */
  private class OutstandingFuturesForPartition {
    public List<ApiFuture<String>> futures = new ArrayList<>();
  }

  /**
   * Holds a list of the unpublished messages for a single partition and the total size in bytes of
   * the messages in the list.
   */
  private class UnpublishedMessagesForPartition {
    public List<PubsubMessage> messages = new ArrayList<>();
    public int size = 0;
  }

  public CloudPubSubSinkTask() {}

  @VisibleForTesting
  public CloudPubSubSinkTask(Publisher publisher) {
    this.publisher = publisher;
  }

  @Override
  public String version() {
    return new CloudPubSubSinkConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    Map<String, Object> validatedProps = new CloudPubSubSinkConnector().config().parse(props);
    cpsProject = validatedProps.get(ConnectorUtils.CPS_PROJECT_CONFIG).toString();
    cpsTopic = validatedProps.get(ConnectorUtils.CPS_TOPIC_CONFIG).toString();
    cpsEndpoint = validatedProps.get(ConnectorUtils.CPS_ENDPOINT).toString();
    maxBufferSize = (Integer) validatedProps.get(CloudPubSubSinkConnector.MAX_BUFFER_SIZE_CONFIG);
    maxBufferBytes = (Long) validatedProps.get(CloudPubSubSinkConnector.MAX_BUFFER_BYTES_CONFIG);
    maxOutstandingRequestBytes =
        (Long) validatedProps.get(CloudPubSubSinkConnector.MAX_OUTSTANDING_REQUEST_BYTES);
    maxOutstandingElementCount =
        (Long) validatedProps.get(CloudPubSubSinkConnector.MAX_OUTSTANDING_ELEMENT_COUNT);
    maxDelayThresholdMs =
        (Integer) validatedProps.get(CloudPubSubSinkConnector.MAX_DELAY_THRESHOLD_MS);
    maxRequestTimeoutMs =
        (Integer) validatedProps.get(CloudPubSubSinkConnector.MAX_REQUEST_TIMEOUT_MS);
    maxTotalTimeoutMs =
        (Integer) validatedProps.get(CloudPubSubSinkConnector.MAX_TOTAL_TIMEOUT_MS);
    maxShutdownTimeoutMs =
        (Integer) validatedProps.get(CloudPubSubSinkConnector.MAX_SHUTDOWN_TIMEOUT_MS);
    messageBodyName = (String) validatedProps.get(CloudPubSubSinkConnector.CPS_MESSAGE_BODY_NAME);
    includeMetadata = (Boolean) validatedProps.get(CloudPubSubSinkConnector.PUBLISH_KAFKA_METADATA);
    includeHeaders = (Boolean) validatedProps.get(CloudPubSubSinkConnector.PUBLISH_KAFKA_HEADERS);
    orderingKeySource =
        OrderingKeySource.getEnum(
            (String) validatedProps.get(CloudPubSubSinkConnector.ORDERING_KEY_SOURCE));
    gcpCredentialsProvider = new ConnectorCredentialsProvider();
    String credentialsPath = (String) validatedProps.get(ConnectorUtils.GCP_CREDENTIALS_FILE_PATH_CONFIG);
    String credentialsJson = (String) validatedProps.get(ConnectorUtils.GCP_CREDENTIALS_JSON_CONFIG);
    if (credentialsPath != null) {
      try {
        gcpCredentialsProvider.loadFromFile(credentialsPath);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else if (credentialsJson != null) {
      try {
        gcpCredentialsProvider.loadJson(credentialsJson);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    if (publisher == null) {
      // Only do this if we did not use the constructor.
      createPublisher();
    }
    log.info("Start CloudPubSubSinkTask");
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    log.debug("Received " + sinkRecords.size() + " messages to send to CPS.");
    for (SinkRecord record : sinkRecords) {
      log.trace("Received record: " + record.toString());
      Map<String, String> attributes = new HashMap<>();
      ByteString value = handleValue(record.valueSchema(), record.value(), attributes);
      String key = null;
      String partition = record.kafkaPartition().toString();
      if (record.key() != null) {
        key = record.key().toString();
        attributes.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, key);
      }
      if (includeMetadata) {
        attributes.put(ConnectorUtils.KAFKA_TOPIC_ATTRIBUTE, record.topic());
        attributes.put(
            ConnectorUtils.KAFKA_PARTITION_ATTRIBUTE, partition);
        attributes.put(ConnectorUtils.KAFKA_OFFSET_ATTRIBUTE, Long.toString(record.kafkaOffset()));
        if (record.timestamp() != null) {
          attributes.put(ConnectorUtils.KAFKA_TIMESTAMP_ATTRIBUTE, record.timestamp().toString());
        }
      }
      if (includeHeaders) {
        for (Header header : getRecordHeaders(record)) {
          attributes.put(header.key(), header.value().toString());
        }
      }
      if (attributes.size() == 0 && value == null) {
        log.warn("Message received with no value and no attributes. Not publishing message");
        SettableApiFuture<String> nullMessageFuture = SettableApiFuture.<String>create();
        nullMessageFuture.set("No message");
        addPendingMessageFuture(record.topic(), record.kafkaPartition(), nullMessageFuture);
        continue;
      }
      PubsubMessage.Builder builder = PubsubMessage.newBuilder();
      builder.putAllAttributes(attributes);
      if (value != null) {
        builder.setData(value);
      }
      if (orderingKeySource == OrderingKeySource.KEY && key != null && !key.isEmpty()) {
        builder.setOrderingKey(key);
      } else if (orderingKeySource == OrderingKeySource.PARTITION) {
        builder.setOrderingKey(partition);
      }

      PubsubMessage message = builder.build();
      publishMessage(record.topic(), record.kafkaPartition(), message);
    }
  }

  private Iterable<? extends Header> getRecordHeaders(SinkRecord record) {
    ConnectHeaders headers = new ConnectHeaders();
    if(record.headers() != null) {
      int headerCount = 0;
      for (Header header : record.headers()) {
        if (header.key().getBytes().length < 257 &&
            String.valueOf(header.value()).getBytes().length < 1025) {
          headers.add(header);
          headerCount++;
        }
        if (headerCount > 100) {
          break;
        }
      }
    }
    return headers;
  }

  private ByteString handleValue(Schema schema, Object value, Map<String, String> attributes) {
    if (value == null) {
      return null;
    }
    if (schema == null) {
      String str = value.toString();
      return ByteString.copyFromUtf8(str);
    }
    Schema.Type t = schema.type();
    switch (t) {
      case INT8:
        byte b = (Byte) value;
        byte[] arr = {b};
        return ByteString.copyFrom(arr);
      case INT16:
        ByteBuffer shortBuf = ByteBuffer.allocate(2);
        shortBuf.putShort((Short) value);
        return ByteString.copyFrom(shortBuf);
      case INT32:
        ByteBuffer intBuf = ByteBuffer.allocate(4);
        intBuf.putInt((Integer) value);
        return ByteString.copyFrom(intBuf);
      case INT64:
        ByteBuffer longBuf = ByteBuffer.allocate(8);
        longBuf.putLong((Long) value);
        return ByteString.copyFrom(longBuf);
      case FLOAT32:
        ByteBuffer floatBuf = ByteBuffer.allocate(4);
        floatBuf.putFloat((Float) value);
        return ByteString.copyFrom(floatBuf);
      case FLOAT64:
        ByteBuffer doubleBuf = ByteBuffer.allocate(8);
        doubleBuf.putDouble((Double) value);
        return ByteString.copyFrom(doubleBuf);
      case BOOLEAN:
        byte bool = (byte) ((Boolean) value ? 1 : 0);
        byte[] boolArr = {bool};
        return ByteString.copyFrom(boolArr);
      case STRING:
        String str = (String) value;
        return ByteString.copyFromUtf8(str);
      case BYTES:
        if (value instanceof ByteString) {
          return (ByteString) value;
        } else if (value instanceof byte[]) {
          return ByteString.copyFrom((byte[]) value);
        } else if (value instanceof ByteBuffer) {
          return ByteString.copyFrom((ByteBuffer) value);
        } else {
          throw new DataException("Unexpected value class with BYTES schema type.");
        }
      case STRUCT:
        Struct struct = (Struct) value;
        ByteString msgBody = null;
        for (Field f : schema.fields()) {
          Schema.Type fieldType = f.schema().type();
          if (fieldType == Type.MAP || fieldType == Type.STRUCT) {
            throw new DataException("Struct type does not support nested Map or Struct types, " +
                "present in field " + f.name());
          }

          Object val = struct.get(f);
          if (val == null) {
            if (!f.schema().isOptional()) {
              throw new DataException("Struct message missing required field " + f.name());
            }  else {
              continue;
            }
          }
          if (f.name().equals(messageBodyName)) {
            Schema bodySchema = f.schema();
            msgBody = handleValue(bodySchema, val, null);
          } else {
            attributes.put(f.name(), val.toString());
          }
        }
        if (msgBody != null) {
          return msgBody;
        } else {
          return ByteString.EMPTY;
        }
      case MAP:
        Map<Object, Object> map = (Map<Object, Object>) value;
        Set<Object> keys = map.keySet();
        ByteString mapBody = null;
        for (Object key : keys) {
          if (key.equals(messageBodyName)) {
            mapBody = ByteString.copyFromUtf8(map.get(key).toString());
          } else {
            attributes.put(key.toString(), map.get(key).toString());
          }
        }
        if (mapBody != null) {
          return mapBody;
        } else {
          return ByteString.EMPTY;
        }
      case ARRAY:
        Schema.Type arrType = schema.valueSchema().type();
        if (arrType == Type.MAP || arrType == Type.STRUCT) {
          throw new DataException("Array type does not support Map or Struct types.");
        }
        ByteString out = ByteString.EMPTY;
        Object[] objArr = (Object[]) value;
        for (Object o : objArr) {
          out = out.concat(handleValue(schema.valueSchema(), o, null));
        }
        return out;
    }
    return ByteString.EMPTY;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> partitionOffsets) {
    log.debug("Flushing...");
    // Process results of all the outstanding futures specified by each TopicPartition.
    for (Map.Entry<TopicPartition, OffsetAndMetadata> partitionOffset :
        partitionOffsets.entrySet()) {
      log.trace("Received flush for partition " + partitionOffset.getKey().toString());
      Map<Integer, OutstandingFuturesForPartition> outstandingFuturesForTopic =
          allOutstandingFutures.get(partitionOffset.getKey().topic());
      if (outstandingFuturesForTopic == null) {
        continue;
      }
      OutstandingFuturesForPartition outstandingFutures =
          outstandingFuturesForTopic.get(partitionOffset.getKey().partition());
      if (outstandingFutures == null) {
        continue;
      }
      try {
        ApiFutures.allAsList(outstandingFutures.futures).get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        outstandingFutures.futures.clear();
      }
    }
    allOutstandingFutures.clear();
  }

  /** Publish all the messages in a partition and store the Future's for each publish request. */
  private void publishMessage(String topic, Integer partition, PubsubMessage message) {
    addPendingMessageFuture(topic, partition, publisher.publish(message));
  }

  private void addPendingMessageFuture(String topic, Integer partition, ApiFuture<String> future) {
    // Get a map containing all futures per partition for the passed in topic.
    Map<Integer, OutstandingFuturesForPartition> outstandingFuturesForTopic =
        allOutstandingFutures.get(topic);
    if (outstandingFuturesForTopic == null) {
      outstandingFuturesForTopic = new HashMap<>();
      allOutstandingFutures.put(topic, outstandingFuturesForTopic);
    }
    // Get the object containing the outstanding futures for this topic and partition..
    OutstandingFuturesForPartition outstandingFutures = outstandingFuturesForTopic.get(partition);
    if (outstandingFutures == null) {
      outstandingFutures = new OutstandingFuturesForPartition();
      outstandingFuturesForTopic.put(partition, outstandingFutures);
    }
    outstandingFutures.futures.add(future);
  }

  private void createPublisher() {
    ProjectTopicName fullTopic = ProjectTopicName.of(cpsProject, cpsTopic);
    com.google.cloud.pubsub.v1.Publisher.Builder builder =
        com.google.cloud.pubsub.v1.Publisher.newBuilder(fullTopic)
            .setCredentialsProvider(gcpCredentialsProvider)
            .setBatchingSettings(
                BatchingSettings.newBuilder()
                    .setDelayThreshold(Duration.ofMillis(maxDelayThresholdMs))
                    .setElementCountThreshold(maxBufferSize)
                    .setRequestByteThreshold(maxBufferBytes)
                    .setFlowControlSettings(FlowControlSettings.newBuilder()
                        .setMaxOutstandingRequestBytes(maxOutstandingRequestBytes)
                        .setMaxOutstandingElementCount(maxOutstandingElementCount)
                        .setLimitExceededBehavior(FlowController.LimitExceededBehavior.Block)
                        .build())
                    .build())
            .setRetrySettings(
                RetrySettings.newBuilder()
                    // All values that are not configurable come from the defaults for the publisher
                    // client library.
                    .setTotalTimeout(Duration.ofMillis(maxTotalTimeoutMs))
                    .setMaxRpcTimeout(Duration.ofMillis(maxRequestTimeoutMs))
                    .setInitialRetryDelay(Duration.ofMillis(5))
                    .setRetryDelayMultiplier(2)
                    .setMaxRetryDelay(Duration.ofMillis(Long.MAX_VALUE))
                    .setInitialRpcTimeout(Duration.ofSeconds(10))
                    .setRpcTimeoutMultiplier(2)
                    .build())
            .setEndpoint(cpsEndpoint);
    if (orderingKeySource != OrderingKeySource.NONE) {
      builder.setEnableMessageOrdering(true); 
    }
    try {
      publisher = builder.build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    log.info("Stopping CloudPubSubSinkTask");

    if (publisher != null) {
      log.info("Shutting down PubSub publisher");
      try {
        publisher.shutdown();
        boolean terminated = publisher.awaitTermination(maxShutdownTimeoutMs, TimeUnit.MILLISECONDS);
        if (!terminated) {
          log.warn(String.format("PubSub publisher did not terminate cleanly in %d ms", maxShutdownTimeoutMs));
        }
      } catch (Exception e) {
        // There is not much we can do here besides logging it as an error
        log.error("An exception occurred while shutting down PubSub publisher", e);
      }
    }
  }
}
