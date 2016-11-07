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
package com.google.pubsub.kafka.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.kafka.source.CloudPubSubSourceConnector.PartitionScheme;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SourceTask} used by a {@link CloudPubSubSourceConnector} to write messages to <a
 * href="http://kafka.apache.org/">Apache Kafka</a>.
 */
public class CloudPubSubSourceTask extends SourceTask {

  private static final Logger log = LoggerFactory.getLogger(CloudPubSubSourceTask.class);
  private static final int NUM_CPS_SUBSCRIBERS = 10;

  private String kafkaTopic;
  private String cpsSubscription;
  private String kafkaMessageKeyAttribute;
  private int kafkaPartitions;
  private PartitionScheme kafkaPartitionScheme;
  private int cpsMaxBatchSize;
  // Keeps track of the current partition to publish to if the partition scheme is round robin.
  private int currentRoundRobinPartition = -1;
  // Keep track of all ack ids that have not been sent correctly acked yet.
  private Set<String> ackIds = Collections.synchronizedSet(new HashSet<>());
  private CloudPubSubSubscriber subscriber;
  private Converter keyConverter;
  private Converter valueConverter;
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

  public CloudPubSubSourceTask() {}

  @VisibleForTesting
  public CloudPubSubSourceTask(CloudPubSubSubscriber subscriber) {
    this.subscriber = subscriber;
  }

  @Override
  public String version() {
    return new CloudPubSubSourceConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    Map<String, Object> validatedProps = new CloudPubSubSourceConnector().config().parse(props);
    cpsSubscription =
        String.format(
            ConnectorUtils.CPS_SUBSCRIPTION_FORMAT,
            validatedProps.get(ConnectorUtils.CPS_PROJECT_CONFIG).toString(),
            validatedProps.get(CloudPubSubSourceConnector.CPS_SUBSCRIPTION_CONFIG).toString());
    kafkaTopic = validatedProps.get(CloudPubSubSourceConnector.KAFKA_TOPIC_CONFIG).toString();
    cpsMaxBatchSize =
        (Integer) validatedProps.get(CloudPubSubSourceConnector.CPS_MAX_BATCH_SIZE_CONFIG);
    kafkaPartitions =
        (Integer) validatedProps.get(CloudPubSubSourceConnector.KAFKA_PARTITIONS_CONFIG);
    kafkaMessageKeyAttribute =
        (String) validatedProps.get(CloudPubSubSourceConnector.KAFKA_MESSAGE_KEY_CONFIG);
    kafkaPartitionScheme =
        PartitionScheme.getEnum(
            (String) validatedProps.get(CloudPubSubSourceConnector.KAFKA_PARTITION_SCHEME_CONFIG));
    if (subscriber == null) {
      // Only do this if we did not set through the constructor.
      subscriber = new CloudPubSubRoundRobinSubscriber(NUM_CPS_SUBSCRIBERS);
    }
    keyConverter = getConverterInstance(
        validatedProps, CloudPubSubSourceConnector.KEY_CONVERTER_CLASS_CONFIG);
    valueConverter = getConverterInstance(
        validatedProps, CloudPubSubSourceConnector.VALUE_CONVERTER_CLASS_CONFIG);
    log.info("Started a CloudPubSubSourceTask.");
  }
  
  public Converter getConverterInstance(Map<String, Object> props, String configKey) {
    Class<?> c = (Class<?>) props.get(configKey);
    Object o = Utils.newInstance(c);
    if (!(o instanceof Converter))
      throw new ConfigException(c.getName() + " is not an instance of Converter");
    return (Converter) o;
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    log.debug("Polling...");
    ackMessages();
    PullRequest request =
        PullRequest.newBuilder()
            .setSubscription(cpsSubscription)
            .setReturnImmediately(false)
            .setMaxMessages(cpsMaxBatchSize)
            .build();
    try {
      PullResponse response = subscriber.pull(request).get();
      List<SourceRecord> sourceRecords = new ArrayList<>();
      for (ReceivedMessage rm : response.getReceivedMessagesList()) {
        PubsubMessage message = rm.getMessage();
        String ackId = rm.getAckId();
        // If we are receiving this message a second (or more) times because the ack for it failed
        // then do not create a SourceRecord for this message.
        rwLock.readLock().lock();
        if (ackIds.contains(ackId)) {
          rwLock.readLock().unlock();
          continue;
        }
        rwLock.readLock().unlock();
        rwLock.writeLock().lock();
        ackIds.add(ackId);
        rwLock.writeLock().unlock();
        Map<String, String> messageAttributes = message.getAttributes();
        byte[] keyBytes = null;
        if (messageAttributes.get(kafkaMessageKeyAttribute) != null) {
          keyBytes = messageAttributes.get(kafkaMessageKeyAttribute).getBytes();
        }
        byte[] valueBytes = message.getData().toByteArray();
        SchemaAndValue key = null;
        if (keyBytes != null) {
          key = keyConverter.toConnectData(kafkaTopic, keyBytes);
        }
        SchemaAndValue value = valueConverter.toConnectData(kafkaTopic, valueBytes);
        SourceRecord record;
        if (key != null) {
          record = new SourceRecord(
              null,
              null,
              kafkaTopic,
              selectPartition(key, value),
              key.schema(),
              key.value(),
              value.schema(),
              value.value());
        } else {
          record = new SourceRecord(
              null,
              null,
              kafkaTopic,
              selectPartition(key, value),
              value.schema(),
              value.value());
        }
        sourceRecords.add(record);
      }
      return sourceRecords;
    } catch (Exception e) {
      // Kafka Connect suppresses any indication of an InterruptedException
      // so we have to throw a RuntimeException.
      throw new RuntimeException(e.getMessage());
    }
  }

  /**
   * Attempt to ack all ids in {@link #ackIds}. If the ack request was unsuccessful then do not
   * clear the list of acks. Instead, wait for the next call to this function to ack those ids.
   */
  private void ackMessages() {
    if (ackIds.size() != 0) {
      AcknowledgeRequest request =
          AcknowledgeRequest.newBuilder()
              .setSubscription(cpsSubscription)
              .addAllAckIds(ackIds)
              .build();
      ListenableFuture<Empty> response = subscriber.ackMessages(request);
      Futures.addCallback(
          response,
          new FutureCallback<Empty>() {
            @Override
            public void onSuccess(Empty result) {
              log.trace("Successfully acked a set of messages.");
              ackIds.clear();
            }

            @Override
            public void onFailure(Throwable t) {
              log.error("An exception occurred acking messages. Will try to ack messages again.");
            }
          });
    }
  }

  /** Return the partition a message should go to based on {@link #kafkaPartitionScheme}. */
  private int selectPartition(Object key, Object value) {
    if (kafkaPartitionScheme.equals(PartitionScheme.HASH_KEY)) {
      return key == null ? 0 : key.hashCode() % kafkaPartitions;
    } else if (kafkaPartitionScheme.equals(PartitionScheme.HASH_VALUE)) {
      return value.hashCode() % kafkaPartitions;
    } else {
      currentRoundRobinPartition = ++currentRoundRobinPartition % kafkaPartitions;
      return currentRoundRobinPartition;
    }
  }

  @Override
  public void stop() {}
}
