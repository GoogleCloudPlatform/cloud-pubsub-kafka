// Copyright 2017 Google Inc.
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

package com.google.pubsub.clients.producer;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.gax.core.RpcFuture;
import com.google.api.gax.core.RpcFutureCallback;
import com.google.api.gax.grpc.BundlingSettings;
import com.google.api.gax.grpc.FlowControlSettings;
import com.google.cloud.pubsub.spi.v1.Publisher;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.serialization.Serializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Kafka client that publishes records to Google Cloud Pub/Sub.
 */
public class PubsubProducer<K, V> implements Producer<K, V> {

  private static final Logger log = LoggerFactory.getLogger(PubsubProducer.class);
  private static final long DEFAULT_ELEMENT_COUNT_THRESHOLD = 950L;

  private final String project;
  private final String topic;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final long batchSize;
  private final int bufferMemory;
  private final boolean isAcks;
  private final int maxRequestSize;
  private final long lingerMs;
  private Publisher publisher;

  private AtomicBoolean closed;

  private PubsubProducer(Builder builder) {
    project = builder.project;
    topic = builder.topic;
    batchSize = builder.batchSize;
    isAcks = builder.isAcks;
    maxRequestSize = builder.maxRequestSize;
    keySerializer = builder.keySerializer;
    valueSerializer = builder.valueSerializer;
    lingerMs = builder.lingerMs;
    bufferMemory = builder.bufferMemory;
    publisher = newPublisher();
    closed = new AtomicBoolean(false);
  }

  public PubsubProducer(Map<String, Object> configs) {
    this(new PubsubProducerConfig(configs), null, null);
  }

  public PubsubProducer(Map<String, Object> configs, Serializer<K> keySerializer,
      Serializer<V> valueSerializer) {
    this(new PubsubProducerConfig(
            PubsubProducerConfig.addSerializerToConfig(configs, keySerializer, valueSerializer)),
        keySerializer, valueSerializer);
  }

  public PubsubProducer(Properties properties) {
    this(new PubsubProducerConfig(properties), null, null);
  }

  public PubsubProducer(Properties properties, Serializer<K> keySerializer,
      Serializer<V> valueSerializer) {
    this(new PubsubProducerConfig(
            PubsubProducerConfig.addSerializerToConfig(properties, keySerializer, valueSerializer)),
        keySerializer, valueSerializer);
  }

  private PubsubProducer(PubsubProducerConfig configs, Serializer<K> keySerializer,
      Serializer<V> valueSerializer) {
    try {
      log.trace("Starting the Pubsub producer");

      if (keySerializer == null) {
        this.keySerializer =
            configs.getConfiguredInstance(
                PubsubProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);
        this.keySerializer.configure(configs.originals(), true);
      } else {
        configs.ignore(PubsubProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        this.keySerializer = keySerializer;
      }

      if (valueSerializer == null) {
        this.valueSerializer = configs.getConfiguredInstance(
            PubsubProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);
        this.valueSerializer.configure(configs.originals(), false);
      } else {
        configs.ignore(PubsubProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        this.valueSerializer = valueSerializer;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    batchSize = configs.getLong(PubsubProducerConfig.BATCH_SIZE_CONFIG);
    isAcks = configs.getString(PubsubProducerConfig.ACKS_CONFIG).matches("1|all");
    project = configs.getString(PubsubProducerConfig.PROJECT_CONFIG);
    topic = configs.getString(PubsubProducerConfig.TOPIC_CONFIG);
    maxRequestSize = configs.getInt(PubsubProducerConfig.MAX_REQUEST_SIZE_CONFIG);
    lingerMs = configs.getLong(PubsubProducerConfig.LINGER_MS_CONFIG);
    bufferMemory = configs.getInt(PubsubProducerConfig.BUFFER_MEMORY_CONFIG);
    publisher = newPublisher();
    closed = new AtomicBoolean(false);
    log.debug("Producer successfully initialized.");
  }

  private Publisher newPublisher() {
    TopicName topicName = TopicName.create(project, topic);
    Publisher newPub = null;
    try {
      newPub = Publisher.newBuilder(topicName)
          .setBundlingSettings(BundlingSettings.newBuilder()
              .setElementCountThreshold(DEFAULT_ELEMENT_COUNT_THRESHOLD)
              .setDelayThreshold(Duration.millis(lingerMs))
              .setRequestByteThreshold(batchSize)
              .build())
          .setFlowControlSettings(FlowControlSettings.newBuilder()
              .setMaxOutstandingRequestBytes(bufferMemory)
              .build())
          .build();
    } catch (IOException e) {
      log.error("Exception occurred: " + e);
    }
    return newPub;
  }

  /**
   * Sends the given record.
   */
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    return send(record, null);
  }

  /**
   * Sends the given record and invokes the specified callback.
   * The given record must have the same topic as the producer.
   */
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    log.trace("Received " + record.toString());
    if (closed.get()) {
      throw new RuntimeException("Publisher is closed");
    }

    if (!record.topic().equals(topic)) {
      throw new IllegalArgumentException("The record's topic must be the same as the Producer's topic.");
    }

    Map<String, String> attributes = new HashMap<>();

    byte[] serializedKey = ByteString.EMPTY.toByteArray();
    if (record.key() != null) {
      serializedKey = this.keySerializer.serialize(topic, record.key());
      attributes
          .put("key", new String(serializedKey, StandardCharsets.ISO_8859_1));
    }

    byte[] valueBytes = ByteString.EMPTY.toByteArray();
    if (record.value() != null) {
      valueBytes = valueSerializer.serialize(topic, record.value());
    }

    checkRecordSize(Records.LOG_OVERHEAD + Record.recordSize(serializedKey, valueBytes));

    PubsubMessage message =
        PubsubMessage.newBuilder()
            .setData(ByteString.copyFrom(valueBytes))
            .putAllAttributes(attributes)
            .build();
    RpcFuture<String> messageIdFuture = publisher.publish(message);
    Future<RecordMetadata> future = SettableFuture.create();
    RecordMetadata metadata = new RecordMetadata(new TopicPartition(topic, 0),
        0, 0,  System.currentTimeMillis(), 0, serializedKey.length, valueBytes.length);

    if (callback != null) {
      if (isAcks) {
        messageIdFuture.addCallback(new RpcFutureCallback<String>() {
          @Override
          public void onFailure(Throwable t) {
            callback.onCompletion(metadata, new ExecutionException(t));
          }

          @Override
          public void onSuccess(String result) {
            callback.onCompletion(metadata, null);
          }
        });
      } else {
        callback.onCompletion(metadata, null);
      }
    }

    return future;
  }

  private void checkRecordSize(int size) {
    if (size > this.maxRequestSize) {
      throw new RecordTooLargeException(
          "Message is " + size + " bytes which is larger than max request size you have"
              + " configured");
    }
  }

  /**
   * Flushes records that have accumulated.
   */
  public void flush() {
    log.debug("Flushing...");
    // Shut down publisher to flush the logs, then immediately restart it.
    try {
      publisher.shutdown();
      publisher = newPublisher();
    } catch (Exception e) {
      log.error("Exception occurred during flush: " + e);
    }
  }

  /**
   * Not supported by this implementation.
   */
  public List<PartitionInfo> partitionsFor(String topic) {
    throw new NotImplementedException("Partitions not supported");
  }

  /**
   * Not supported by this implementation.
   */
  public Map<MetricName, ? extends Metric> metrics() {
    throw new NotImplementedException("Metrics not supported.");
  }

  /**
   * Closes the producer.
   */
  public void close() {
    close(0, null);
  }

  /**
   * Closes the producer with the given timeout.
   */
  public void close(long timeout, TimeUnit unit) {
    if (timeout < 0) {
      throw new IllegalArgumentException("Timeout cannot be negative.");
    }
    if (closed.getAndSet(true)) {
      throw new IllegalStateException("Cannot close a producer if already closed.");
    }
    try {
      publisher.shutdown();
    } catch (Exception e) {
      log.error("Exception occurred during close: " + e);
    }
    log.debug("Closed producer");
  }

  /**
   * PubsubProducer.Builder is used to create an instance of the publisher, with the specified
   * properties and configurations.
   */
  public static class Builder<K, V> {

    private final String project;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final String topic;

    private long batchSize;
    private long lingerMs;
    private int bufferMemory;
    private boolean isAcks;
    private int maxRequestSize;

    public Builder(String project, String topic, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
      Preconditions
          .checkArgument(project != null && keySerializer != null && valueSerializer != null);
      this.project = project;
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
      this.topic = topic;
      setDefaults();
    }

    private void setDefaults() {
      // this is where to set 'regular' fields w/o side effects
      this.batchSize = PubsubProducerConfig.DEFAULT_BATCH_SIZE;
      this.isAcks = PubsubProducerConfig.DEFAULT_ACKS;
      this.maxRequestSize = PubsubProducerConfig.DEFAULT_MAX_REQUEST_SIZE;
      this.lingerMs = PubsubProducerConfig.DEFAULT_LINGER_MS;
      this.bufferMemory = PubsubProducerConfig.DEFAULT_BUFFER_MEMORY;
    }

    public Builder batchSize(long val) {
      Preconditions.checkArgument(val > 0);
      batchSize = val;
      return this;
    }

    public Builder isAcks(boolean val) {
      isAcks = val;
      return this;
    }

    public Builder maxRequestSize(int val) {
      Preconditions.checkArgument(val >= 0);
      maxRequestSize = val;
      return this;
    }

    public Builder lingerMs(long val) {
      Preconditions.checkArgument(val >= 0);
      lingerMs = val;
      return this;
    }

    public Builder bufferMemory(int val) {
      Preconditions.checkArgument(val >= 0);
      bufferMemory = val;
      return this;
    }

    public PubsubProducer build() {
      return new PubsubProducer(this);
    }
  }

}
