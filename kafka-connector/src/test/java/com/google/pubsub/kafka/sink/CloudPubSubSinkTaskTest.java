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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.v1.PubsubMessage;
import java.lang.Runnable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/** Tests for {@link CloudPubSubSinkTask}. */
public class CloudPubSubSinkTaskTest {

  private static final String CPS_TOPIC = "the";
  private static final String CPS_PROJECT = "quick";
  private static final String CPS_MIN_BATCH_SIZE1 = "2";
  private static final String CPS_MIN_BATCH_SIZE2 = "9";
  private static final String KAFKA_TOPIC = "brown";
  private static final ByteString KAFKA_MESSAGE1 = ByteString.copyFromUtf8("fox");
  private static final ByteString KAFKA_MESSAGE2 = ByteString.copyFromUtf8("jumps");
  private static final String FIELD_STRING1 = "over";
  private static final String FIELD_STRING2 = "lazy";
  private static final String KAFKA_MESSAGE_KEY1 = "dog";
  private static final String KAFKA_MESSAGE_KEY2 = "cat";
  private static final Schema STRING_SCHEMA = SchemaBuilder.string().build();
  private static final Schema BYTE_STRING_SCHEMA =
      SchemaBuilder.bytes().name(ConnectorUtils.SCHEMA_NAME).build();

  private CloudPubSubSinkTask task;
  private Map<String, String> props;
  private Publisher publisher;

  private class SpyableFuture<V> implements ApiFuture<V> {
    private V value = null;
    private Throwable exception = null;

    public SpyableFuture(V value) {
      this.value = value;
    }

    public <V> SpyableFuture(Throwable exception) {
      this.exception = exception;
    }

    @Override
    public V get() throws ExecutionException {
      if (exception != null) {
        throw new ExecutionException(exception);
      }
      return value;
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws ExecutionException {
      return get();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
      executor.execute(listener);
    }
  }

  @Before
  public void setup() {
    publisher = mock(Publisher.class, RETURNS_DEEP_STUBS);
    task = new CloudPubSubSinkTask(publisher);
    props = new HashMap<>();
    props.put(ConnectorUtils.CPS_TOPIC_CONFIG, CPS_TOPIC);
    props.put(ConnectorUtils.CPS_PROJECT_CONFIG, CPS_PROJECT);
    props.put(CloudPubSubSinkConnector.MAX_BUFFER_SIZE_CONFIG, CPS_MIN_BATCH_SIZE2);
  }

  /** Tests that an exception is thrown when the schema of the value is not BYTES. */
  @Test
  public void testPutPrimitives() {
    task.start(props);
    SinkRecord record8 =
        new SinkRecord(null, -1, null, null, SchemaBuilder.int8(), (byte) 5, -1);
    SinkRecord record16 =
        new SinkRecord(null, -1, null, null, SchemaBuilder.int16(), (short) 5, -1);
    SinkRecord record32 =
        new SinkRecord(null, -1, null, null, SchemaBuilder.int32(), (int) 5, -1);
    SinkRecord record64 =
        new SinkRecord(null, -1, null, null, SchemaBuilder.int64(), (long) 5, -1);
    SinkRecord recordFloat32 =
        new SinkRecord(null, -1, null, null, SchemaBuilder.float32(), (float) 8, -1);
    SinkRecord recordFloat64 =
        new SinkRecord(null, -1, null, null, SchemaBuilder.float64(), (double) 8, -1);
    SinkRecord recordBool =
        new SinkRecord(null, -1, null, null, SchemaBuilder.bool(), true, -1);
    SinkRecord recordString =
        new SinkRecord(null, -1, null, null, SchemaBuilder.string(), "Test put.", -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record8);
    list.add(record16);
    list.add(record32);
    list.add(record64);
    list.add(recordFloat32);
    list.add(recordFloat64);
    list.add(recordBool);
    list.add(recordString);
    task.put(list);
  }

  @Test
  public void testStructSchema() {
    task.start(props);
    Schema schema = SchemaBuilder.struct().field(FIELD_STRING1, SchemaBuilder.string())
        .field(FIELD_STRING2, SchemaBuilder.string()).build();
    Struct val = new Struct(schema);
    val.put(FIELD_STRING1, "tide");
    val.put(FIELD_STRING2, "eagle");
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);
  }

  @Test
  public void testStructSchemaWithOptionalField() {
    task.start(props);

    Schema schema = SchemaBuilder.struct().field(FIELD_STRING1, SchemaBuilder.string())
        .field(FIELD_STRING2, SchemaBuilder.string().optional()).build();

    // With the optional field missing.
    Struct val = new Struct(schema);
    val.put(FIELD_STRING1, "tide");
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);

    // With the optional field present.
    val.put(FIELD_STRING2, "eagle");
    record = new SinkRecord(null, -1, null, null, schema, val, -1);
    list = new ArrayList<>();
    list.add(record);
    task.put(list);
  }

  @Test(expected = DataException.class)
  public void testStructSchemaWithMissingField() {
    task.start(props);

    Schema schema = SchemaBuilder.struct().field(FIELD_STRING1, SchemaBuilder.string())
        .field(FIELD_STRING2, SchemaBuilder.string()).build();
    Struct val = new Struct(schema);
    val.put(FIELD_STRING1, "tide");
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);
  }

  @Test(expected = DataException.class)
  public void testStructSchemaWithNestedSchema() {
    task.start(props);

    Schema nestedSchema = SchemaBuilder.struct().build();
    Struct nestedVal = new Struct(nestedSchema);

    Schema schema = SchemaBuilder.struct().field(FIELD_STRING1, SchemaBuilder.string())
        .field(FIELD_STRING2, nestedSchema).build();
    Struct val = new Struct(schema);
    val.put(FIELD_STRING1, "tide");
    val.put(FIELD_STRING2, nestedVal);
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);
  }

  @Test
  public void testMapSchema() {
    task.start(props);
    Schema schema = SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.string()).build();
    Map<String, String> val = new HashMap<>();
    val.put(FIELD_STRING1, "tide");
    val.put(FIELD_STRING2, "eagle");
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);
    schema = SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.bytes()).build();
    record = new SinkRecord(null, -1, null, null, schema, val, -1);
    list.add(record);
    try {
      task.put(list);
    } catch (DataException e) { } // Expected, pass.
  }

  @Test
  public void testArraySchema() {
    task.start(props);
    Schema schema = SchemaBuilder.array(SchemaBuilder.string()).build();
    String[] val = {"Roll", "tide"};
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);
    schema = SchemaBuilder.array(SchemaBuilder.struct()).build();
    record = new SinkRecord(null, -1, null, null, schema, null, -1);
    list.add(record);
    try {
      task.put(list);
    } catch (DataException e) { } // Expected, pass.
  }

  @Test
  public void testNullSchema() {
    task.start(props);
    String val = "I have no schema";
    SinkRecord record = new SinkRecord(null, -1, null, null, null, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);
  }

  /**
   * Tests that the correct message is sent to the publisher.
   */
  @Test
  public void testPutWherePublishesAreInvoked() {
    props.put(CloudPubSubSinkConnector.MAX_BUFFER_SIZE_CONFIG, CPS_MIN_BATCH_SIZE1);
    task.start(props);
    List<SinkRecord> records = getSampleRecords();
    task.put(records);
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher, times(2)).publish(captor.capture());
    List<PubsubMessage> requestArgs = captor.getAllValues();
    assertEquals(requestArgs, getPubsubMessagesFromSampleRecords());
  }

  /**
   * Tests that the correct message is sent to the publisher when the record has a null value.
   */
  @Test
  public void testPutWithNullValues() {
    props.put(CloudPubSubSinkConnector.MAX_BUFFER_SIZE_CONFIG, CPS_MIN_BATCH_SIZE1);
    task.start(props);
    List<SinkRecord> records = new ArrayList<>();
    records.add(
        new SinkRecord(
            KAFKA_TOPIC,
            0,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            STRING_SCHEMA,
            null,
            -1));
    task.put(records);
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher, times(1)).publish(captor.capture());
    List<PubsubMessage> requestArgs = captor.getAllValues();
    List<PubsubMessage> expectedMessages = new ArrayList<>();
    Map<String, String> attributes = new HashMap<>();
    attributes.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    expectedMessages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes).build());
    assertEquals(requestArgs, expectedMessages);
  }

  /**
   * Tests that the no message is sent when a message is completely null.
   */
  @Test
  public void testPutWithNullMessage() {
    System.out.println("NULL MSG");
    props.put(CloudPubSubSinkConnector.MAX_BUFFER_SIZE_CONFIG, CPS_MIN_BATCH_SIZE1);
    task.start(props);
    List<SinkRecord> records = new ArrayList<>();
    records.add(
        new SinkRecord(
            KAFKA_TOPIC,
            0,
            STRING_SCHEMA,
            null,
            STRING_SCHEMA,
            null,
            -1));
    task.put(records);
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher, times(0)).publish(captor.capture());
  }

  /**
   * Tests that a call to flush() processes the Futures that were generated by calls to put.
   */
  @Test
  public void testFlushWithNoPublishInPut() throws Exception {
    task.start(props);
    Map<TopicPartition, OffsetAndMetadata> partitionOffsets = new HashMap<>();
    partitionOffsets.put(new TopicPartition(KAFKA_TOPIC, 0), null);
    List<SinkRecord> records = getSampleRecords();
    ApiFuture<String> goodFuture = getSuccessfulPublishFuture();
    when(publisher.publish(any(PubsubMessage.class))).thenReturn(goodFuture);
    task.put(records);
    task.flush(partitionOffsets);
    verify(publisher, times(1)).publishAllOutstanding();
    verify(publisher, times(2)).publish(any(PubsubMessage.class));
    verify(goodFuture, times(2)).addListener(any(Runnable.class), any(Executor.class));
  }

  /**
   * Tests that if a Future that is being processed in flush() failed with an exception, that an
   * exception is thrown.
   */
  @Test(expected = RuntimeException.class)
  public void testFlushExceptionCase() throws Exception {
    task.start(props);
    Map<TopicPartition, OffsetAndMetadata> partitionOffsets = new HashMap<>();
    partitionOffsets.put(new TopicPartition(KAFKA_TOPIC, 0), null);
    List<SinkRecord> records = getSampleRecords();
    ApiFuture<String> badFuture = getFailedPublishFuture();
    when(publisher.publish(any(PubsubMessage.class))).thenReturn(badFuture);
    task.put(records);
    task.flush(partitionOffsets);
    verify(publisher, times(1)).publishAllOutstanding();
    verify(publisher, times(1)).publish(any(PubsubMessage.class));
    verify(badFuture, times(1)).addListener(any(Runnable.class), any(Executor.class));
  }

  /**
   * Tests that when requested, Kafka metadata is included in the messages published to Cloud
   * Pub/Sub.
   */
  @Test
  public void testKafkaMetadata() {
    props.put(CloudPubSubSinkConnector.PUBLISH_KAFKA_METADATA, "true");
    props.put(CloudPubSubSinkConnector.MAX_BUFFER_SIZE_CONFIG, CPS_MIN_BATCH_SIZE1);
    task.start(props);
    List<SinkRecord> records = new ArrayList<SinkRecord>();
    records.add(
        new SinkRecord(
            KAFKA_TOPIC,
            4,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE1,
            1000,
            50000L,
            TimestampType.CREATE_TIME));
    records.add(
        new SinkRecord(
            KAFKA_TOPIC,
            4,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE2,
            1001,
            50001L,
            TimestampType.CREATE_TIME));
    records.add(
        new SinkRecord(
            KAFKA_TOPIC,
            4,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE2,
            1002,
            null,
            TimestampType.CREATE_TIME));
    task.put(records);
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher, times(3)).publish(captor.capture());
    List<PubsubMessage> requestArgs = captor.getAllValues();


    List<PubsubMessage> expectedMessages = new ArrayList<>();
    Map<String, String> attributes1 = new HashMap<>();
    attributes1.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    attributes1.put(ConnectorUtils.KAFKA_TOPIC_ATTRIBUTE, KAFKA_TOPIC);
    attributes1.put(ConnectorUtils.KAFKA_PARTITION_ATTRIBUTE, "4");
    attributes1.put(ConnectorUtils.KAFKA_OFFSET_ATTRIBUTE, "1000");
    attributes1.put(ConnectorUtils.KAFKA_TIMESTAMP_ATTRIBUTE, "50000");
    expectedMessages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes1).setData(KAFKA_MESSAGE1).build());
    Map<String, String> attributes2 = new HashMap<>();
    attributes2.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    attributes2.put(ConnectorUtils.KAFKA_TOPIC_ATTRIBUTE, KAFKA_TOPIC);
    attributes2.put(ConnectorUtils.KAFKA_PARTITION_ATTRIBUTE, "4");
    attributes2.put(ConnectorUtils.KAFKA_OFFSET_ATTRIBUTE, "1001");
    attributes2.put(ConnectorUtils.KAFKA_TIMESTAMP_ATTRIBUTE, "50001");
    expectedMessages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes2).setData(KAFKA_MESSAGE2).build());
    Map<String, String> attributes3 = new HashMap<>();
    attributes3.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    attributes3.put(ConnectorUtils.KAFKA_TOPIC_ATTRIBUTE, KAFKA_TOPIC);
    attributes3.put(ConnectorUtils.KAFKA_PARTITION_ATTRIBUTE, "4");
    attributes3.put(ConnectorUtils.KAFKA_OFFSET_ATTRIBUTE, "1002");
    expectedMessages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes3).setData(KAFKA_MESSAGE2).build());

    assertEquals(requestArgs, expectedMessages);
  }

  /**
   * Tests that when requested, Kafka headers are included in the messages published to Cloud
   * Pub/Sub.
   */
  @Test
  public void testKafkaHeaders() {
    props.put(CloudPubSubSinkConnector.PUBLISH_KAFKA_HEADERS, "true");
    task.start(props);
    List<SinkRecord> records = new ArrayList<SinkRecord>();
    SinkRecord record = new SinkRecord(
            KAFKA_TOPIC,
            4,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE1,
            1000,
            50000L,
        TimestampType.CREATE_TIME);
    record.headers().addString("myHeader", "myValue");
    records.add(record);
    record = new SinkRecord(
            KAFKA_TOPIC,
            4,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE2,
            1001,
            50001L,
        TimestampType.CREATE_TIME);
    record.headers().addString("yourHeader", "yourValue");
    records.add(record);
    task.put(records);
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher, times(2)).publish(captor.capture());
    List<PubsubMessage> requestArgs = captor.getAllValues();


    List<PubsubMessage> expectedMessages = new ArrayList<>();
    Map<String, String> attributes1 = new HashMap<>();
    attributes1.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    attributes1.put("myHeader", "myValue");
    expectedMessages.add(
            PubsubMessage.newBuilder().putAllAttributes(attributes1).setData(KAFKA_MESSAGE1).build());
    Map<String, String> attributes2 = new HashMap<>();
    attributes2.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    attributes2.put("yourHeader", "yourValue");
    expectedMessages.add(
            PubsubMessage.newBuilder().putAllAttributes(attributes2).setData(KAFKA_MESSAGE2).build());

    assertEquals(expectedMessages, requestArgs);
  }

  /**
   * Tests that when requested, Kafka headers are included in the messages published to Cloud
   * Pub/Sub but if some of these headers are unsupported either if its key has more than 256 bytes long
   * or its value has more than 1024 bytes it will be discarded.
   */
  @Test
  public void testUnsupportedKafkaHeaders() {
    props.put(CloudPubSubSinkConnector.PUBLISH_KAFKA_HEADERS, "true");
    task.start(props);
    String veryLongHeaderName;
    String veryLongValue;
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < 257; i++) {
      stringBuilder.append("-");
    }
    veryLongHeaderName = stringBuilder.toString();
    stringBuilder.setLength(0);
    for (int i = 0; i < 1025; i++) {
      stringBuilder.append(".");
    }
    veryLongValue = stringBuilder.toString();
    stringBuilder.setLength(0);
    List<SinkRecord> records = new ArrayList<SinkRecord>();
    SinkRecord record = new SinkRecord(
        KAFKA_TOPIC,
        4,
        STRING_SCHEMA,
        KAFKA_MESSAGE_KEY1,
        BYTE_STRING_SCHEMA,
        KAFKA_MESSAGE1,
        1000,
        50000L,
        TimestampType.CREATE_TIME);
    record.headers().addString("myHeader", "myValue");
    record.headers().addString(veryLongHeaderName, "anotherValue");
    record.headers().addString("anotherHeader", veryLongValue);
    record.headers().addString(veryLongHeaderName, veryLongValue);
    records.add(record);
    record = new SinkRecord(
        KAFKA_TOPIC,
        4,
        STRING_SCHEMA,
        KAFKA_MESSAGE_KEY1,
        BYTE_STRING_SCHEMA,
        KAFKA_MESSAGE2,
        1001,
        50001L,
        TimestampType.CREATE_TIME);
    record.headers().addString("yourHeader", "yourValue");
    records.add(record);
    task.put(records);
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher, times(2)).publish(captor.capture());
    List<PubsubMessage> requestArgs = captor.getAllValues();


    List<PubsubMessage> expectedMessages = new ArrayList<>();
    Map<String, String> attributes1 = new HashMap<>();
    attributes1.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    attributes1.put("myHeader", "myValue");
    expectedMessages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes1).setData(KAFKA_MESSAGE1).build());
    Map<String, String> attributes2 = new HashMap<>();
    attributes2.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    attributes2.put("yourHeader", "yourValue");
    expectedMessages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes2).setData(KAFKA_MESSAGE2).build());

    assertEquals(257, veryLongHeaderName.getBytes().length);
    assertEquals(1025, veryLongValue.getBytes().length);
    assertEquals(expectedMessages, requestArgs);
  }

  /**
   * Tests that if a Future that is being processed in flush() failed with an exception and then a
   * second Future is processed successfully in a subsequent flush, then the subsequent flush
   * succeeds.
   */
  @Test
  public void testFlushExceptionThenNoExceptionCase() throws Exception {
    task.start(props);
    Map<TopicPartition, OffsetAndMetadata> partitionOffsets = new HashMap<>();
    partitionOffsets.put(new TopicPartition(KAFKA_TOPIC, 0), null);
    List<SinkRecord> records = getSampleRecords();
    ApiFuture<String> badFuture = getFailedPublishFuture();
    ApiFuture<String> goodFuture = getSuccessfulPublishFuture();
    when(publisher.publish(any(PubsubMessage.class))).thenReturn(badFuture).thenReturn(badFuture).thenReturn(goodFuture);
    task.put(records);
    try {
      task.flush(partitionOffsets);
    } catch (RuntimeException e) {
    }
    records = getSampleRecords();
    task.put(records);
    task.flush(partitionOffsets);
    verify(publisher, times(2)).publishAllOutstanding();
    verify(publisher, times(4)).publish(any(PubsubMessage.class));
    verify(badFuture, times(2)).addListener(any(Runnable.class), any(Executor.class));
    verify(goodFuture, times(2)).addListener(any(Runnable.class), any(Executor.class));
  }

  @Test
  public void testPublisherShutdownOnStop() throws Exception {
    int maxShutdownTimeoutMs = 20000;
    props.put(CloudPubSubSinkConnector.MAX_SHUTDOWN_TIMEOUT_MS, Integer.toString(maxShutdownTimeoutMs));

    task.start(props);
    task.stop();

    verify(publisher, times(1)).shutdown();
    verify(publisher, times(1)).awaitTermination(maxShutdownTimeoutMs, TimeUnit.MILLISECONDS);
  }

  /** Get some sample SinkRecords's to use in the tests. */
  private List<SinkRecord> getSampleRecords() {
    List<SinkRecord> records = new ArrayList<>();
    records.add(
        new SinkRecord(
            KAFKA_TOPIC,
            0,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE1,
            -1));
    records.add(
        new SinkRecord(
            KAFKA_TOPIC,
            0,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE2,
            -1));
    return records;
  }

  /**
   * Tests that when requested, Kafka key is used as the ordering key in the messages published to
   * Cloud Pub/Sub.
   */
  @Test
  public void testOrderingKeysKey() {
    props.put(CloudPubSubSinkConnector.ORDERING_KEY_SOURCE, "key");
    task.start(props);
    List<SinkRecord> records = new ArrayList<SinkRecord>();
    SinkRecord record = new SinkRecord(
            KAFKA_TOPIC,
            4,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE1,
            1000,
            50000L,
        TimestampType.CREATE_TIME);
    records.add(record);
    record = new SinkRecord(
            KAFKA_TOPIC,
            4,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY2,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE2,
            1001,
            50001L,
            TimestampType.CREATE_TIME);
    records.add(record);
    task.put(records);
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher, times(2)).publish(captor.capture());
    List<PubsubMessage> requestArgs = captor.getAllValues();

    List<PubsubMessage> expectedMessages = new ArrayList<>();
    Map<String, String> attributes1 = new HashMap<>();
    attributes1.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    expectedMessages.add(
        PubsubMessage.newBuilder()
            .putAllAttributes(attributes1)
            .setOrderingKey(KAFKA_MESSAGE_KEY1)
            .setData(KAFKA_MESSAGE1)
            .build());
    Map<String, String> attributes2 = new HashMap<>();
    attributes2.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY2);
    expectedMessages.add(
        PubsubMessage.newBuilder()
            .putAllAttributes(attributes2)
            .setOrderingKey(KAFKA_MESSAGE_KEY2)
            .setData(KAFKA_MESSAGE2)
            .build());

    assertEquals(expectedMessages, requestArgs);
  }

  /**
   * Tests that when requested, Kafka key is used as the ordering key in the messages published to
   * Cloud Pub/Sub.
   */
  @Test
  public void testOrderingKeysPartition() {
    props.put(CloudPubSubSinkConnector.ORDERING_KEY_SOURCE, "partition");
    task.start(props);
    List<SinkRecord> records = new ArrayList<SinkRecord>();
    SinkRecord record = new SinkRecord(
            KAFKA_TOPIC,
            4,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE1,
            1000,
            50000L,
        TimestampType.CREATE_TIME);
    records.add(record);
    record = new SinkRecord(
            KAFKA_TOPIC,
            5,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY2,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE2,
            1001,
            50001L,
            TimestampType.CREATE_TIME);
    records.add(record);
    task.put(records);
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher, times(2)).publish(captor.capture());
    List<PubsubMessage> requestArgs = captor.getAllValues();

    List<PubsubMessage> expectedMessages = new ArrayList<>();
    Map<String, String> attributes1 = new HashMap<>();
    attributes1.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    expectedMessages.add(
        PubsubMessage.newBuilder()
            .putAllAttributes(attributes1)
            .setOrderingKey("4")
            .setData(KAFKA_MESSAGE1)
            .build());
    Map<String, String> attributes2 = new HashMap<>();
    attributes2.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY2);
    expectedMessages.add(
        PubsubMessage.newBuilder()
            .putAllAttributes(attributes2)
            .setOrderingKey("5")
            .setData(KAFKA_MESSAGE2)
            .build());

    assertEquals(expectedMessages, requestArgs);
  }

  /**
   * Get some PubsubMessage's which correspond to the SinkRecord's created in {@link
   * #getSampleRecords()}.
   */
  private List<PubsubMessage> getPubsubMessagesFromSampleRecords() {
    List<PubsubMessage> messages = new ArrayList<>();
    Map<String, String> attributes = new HashMap<>();
    attributes.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY1);
    messages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes).setData(KAFKA_MESSAGE1).build());
    messages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes).setData(KAFKA_MESSAGE2).build());
    return messages;
  }

  private ApiFuture<String> getSuccessfulPublishFuture() {
    SpyableFuture<String> future = new SpyableFuture("abcd");
    return spy(future);
  }

  private ApiFuture<String> getFailedPublishFuture() {
    SpyableFuture<String> future = new SpyableFuture(new Exception());
    return spy(future);
  }
}
