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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for {@link CloudPubSubSourceTask}. */
public class CloudPubSubSourceTaskTest {
  private static final Logger log = LoggerFactory.getLogger(CloudPubSubSourceTaskTest.class);

  private static final String CPS_PROJECT = "the";
  private static final String CPS_MAX_BATCH_SIZE = "1000";
  private static final String CPS_SUBSCRIPTION = "quick";
  private static final String KAFKA_TOPIC = "brown";
  private static final String KAFKA_MESSAGE_KEY_ATTRIBUTE = "fox";
  private static final String KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE = "jumped";
  private static final String KAFKA_PARTITIONS = "3";
  private static final ByteString CPS_MESSAGE = ByteString.copyFromUtf8("over");
  private static final ByteBuffer KAFKA_VALUE = CPS_MESSAGE.asReadOnlyByteBuffer();
  private static final String ACK_ID1 = "ackID1";
  private static final String ACK_ID2 = "ackID2";
  private static final String ACK_ID3 = "ackID3";
  private static final String ACK_ID4 = "ackID4";

  private CloudPubSubSourceTask task;
  private Map<String, String> props;
  private CloudPubSubSubscriber subscriber;

  @Before
  public void setup() {
    subscriber = mock(CloudPubSubSubscriber.class, RETURNS_DEEP_STUBS);
    task = new CloudPubSubSourceTask(subscriber);
    props = new HashMap<>();
    props.put(ConnectorUtils.CPS_PROJECT_CONFIG, CPS_PROJECT);
    props.put(CloudPubSubSourceConnector.CPS_MAX_BATCH_SIZE_CONFIG, CPS_MAX_BATCH_SIZE);
    props.put(CloudPubSubSourceConnector.CPS_SUBSCRIPTION_CONFIG, CPS_SUBSCRIPTION);
    props.put(CloudPubSubSourceConnector.KAFKA_TOPIC_CONFIG, KAFKA_TOPIC);
    props.put(CloudPubSubSourceConnector.KAFKA_MESSAGE_KEY_CONFIG, KAFKA_MESSAGE_KEY_ATTRIBUTE);
    props.put(CloudPubSubSourceConnector.KAFKA_PARTITIONS_CONFIG, KAFKA_PARTITIONS);
    props.put(
        CloudPubSubSourceConnector.KAFKA_PARTITION_SCHEME_CONFIG,
        CloudPubSubSourceConnector.PartitionScheme.ROUND_ROBIN.toString());
  }

  /** Tests when no messages are received from the Cloud Pub/Sub PullResponse. */
  @Test
  public void testPollCaseWithNoMessages() throws Exception {
    task.start(props);
    PullResponse stubbedPullResponse = PullResponse.newBuilder().build();
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    assertEquals(0, task.poll().size());
    verify(subscriber, never()).ackMessages(any(AcknowledgeRequest.class));
  }

  /**
   * Tests that when ackMessages() succeeds and the subsequent call to poll() has no messages, that
   * the subscriber does not invoke ackMessages because there should be no acks.
   */
  @Test
  public void testPollInRegularCase() throws Exception {
    task.start(props);
    ReceivedMessage rm1 = createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<>());
    PullResponse stubbedPullResponse = PullResponse.newBuilder().addReceivedMessages(rm1).build();
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    List<SourceRecord> result = task.poll();
    assertEquals(1, result.size());
    stubbedPullResponse = PullResponse.newBuilder().build();
    ListenableFuture<Empty> goodFuture = Futures.immediateFuture(Empty.getDefaultInstance());
    when(subscriber.ackMessages(any(AcknowledgeRequest.class))).thenReturn(goodFuture);
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    result = task.poll();
    assertEquals(0, result.size());
    result = task.poll();
    assertEquals(0, result.size());
    verify(subscriber, times(1)).ackMessages(any(AcknowledgeRequest.class));
  }


  /**
   * Tests that when a call to ackMessages() fails, that the message is not sent again to Kafka if
   * the message is received again by Cloud Pub/Sub. Also tests that ack ids are added properly if
   * the ack id has not been seen before.
   */
  @Test
  public void testPollWithDuplicateReceivedMessages() throws Exception {
    task.start(props);
    ReceivedMessage rm1 = createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<>());
    PullResponse stubbedPullResponse = PullResponse.newBuilder().addReceivedMessages(rm1).build();
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    List<SourceRecord> result = task.poll();
    assertEquals(1, result.size());
    ReceivedMessage rm2 = createReceivedMessage(ACK_ID2, CPS_MESSAGE, new HashMap<>());
    stubbedPullResponse =
        PullResponse.newBuilder().addReceivedMessages(0, rm1).addReceivedMessages(1, rm2).build();
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    ListenableFuture<Empty> failedFuture = Futures.immediateFailedFuture(new Throwable());
    when(subscriber.ackMessages(any(AcknowledgeRequest.class))).thenReturn(failedFuture);
    result = task.poll();
    assertEquals(1, result.size());
    verify(subscriber, times(1)).ackMessages(any(AcknowledgeRequest.class));

  }

  /**
   * Tests when the message(s) retrieved from Cloud Pub/Sub do not have an attribute that matches
   * {@link #KAFKA_MESSAGE_KEY_ATTRIBUTE}.
   */
  @Test
  public void testPollWithNoMessageKeyAttribute() throws Exception {
    task.start(props);
    ReceivedMessage rm = createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<>());
    PullResponse stubbedPullResponse = PullResponse.newBuilder().addReceivedMessages(rm).build();
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any(AcknowledgeRequest.class));
    assertEquals(1, result.size());
    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    assertEquals(expected, result.get(0));
  }

  /**
   * Tests when the message(s) retrieved from Cloud Pub/Sub do have an attribute that matches {@link
   * #KAFKA_MESSAGE_KEY_ATTRIBUTE}.
   */
  @Test
  public void testPollWithMessageKeyAttribute() throws Exception {
    task.start(props);
    Map<String, String> attributes = new HashMap<>();
    attributes.put(KAFKA_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE);
    ReceivedMessage rm = createReceivedMessage(ACK_ID1, CPS_MESSAGE, attributes);
    PullResponse stubbedPullResponse = PullResponse.newBuilder().addReceivedMessages(rm).build();
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any(AcknowledgeRequest.class));
    assertEquals(1, result.size());
    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.STRING_SCHEMA,
            KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    assertEquals(expected, result.get(0));
  }

  /**
   * Tests when the message retrieved from Cloud Pub/Sub have several attributes, including
   * one that matches {@link #KAFKA_MESSAGE_KEY_ATTRIBUTE}.
   */
  @Test
  public void testPollWithMultipleAttributes() throws Exception {
    task.start(props);
    Map<String, String> attributes = new HashMap<>();
    attributes.put(KAFKA_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE);
    attributes.put("attribute1", "attribute_value1");
    attributes.put("attribute2", "attribute_value2");
    ReceivedMessage rm = createReceivedMessage(ACK_ID1, CPS_MESSAGE, attributes);
    PullResponse stubbedPullResponse = PullResponse.newBuilder().addReceivedMessages(rm).build();
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any(AcknowledgeRequest.class));
    assertEquals(1, result.size());
    Schema expectedSchema =
        SchemaBuilder.struct()
            .field(ConnectorUtils.CPS_MESSAGE_KAFKA_STRUCT_ATTRIBUTE, Schema.BYTES_SCHEMA)
            .field("attribute1", Schema.STRING_SCHEMA)
            .field("attribute2", Schema.STRING_SCHEMA)
            .build();
    Struct expectedValue = new Struct(expectedSchema)
                               .put(ConnectorUtils.CPS_MESSAGE_KAFKA_STRUCT_ATTRIBUTE, KAFKA_VALUE)
                               .put("attribute1", "attribute_value1")
                               .put("attribute2", "attribute_value2");
    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.STRING_SCHEMA,
            KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE,
            expectedSchema,
            expectedValue);
    assertEquals(expected, result.get(0));
  }

  /**
   * Tests that the correct partition is assigned when the partition scheme is "hash_key". The test
   * has two cases, one where a key does exist and one where it does not.
   */
  @Test
  public void testPollWithPartitionSchemeHashKey() throws Exception {
    props.put(
        CloudPubSubSourceConnector.KAFKA_PARTITION_SCHEME_CONFIG,
        CloudPubSubSourceConnector.PartitionScheme.HASH_KEY.toString());
    task.start(props);
    Map<String, String> attributes = new HashMap<>();
    attributes.put(KAFKA_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE);
    ReceivedMessage withoutKey = createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<>());
    ReceivedMessage withKey = createReceivedMessage(ACK_ID2, CPS_MESSAGE, attributes);
    PullResponse stubbedPullResponse =
        PullResponse.newBuilder()
            .addReceivedMessages(0, withKey)
            .addReceivedMessages(1, withoutKey)
            .build();
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any(AcknowledgeRequest.class));
    assertEquals(2, result.size());
    SourceRecord expectedForMessageWithKey =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE.hashCode() % Integer.parseInt(KAFKA_PARTITIONS),
            Schema.STRING_SCHEMA,
            KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    SourceRecord expectedForMessageWithoutKey =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);

    assertEquals(expectedForMessageWithKey, result.get(0));
    assertEquals(expectedForMessageWithoutKey.value(), result.get(1).value());
  }

  /** Tests that the correct partition is assigned when the partition scheme is "hash_value". */
  @Test
  public void testPollWithPartitionSchemeHashValue() throws Exception {
    props.put(
        CloudPubSubSourceConnector.KAFKA_PARTITION_SCHEME_CONFIG,
        CloudPubSubSourceConnector.PartitionScheme.HASH_VALUE.toString());
    task.start(props);
    ReceivedMessage rm = createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<>());
    PullResponse stubbedPullResponse = PullResponse.newBuilder().addReceivedMessages(rm).build();
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any(AcknowledgeRequest.class));
    assertEquals(1, result.size());
    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            KAFKA_VALUE.hashCode() % Integer.parseInt(KAFKA_PARTITIONS),
            Schema.STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    assertEquals(expected, result.get(0));
  }

  /**
   * Tests that the correct partition is assigned when the partition scheme is "round_robin". The
   * tests makes sure to submit an approrpriate number of messages to poll() so that all partitions
   * in the round robin are hit once.
   */
  @Test
  public void testPollWithPartitionSchemeRoundRobin() throws Exception {
    task.start(props);
    ReceivedMessage rm1 = createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<>());
    ReceivedMessage rm2 = createReceivedMessage(ACK_ID2, CPS_MESSAGE, new HashMap<>());
    ReceivedMessage rm3 = createReceivedMessage(ACK_ID3, CPS_MESSAGE, new HashMap<>());
    ReceivedMessage rm4 = createReceivedMessage(ACK_ID4, CPS_MESSAGE, new HashMap<>());
    PullResponse stubbedPullResponse =
        PullResponse.newBuilder()
            .addReceivedMessages(0, rm1)
            .addReceivedMessages(1, rm2)
            .addReceivedMessages(2, rm3)
            .addReceivedMessages(3, rm4)
            .build();
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any(AcknowledgeRequest.class));
    assertEquals(4, result.size());
    SourceRecord expected1 =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    SourceRecord expected2 =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            1,
            Schema.STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    SourceRecord expected3 =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            2,
            Schema.STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    SourceRecord expected4 =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    assertEquals(expected1, result.get(0));
    assertEquals(expected2, result.get(1));
    assertEquals(expected3, result.get(2));
    assertEquals(expected4, result.get(3));
  }

  @Test(expected = RuntimeException.class)
  public void testPollExceptionCase() throws Exception {
    task.start(props);
    // Could also throw ExecutionException if we wanted to...
    when(subscriber.pull(any(PullRequest.class)).get()).thenThrow(new InterruptedException());
    task.poll();
  }

  private ReceivedMessage createReceivedMessage(
      String ackId, ByteString data, Map<String, String> attributes) {
    PubsubMessage message =
        PubsubMessage.newBuilder().setData(data).putAllAttributes(attributes).build();
    return ReceivedMessage.newBuilder().setAckId(ackId).setMessage(message).build();
  }
}
