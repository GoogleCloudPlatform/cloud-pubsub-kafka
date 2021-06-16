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

import com.google.pubsub.kafka.common.ConnectorUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SinkConnector} that writes messages to a specified topic in <a
 * href="https://cloud.google.com/pubsub">Google Cloud Pub/Sub</a>.
 */
public class CloudPubSubSinkConnector extends SinkConnector {

  private static final Logger log = LoggerFactory.getLogger(CloudPubSubSinkConnector.class);

  public static final String MAX_BUFFER_SIZE_CONFIG = "maxBufferSize";
  public static final String MAX_BUFFER_BYTES_CONFIG = "maxBufferBytes";
  public static final String MAX_OUTSTANDING_REQUEST_BYTES = "maxOutstandingRequestBytes";
  public static final String MAX_OUTSTANDING_ELEMENT_COUNT = "maxOutstandingElementCount";
  public static final String MAX_DELAY_THRESHOLD_MS = "delayThresholdMs";
  public static final String MAX_REQUEST_TIMEOUT_MS = "maxRequestTimeoutMs";
  public static final String MAX_TOTAL_TIMEOUT_MS = "maxTotalTimeoutMs";
  public static final String MAX_SHUTDOWN_TIMEOUT_MS = "maxShutdownTimeoutMs";
  public static final int DEFAULT_MAX_BUFFER_SIZE = 100;
  public static final long DEFAULT_MAX_BUFFER_BYTES = 9500000L;
  public static final int DEFAULT_DELAY_THRESHOLD_MS = 100;
  public static final int DEFAULT_REQUEST_TIMEOUT_MS = 10000;
  public static final int DEFAULT_TOTAL_TIMEOUT_MS = 60000;
  public static final int DEFAULT_SHUTDOWN_TIMEOUT_MS = 60000;
  public static final String CPS_MESSAGE_BODY_NAME = "messageBodyName";
  public static final String DEFAULT_MESSAGE_BODY_NAME = "cps_message_body";
  public static final String PUBLISH_KAFKA_METADATA = "metadata.publish";
  public static final String PUBLISH_KAFKA_HEADERS = "headers.publish";
  public static final String ORDERING_KEY_SOURCE = "orderingKeySource";
  public static final String DEFAULT_ORDERING_KEY_SOURCE = "none";

  /** Defines the accepted values for the {@link #ORDERING_KEY_SOURCE}. */
  public enum OrderingKeySource {
    NONE("none"),
    KEY("key"),
    PARTITION("partition");

    private String value;

    OrderingKeySource(String value) {
      this.value = value;
    }

    public String toString() {
      return value;
    }

    public static OrderingKeySource getEnum(String value) {
      if (value.equals("none")) {
        return OrderingKeySource.NONE;
      } else if (value.equals("key")) {
        return OrderingKeySource.KEY;
      } else if (value.equals("partition")) {
        return OrderingKeySource.PARTITION;
      } else {
        return null;
      }
    }

    /** Validator class for {@link CloudPubSubSinkConnector.OrderingKeySource}. */
    public static class Validator implements ConfigDef.Validator {

      @Override
      public void ensureValid(String name, Object o) {
        String value = (String) o;
        if (!value.equals(CloudPubSubSinkConnector.OrderingKeySource.NONE.toString())
            && !value.equals(CloudPubSubSinkConnector.OrderingKeySource.KEY.toString())
            && !value.equals(CloudPubSubSinkConnector.OrderingKeySource.PARTITION.toString())) {
          throw new ConfigException(
              "Valid values for "
                  + CloudPubSubSinkConnector.ORDERING_KEY_SOURCE
                  + " are "
                  + Arrays.toString(OrderingKeySource.values()));
        }
      }
    }
  }

  private Map<String, String> props;

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    this.props = props;
    log.info("Started the CloudPubSubSinkConnector.");
  }

  @Override
  public Class<? extends Task> taskClass() {
    return CloudPubSubSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    // Each task will get the exact same configuration. Delegate all config validation to the task.
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> config = new HashMap<>(props);
      configs.add(config);
    }
    return configs;
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef()
        .define(
            ConnectorUtils.CPS_PROJECT_CONFIG,
            Type.STRING,
            Importance.HIGH,
            "The project containing the topic to which to publish.")
        .define(
            ConnectorUtils.CPS_TOPIC_CONFIG,
            Type.STRING,
            Importance.HIGH,
            "The topic to which to publish.")
        .define(
            MAX_BUFFER_SIZE_CONFIG,
            Type.INT,
            DEFAULT_MAX_BUFFER_SIZE,
            ConfigDef.Range.between(1, Integer.MAX_VALUE),
            Importance.MEDIUM,
            "The maximum number of messages that can be received for the messages on a topic "
                + "partition before publishing them to Cloud Pub/Sub.")
        .define(
            MAX_BUFFER_BYTES_CONFIG,
            Type.LONG,
            DEFAULT_MAX_BUFFER_BYTES,
            ConfigDef.Range.between(1, DEFAULT_MAX_BUFFER_BYTES),
            Importance.MEDIUM,
            "The maximum number of bytes that can be received for the messages on a topic "
                + "partition before publishing the messages to Cloud Pub/Sub.")
        .define(MAX_OUTSTANDING_REQUEST_BYTES,
            Type.LONG,
            Long.MAX_VALUE,
            Importance.MEDIUM,
            "The maximum outstanding bytes from incomplete requests before the task blocks."
        )
        .define(MAX_OUTSTANDING_ELEMENT_COUNT,
            Type.LONG,
            Long.MAX_VALUE,
            Importance.MEDIUM,
            "The maximum outstanding incomplete messages before the task blocks."
        )
        .define(
            MAX_DELAY_THRESHOLD_MS,
            Type.INT,
            DEFAULT_DELAY_THRESHOLD_MS,
            ConfigDef.Range.between(1, Integer.MAX_VALUE),
            Importance.MEDIUM,
            "The maximum amount of time to wait after receiving the first message in a batch for a "
                + "before publishing the messages to Cloud Pub/Sub.")
        .define(
            MAX_REQUEST_TIMEOUT_MS,
            Type.INT,
            DEFAULT_REQUEST_TIMEOUT_MS,
            ConfigDef.Range.between(10000, Integer.MAX_VALUE),
            Importance.MEDIUM,
            "The maximum amount of time to wait for a single publish request to Cloud Pub/Sub.")
        .define(
            MAX_TOTAL_TIMEOUT_MS,
            Type.INT,
            DEFAULT_TOTAL_TIMEOUT_MS,
            ConfigDef.Range.between(10000, Integer.MAX_VALUE),
            Importance.MEDIUM,
            "The maximum amount of time to wait for a publish to complete (including retries) in "
                + "Cloud Pub/Sub.")
        .define(
            MAX_SHUTDOWN_TIMEOUT_MS,
            Type.INT,
            DEFAULT_SHUTDOWN_TIMEOUT_MS,
            ConfigDef.Range.between(10000, Integer.MAX_VALUE),
            Importance.MEDIUM,
            "The maximum amount of time to wait for a publisher to shutdown when stopping task "
                + "in Kafka Connect.")
        .define(
            PUBLISH_KAFKA_METADATA,
            Type.BOOLEAN,
            false,
            Importance.MEDIUM,
            "When true, include the Kafka topic, partition, offset, and timestamp as message "
                + "attributes when a message is published to Cloud Pub/Sub.")
        .define(
           PUBLISH_KAFKA_HEADERS,
           Type.BOOLEAN,
           false,
           Importance.MEDIUM,
           "When true, include any headers as attributes when a message is published to Cloud Pub/Sub.")
        .define(CPS_MESSAGE_BODY_NAME,
            Type.STRING,
            DEFAULT_MESSAGE_BODY_NAME,
            Importance.MEDIUM,
            "When using a struct or map value schema, this field or key name indicates that the "
                + "corresponding value will go into the Pub/Sub message body.")
        .define(ConnectorUtils.GCP_CREDENTIALS_FILE_PATH_CONFIG,
            Type.STRING,
            null,
            Importance.HIGH,
            "The path to the GCP credentials file")
        .define(ConnectorUtils.GCP_CREDENTIALS_JSON_CONFIG,
            Type.STRING,
            null,
            Importance.HIGH,
            "GCP JSON credentials")
        .define(ORDERING_KEY_SOURCE,
            Type.STRING,
            DEFAULT_ORDERING_KEY_SOURCE,
            new OrderingKeySource.Validator(),
            Importance.MEDIUM,
            "What to use to populate the Pub/Sub message ordering key. Possible values are "
                + "\"none\", \"key\", or \"partition\".")
        .define(ConnectorUtils.CPS_ENDPOINT,
            Type.STRING,
            ConnectorUtils.CPS_DEFAULT_ENDPOINT,
            Importance.LOW,
            "The Pub/Sub endpoint to use.");
  }

  @Override
  public void stop() {}
}
