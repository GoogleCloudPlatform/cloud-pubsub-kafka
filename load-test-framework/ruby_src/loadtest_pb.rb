# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: src/main/proto/loadtest.proto

require 'google/protobuf'

require 'google/protobuf/duration_pb'
require 'google/protobuf/timestamp_pb'
Google::Protobuf::DescriptorPool.generated_pool.build do
  add_message "google.pubsub.loadtest.StartRequest" do
    optional :project, :string, 1
    optional :topic, :string, 2
    optional :request_rate, :int32, 3
    optional :message_size, :int32, 4
    optional :max_outstanding_requests, :int32, 5
    optional :start_time, :message, 6, "google.protobuf.Timestamp"
    optional :burn_in_duration, :message, 12, "google.protobuf.Duration"
    optional :publish_batch_size, :int32, 11
    optional :publish_batch_duration, :message, 13, "google.protobuf.Duration"
    oneof :stop_conditions do
      optional :test_duration, :message, 7, "google.protobuf.Duration"
      optional :number_of_messages, :int32, 8
    end
    oneof :options do
      optional :pubsub_options, :message, 9, "google.pubsub.loadtest.PubsubOptions"
      optional :kafka_options, :message, 10, "google.pubsub.loadtest.KafkaOptions"
    end
  end
  add_message "google.pubsub.loadtest.StartResponse" do
  end
  add_message "google.pubsub.loadtest.PubsubOptions" do
    optional :subscription, :string, 1
    optional :max_messages_per_pull, :int32, 2
  end
  add_message "google.pubsub.loadtest.KafkaOptions" do
    optional :broker, :string, 1
    optional :poll_duration, :message, 2, "google.protobuf.Duration"
    optional :zookeeper_ip_address, :string, 3
    optional :replication_factor, :int32, 4
    optional :partitions, :int32, 5
  end
  add_message "google.pubsub.loadtest.MessageIdentifier" do
    optional :publisher_client_id, :int64, 1
    optional :sequence_number, :int32, 2
  end
  add_message "google.pubsub.loadtest.CheckRequest" do
    repeated :duplicates, :message, 1, "google.pubsub.loadtest.MessageIdentifier"
  end
  add_message "google.pubsub.loadtest.CheckResponse" do
    repeated :bucket_values, :int64, 1
    optional :running_duration, :message, 2, "google.protobuf.Duration"
    optional :is_finished, :bool, 3
    repeated :received_messages, :message, 4, "google.pubsub.loadtest.MessageIdentifier"
  end
  add_message "google.pubsub.loadtest.ExecuteRequest" do
  end
  add_message "google.pubsub.loadtest.ExecuteResponse" do
    repeated :latencies, :int64, 1
    repeated :received_messages, :message, 2, "google.pubsub.loadtest.MessageIdentifier"
  end
end

module Google
  module Pubsub
    module Loadtest
      StartRequest = Google::Protobuf::DescriptorPool.generated_pool.lookup("google.pubsub.loadtest.StartRequest").msgclass
      StartResponse = Google::Protobuf::DescriptorPool.generated_pool.lookup("google.pubsub.loadtest.StartResponse").msgclass
      PubsubOptions = Google::Protobuf::DescriptorPool.generated_pool.lookup("google.pubsub.loadtest.PubsubOptions").msgclass
      KafkaOptions = Google::Protobuf::DescriptorPool.generated_pool.lookup("google.pubsub.loadtest.KafkaOptions").msgclass
      MessageIdentifier = Google::Protobuf::DescriptorPool.generated_pool.lookup("google.pubsub.loadtest.MessageIdentifier").msgclass
      CheckRequest = Google::Protobuf::DescriptorPool.generated_pool.lookup("google.pubsub.loadtest.CheckRequest").msgclass
      CheckResponse = Google::Protobuf::DescriptorPool.generated_pool.lookup("google.pubsub.loadtest.CheckResponse").msgclass
      ExecuteRequest = Google::Protobuf::DescriptorPool.generated_pool.lookup("google.pubsub.loadtest.ExecuteRequest").msgclass
      ExecuteResponse = Google::Protobuf::DescriptorPool.generated_pool.lookup("google.pubsub.loadtest.ExecuteResponse").msgclass
    end
  end
end
