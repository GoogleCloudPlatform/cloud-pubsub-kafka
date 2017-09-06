# Generated by the protocol buffer compiler.  DO NOT EDIT!
# Source: src/main/proto/loadtest.proto for package 'google.pubsub.loadtest'

require 'grpc'
require './loadtest_pb'

module Google
  module Pubsub
    module Loadtest
      module Loadtest
        class Service

          include GRPC::GenericService

          self.marshal_class_method = :encode
          self.unmarshal_class_method = :decode
          self.service_name = 'google.pubsub.loadtest.Loadtest'

          # Starts a load test
          rpc :Start, StartRequest, StartResponse
          # Checks the status of a load test
          rpc :Check, CheckRequest, CheckResponse
        end

        Stub = Service.rpc_stub_class
      end
      module LoadtestWorker
        class Service

          include GRPC::GenericService

          self.marshal_class_method = :encode
          self.unmarshal_class_method = :decode
          self.service_name = 'google.pubsub.loadtest.LoadtestWorker'

          # Starts a worker
          rpc :Start, StartRequest, StartResponse
          # Executes a command on the worker, returning the latencies of the operations. Since some
          # commands consist of multiple operations (i.e. pulls contain many received messages with
          # different end to end latencies) a single command can have multiple latencies returned.
          rpc :Execute, ExecuteRequest, ExecuteResponse
        end

        Stub = Service.rpc_stub_class
      end
    end
  end
end
