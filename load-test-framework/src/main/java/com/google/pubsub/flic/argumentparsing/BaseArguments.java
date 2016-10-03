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
package com.google.pubsub.flic.argumentparsing;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.pubsub.flic.common.Utils.GreaterThanZeroValidator;
import java.util.ArrayList;
import java.util.List;

/** Command line options that are not associated with any command. */
@Parameters(separators = "=")
public class BaseArguments {

  @Parameter(
    names = {"--help"},
    help = true
  )
  private boolean help = false;

  @Parameter(
    names = {"--topics", "-t"},
    description = "Topics to publish/consume from."
  )
  private List<String> topics = new ArrayList<>();

  @Parameter(
    names = {"--dump_data", "-d"},
    description = "Whether to dump relevant message data (only when consuming messages)."
  )
  private boolean dumpData = false;

  @Parameter(
    names = {"--publish", "-p"},
    description = "Publish mode (default).",
    arity = 1
  )
  private boolean publish = true;

  @Parameter(
    names = {"--num_messages", "-n"},
    description = "Total number of messages to publish or consume.",
    validateWith = GreaterThanZeroValidator.class
  )
  private int numMessages = 10000;

  @Parameter(
    names = {"--message_size", "-m"},
    description = "Message size in bytes (only when publishing messages).",
    validateWith = GreaterThanZeroValidator.class
  )
  private int messageSize = 10;

  public boolean isHelp() {
    return help;
  }

  public List<String> getTopics() {
    return topics;
  }

  public boolean isPublish() {
    return publish;
  }

  public boolean isDumpData() {
    return dumpData;
  }

  public int getNumMessages() {
    return numMessages;
  }

  public int getMessageSize() {
    return messageSize;
  }
}
