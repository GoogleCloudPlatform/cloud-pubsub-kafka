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

package com.google.pubsub.clients.common;

import com.google.common.base.Stopwatch;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Each task is responsible for implementing its action and for creating {@link LoadTestRunner}.
 */
public abstract class Task implements Runnable {
  protected final MetricsHandler metricsHandler;
  protected final AtomicInteger numberOfMessages = new AtomicInteger(0);
  protected final AtomicInteger errorCount = new AtomicInteger(0);
  protected final Stopwatch wasteTime = Stopwatch.createUnstarted();

  protected Task(String project, String type, MetricsHandler.MetricName metricName) {
    this.metricsHandler = new MetricsHandler(project, type, metricName);
  }

  List<Long> getBucketValues() {
    return metricsHandler.flushBucketValues();
  }

  int getNumberOfMessages() {
    return numberOfMessages.get();
  }

  long getWasteElapsed() { return wasteTime.elapsed(TimeUnit.MILLISECONDS); }
}
