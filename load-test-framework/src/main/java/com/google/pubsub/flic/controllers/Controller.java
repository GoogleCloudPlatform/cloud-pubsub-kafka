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

package com.google.pubsub.flic.controllers;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.pubsub.flic.common.LatencyDistribution;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Each subclass of Controller is responsible for instantiating and cleaning up a given environment.
 * When an environment is started, it adds {@link Client} objects to the clients array, which is
 * used to start the load test and collect results. This base class manages every
 * environment-agnostic part of this process.
 */
public abstract class Controller {
  protected static final Logger log = LoggerFactory.getLogger(Controller.class);
  public static String resourceDirectory = "src/main/resources/gce";
  protected final List<Client> clients = new ArrayList<>();
  protected final ScheduledExecutorService executor;

  /**
   * Creates the given environments and starts the virtual machines. When this function returns,
   * each client is guaranteed to have been connected and be network reachable, but is not started.
   * If an error occurred attempting to start the environment, the environment will be shut down,
   * and an Exception will be thrown. It is not guaranteed that we have completed shutting down when
   * this function returns, but it is guaranteed that we are in process.
   *
   * @param executor the executor that will be used to schedule all environment initialization tasks
   */
  public Controller(ScheduledExecutorService executor) {
    this.executor = executor;
  }

  /**
   * Shuts down the given environment. When this function returns, each client is guaranteed to be
   * in process of being deleted, or else output directions on how to manually delete any potential
   * remaining instances if unable.
   *
   * @param t the error that caused the shutdown, or null if shutting down successfully
   */
  public abstract void shutdown(Throwable t);

  /**
   * @return the types map
   */
  public abstract Map<String, Map<ClientParams, Integer>> getTypes();

  /**
   * Waits for clients to complete the load test.
   */
  public void waitForClients() throws Throwable {
    try {
      Futures.allAsList(clients.stream()
          .map(Client::getDoneFuture)
          .collect(Collectors.toList())
      ).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  /**
   * Waits for publishers to complete the load test.
   */
  public void waitForPublisherClients() throws Throwable {
    try {
      Futures.allAsList(clients.stream()
          .filter(c -> c.getClientType().isPublisher())
          .map(Client::getDoneFuture)
          .collect(Collectors.toList())
      ).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  /**
   * Gets the current statistics for the given type.
   *
   * @param type the client type to aggregate results for
   * @return the results from the load test up to this point
   */
  private LoadtestStats getStatsForClientType(Client.ClientType type) {
    List<Client> clientsOfType = clients.stream()
        .filter(c -> c.getClientType() == type).collect(Collectors.toList());
    if (clientsOfType.size() == 0) {
      // There are no clients of this type, so return null
      return null;
    }
    LoadtestStats stats = new LoadtestStats();
    Optional<Client> longestRunningClient = clientsOfType.stream()
        .max((a, b) -> Long.compare(a.getRunningSeconds(), b.getRunningSeconds()));
    stats.runningSeconds =
        longestRunningClient.isPresent() ? longestRunningClient.get().getRunningSeconds()
            : System.currentTimeMillis() / 1000 - Client.startTime.getSeconds();
    clientsOfType.forEach(client -> {
      long[] bucketValues = client.getBucketValues();
      for (int i = 0; i < LatencyDistribution.LATENCY_BUCKETS.length; i++) {
        stats.bucketValues[i] += bucketValues[i];
      }
      stats.wasteMillis = client.getWastedMillis();
    });
    return stats;
  }

  /**
   * Gets the results for all available types.
   *
   * @return the map from type to result, every type running is a valid key
   */
  public Map<Client.ClientType, LoadtestStats> getStatsForAllClientTypes() {
    final Map<Client.ClientType, LoadtestStats> results = new HashMap<>();
    List<ListenableFuture<Void>> resultFutures = new ArrayList<>();
    for (Client.ClientType type : Client.ClientType.values()) {
      SettableFuture<Void> resultFuture = SettableFuture.create();
      resultFutures.add(resultFuture);
      executor.submit(() -> {
        try {
          LoadtestStats stats = getStatsForClientType(type);
          if (stats != null) {
            results.put(type, stats);
          }
          resultFuture.set(null);
        } catch (Throwable t) {
          resultFuture.setException(t);
        }
      });
    }
    try {
      Futures.allAsList(resultFutures).get();
    } catch (ExecutionException | InterruptedException e) {
      log.error("Failed health check, will return results accumulated during test up to now.",
          e instanceof ExecutionException ? e.getCause() : e);
    }
    return results;
  }

  /**
   * Sends a LoadtestFramework.Start RPC to all clients to commence the load test. When this
   * function returns it is guaranteed that all clients have started.
   */
  public void startClients(MessageTracker messageTracker) {
    SettableFuture<Void> startFuture = SettableFuture.create();
    clients.forEach((client) -> executor.execute(() -> {
      try {
        client.start(messageTracker);
        startFuture.set(null);
      } catch (Throwable t) {
        startFuture.setException(t);
      }
    }));
    try {
      startFuture.get();
    } catch (ExecutionException e) {
      shutdown(e.getCause());
    } catch (InterruptedException e) {
      shutdown(e);
    }
  }

  /** The statistics that are exported by each load test client. */
  public static class LoadtestStats {
    public long runningSeconds;
    public long[] bucketValues = new long[LatencyDistribution.LATENCY_BUCKETS.length];
    public long wasteMillis;
  }
}

