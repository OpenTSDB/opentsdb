// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.tsd;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.stats.StatsCollector.StatsTimer;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.YAML;

/**
 * A simple config and runner for a query.
 * 
 * @since 3.0
 */
public class QueryConfig implements TimerTask {
  private static Logger LOG = LoggerFactory.getLogger(QueryConfig.class);
  
  public static final String OVERALL_METRIC = "query.time.overall";
  public static final String SIZE_METRIC = "query.size.bytes";
  
  /** The non-null ID that's reported as the "id" tag for metrics. */
  protected String id;
  
  /** A list of fully qualified endpoints to post to, e.g. https://tsdb/api/query/graph */
  protected List<String> endpoints;
  
  /** A frequency to run the queries, e.g. "1m" to run every minute. */
  protected String frequency;
  
  /** The query to run. */
  protected SemanticQuery.Builder query_builder;
  
  /** A schedule timeout used to cancel runs. */
  protected Timeout timeout;
  
  /** A flag set when we've been canceled. */
  protected final AtomicBoolean cancel;
  
  /** Set to true wile queries are inflight and false when ready to run. */
  protected final AtomicBoolean outstanding;
  
  /** A latch of outstanding queries. */
  protected final AtomicInteger latch;
  
  /** The http client runner. */
  protected final CloseableHttpAsyncClient client;
  
  /** The TSDB reference. */
  protected final TSDB tsdb;
  
  /** The list of callbacks. */
  protected List<ResponseCallback> callbacks;
  
  private QueryConfig(final Builder builder) {
    this.id = builder.id;
    this.endpoints = builder.endpoints;
    this.frequency = builder.frequency;
    this.query_builder = builder.query_builder;
    this.client = builder.client;
    this.tsdb = builder.tsdb;
    cancel = new AtomicBoolean();
    outstanding = new AtomicBoolean();
    latch = new AtomicInteger();
    callbacks = Lists.newArrayListWithExpectedSize(endpoints.size());
    for (int i = 0; i < endpoints.size(); i++) {
      callbacks.add(new ResponseCallback(i, TsdbQueryRunner.TSDB));
    }
  }
  
  /**
   * Parses the config file.
   * @param tsdb The non-null TSDB.
   * @param 
   * @param file The non-null file.
   * @return The query config.
   * Throws IllegalArgumentException if a required config is missing or invalid.
   */
  public static QueryConfig parse(final TSDB tsdb, 
                                  final CloseableHttpAsyncClient client, 
                                  final File file) {
    try (final FileReader reader = new FileReader(file)) {
      final String yaml = CharStreams.toString(reader);
      final JsonNode node = YAML.getMapper().readTree(yaml);
      
      JsonNode temp = node.get("id");
      if (temp == null || temp.isNull()) {
        throw new IllegalArgumentException("File " + file + " was missing the ID.");
      }
      QueryConfig.Builder config = new Builder()
          .setTSDB(tsdb)
          .setClient(client);
      config.setId(temp.asText());
      
      temp = node.get("endpoints");
      if (temp == null || temp.isNull()) {
        throw new IllegalArgumentException("File " + file + " was missing the endpoints.");
      }
      config.setEndpoints(YAML.getMapper().treeToValue(temp, List.class));
      
      temp = node.get("frequency");
      if (temp == null || temp.isNull()) {
        throw new IllegalArgumentException("File " + file + " was missing the frequency.");
      }
      config.setFrequency(temp.asText());
      
      temp = node.get("query");
      if (temp == null || temp.isNull()) {
        throw new IllegalArgumentException("File " + file + " was missing the query.");
      }
      config.setQueryBuilder(SemanticQuery.parse(tsdb, temp));
      
      return config.build();
    } catch (FileNotFoundException e) {
      LOG.error("Failed to parse file: " + file);
      return null;
    } catch (IOException e) {
      LOG.error("Failed to parse file" + file);
      return null;
    }
  }
  
  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof QueryConfig)) {
      return false;
    }
    
    final QueryConfig other = (QueryConfig) obj;
    if (!Objects.equal(id, other.id) ||
        !Objects.equal(endpoints, other.endpoints) ||
        !Objects.equal(frequency, other.frequency)) {
      return false;
    }
    
    String local = JSON.serializeToString(query_builder.build());
    String remote = JSON.serializeToString(other.query_builder.build());
    return Objects.equal(local, remote);
  }

  @Override
  public void run(final Timeout timeout) throws Exception {
    if (cancel.get()) {
      LOG.debug("Cancelling query: " + id);
      return;
    }
    
    if (outstanding.compareAndSet(false, true)) {
      try {
        latch.set(endpoints.size());
        for (int i = 0; i < endpoints.size(); i++) {
          final HttpPost post = new HttpPost(endpoints.get(i));
          post.addHeader("Content-Type", "application/json");
          post.setEntity(new StringEntity(JSON.serializeToString(query_builder.build())));
          ResponseCallback cb = callbacks.get(i);
          cb.start();
          client.execute(post, cb);
          LOG.debug("Sent query " + id + " to endpoint " + endpoints.get(i));
        }
      } catch (Throwable t) {
        LOG.error("Failed to send query for: " + id, t);
      }
    } else {
      LOG.debug("Outstanding queries for: " + id + ". Skipping schedule.");
    }
    this.timeout = tsdb.getQueryTimer().newTimeout(this, 
        DateTime.parseDuration(frequency), 
        TimeUnit.MILLISECONDS);
  }
  
  /**
   * Called just after instantiation to start the first run with jitter.
   * @param 
   */
  public void schedule(final Random random) {
    long interval = DateTime.parseDuration(frequency);
    int rnd = random.nextInt((int) interval);
    int jitter = (int) interval - rnd;
    LOG.debug("Scheduling test for " + id + " in " + (jitter / 1000) + " seconds");
    timeout = tsdb.getQueryTimer().newTimeout(this, 
        jitter,
        TimeUnit.MILLISECONDS);
  }
  
  /** Cancels the query execution. */
  public void cancel() {
    cancel.set(true);
    if (timeout != null) {
      timeout.cancel();
    }
  }

  /**
   * Callback for the 
   * @author clarsen
   *
   */
  class ResponseCallback implements FutureCallback<HttpResponse> {
    private final int index;
    private final TSDB tsdb;
    private StatsTimer timer;
    
    ResponseCallback(final int index, final TSDB tsdb) {
      this.index = index;
      this.tsdb = tsdb;
    }
    
    public void start() {
      timer = tsdb.getStatsCollector().startTimer(OVERALL_METRIC, true);
    }
    
    @Override
    public void completed(final HttpResponse result) {
      stop(result.getStatusLine().getStatusCode());
      double duration = DateTime.msFromNanoDiff(
          DateTime.nanoTime(), timer.startTimeNanos()); 
      if (result.getStatusLine().getStatusCode() >= 200 &&
          result.getStatusLine().getStatusCode() < 300) {
        tsdb.getStatsCollector().setGauge(SIZE_METRIC, 
            result.getEntity().getContentLength(),
            "id", id, 
            "endpoint", endpoints.get(index));
      }
      try {
        EntityUtils.consume(result.getEntity());
      } catch (IOException e) {
        LOG.error("Unable to consume response", e);
      }
      LOG.debug("Finished query " + id + " against " + endpoints.get(index) 
        + " with code: " + result.getStatusLine().getStatusCode() + " in " 
        + duration + "ms");
    }

    @Override
    public void failed(final Exception ex) {
      stop(0);
      LOG.error("Failed query " + id + " against " + endpoints.get(index), ex);
    }

    @Override
    public void cancelled() {
      stop(0);
    }
    
    void stop(final int status_code) {
      timer.stop("id", id, 
                 "endpoint", endpoints.get(index), 
                 "status", Integer.toString(status_code));
      if (latch.decrementAndGet() == 0) {
        outstanding.set(false);
      }
    }
  }

  static class Builder {
    private String id;
    private List<String> endpoints;
    private String frequency;
    private SemanticQuery.Builder query_builder;
    private TSDB tsdb;
    private CloseableHttpAsyncClient client;
    
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    public Builder setEndpoints(final List<String> endpoints) {
      this.endpoints = endpoints;
      return this;
    }
    
    public Builder setFrequency(final String frequency) {
      this.frequency = frequency;
      return this;
    }
    
    public Builder setQueryBuilder(final SemanticQuery.Builder query_builder) {
      this.query_builder = query_builder;
      return this;
    }
    
    public Builder setTSDB(final TSDB tsdb) {
      this.tsdb = tsdb;
      return this;
    }
    
    public Builder setClient(final CloseableHttpAsyncClient client) {
      this.client = client;
      return this;
    }
    
    public QueryConfig build() {
      return new QueryConfig(this);
    }
  }
}
