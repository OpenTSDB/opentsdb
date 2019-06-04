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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import jersey.repackaged.com.google.common.collect.Maps;
import net.opentsdb.common.Const;
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
  
  public static final String METRIC_PREFIX = "query.";
  public static final String OVERALL_METRIC = "query.time.overall";
  public static final String SIZE_METRIC = "query.size.bytes";
  public static final Pattern CURL_METRIC_PATTERN = 
      Pattern.compile("^([a-zA-Z\\.]+):\\s+([0-9\\.]+)$");
  
  /** The non-null ID that's reported as the "id" tag for metrics. */
  protected final String id;
  
  /** A list of fully qualified endpoints to post to, e.g. https://tsdb/api/query/graph */
  protected final List<String> endpoints;
  protected final List<String> shuffled_endpoints;
  
  /** A frequency to run the queries, e.g. "1m" to run every minute. */
  protected final String frequency;
  
  /** The query to run. */
  protected final SemanticQuery.Builder query_builder;
  
  /** A schedule timeout used to cancel runs. */
  protected Timeout timeout;
  
  /** A flag set when we've been canceled. */
  protected final AtomicBoolean cancel;
  
  /** Set to true wile queries are inflight and false when ready to run. */
  protected final AtomicBoolean outstanding;
  
  /** A latch of outstanding queries. */
  protected final AtomicInteger latch;
  
  /** The runner. */
  protected final TsdbQueryRunner runner;
  
  /** Random number generator used for jitter and scheduling. */
  protected final Random rnd;
  
  /** The list of callbacks. */
  protected List<ResponseCallback> callbacks;
  
  private QueryConfig(final Builder builder) {
    this.id = builder.id;
    this.endpoints = builder.endpoints;
    shuffled_endpoints = Lists.newArrayList(endpoints);
    this.frequency = builder.frequency;
    this.query_builder = builder.query_builder;
    this.runner = builder.runner;
    rnd = new Random(DateTime.currentTimeMillis());
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
   * @param runner The query runner.
   * @param file The non-null file.
   * @return The query config.
   * Throws IllegalArgumentException if a required config is missing or invalid.
   */
  public static QueryConfig parse(final TsdbQueryRunner runner, 
                                  final File file) {
    try (final FileReader reader = new FileReader(file)) {
      final String yaml = CharStreams.toString(reader);
      final JsonNode node = YAML.getMapper().readTree(yaml);
      
      JsonNode temp = node.get("id");
      if (temp == null || temp.isNull()) {
        throw new IllegalArgumentException("File " + file + " was missing the ID.");
      }
      QueryConfig.Builder config = new Builder()
          .setRunner(runner);
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
      config.setQueryBuilder(SemanticQuery.parse(TsdbQueryRunner.TSDB, temp));
      
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
    
    // TODO - WARNING: This is not a good long-term solution as we need to 
    // build the query and use the .equals() on the constructed queries.
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
        if (shuffled_endpoints.size() > 0) {
          Collections.shuffle(shuffled_endpoints);
        }
        
        latch.set(shuffled_endpoints.size());
        if (runner.use_curl) {
          // write the query
          Files.write(JSON.serializeToBytes(query_builder.build()), 
              new File(runner.curl_temp + id + ".json"));
          TsdbQueryRunner.TSDB.getQueryThreadPool().submit(new CurlRunner(0));
        } else {
          final String endpoint = shuffled_endpoints.get(0);
          try {
            final HttpPost post = new HttpPost(endpoint);
            post.addHeader("Content-Type", "application/json");
            post.setEntity(new StringEntity(JSON.serializeToString(
                query_builder.build())));
            ResponseCallback cb = callbacks.get(0);
            cb.start();
            runner.client.execute(post, cb);
            LOG.debug("Sent first query " + id + " to endpoint " + endpoint);
          } catch (Exception e) {
            LOG.error("Failed to send query for: " + id + " and endpoint " 
                + endpoint, e);
            if (latch.decrementAndGet() <= 0) {
              outstanding.set(false);
            }
          }
        }
      } catch (Throwable t) {
        LOG.error("Failed to send query for: " + id, t);
      }
    } else {
      LOG.debug("Outstanding queries for: " + id + ". Skipping schedule.");
    }
    this.timeout = TsdbQueryRunner.TSDB.getQueryTimer().newTimeout(this, 
        DateTime.parseDuration(frequency), 
        TimeUnit.MILLISECONDS);
  }
  
  /**
   * Called just after instantiation to start the first run with jitter.
   * @param 
   */
  public void schedule() {
    long interval = DateTime.parseDuration(frequency);
    int num = rnd.nextInt((int) interval);
    int jitter = (int) interval - num;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Scheduling test for " + id + " in " + (jitter / 1000) + " seconds");
    }
    timeout = TsdbQueryRunner.TSDB.getQueryTimer().newTimeout(this, 
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
    private volatile StatsTimer timer;
    
    ResponseCallback(final int index, final TSDB tsdb) {
      this.index = index;
      this.tsdb = tsdb;
    }
    
    public synchronized void start() {
      timer = tsdb.getStatsCollector().startTimer(OVERALL_METRIC, ChronoUnit.MILLIS);
    }
    
    @Override
    public void completed(final HttpResponse result) {
      stop(result.getStatusLine().getStatusCode());
      double duration = DateTime.currentTimeMillis() - timer.startTime();
      int content_length = 0;
      if (result.getStatusLine().getStatusCode() >= 200 &&
          result.getStatusLine().getStatusCode() < 300) {
        byte[] content;
        try {
          content = EntityUtils.toByteArray(result.getEntity());
          content_length = content.length;
          tsdb.getStatsCollector().setGauge(SIZE_METRIC, 
              content_length,
              "id", id, 
              "endpoint", shuffled_endpoints.get(index));
        } catch (IOException e) {
          LOG.error("Failed to parse response content", e);
        }
      }
      try {
        EntityUtils.consume(result.getEntity());
      } catch (IOException e) {
        LOG.error("Unable to consume response", e);
      }
      LOG.debug("Finished query " + id + " against " + shuffled_endpoints.get(index) 
        + " with code: " + result.getStatusLine().getStatusCode() + " in " 
        + duration + "ms for " + content_length + " bytes");
    }

    @Override
    public void failed(final Exception ex) {
      stop(0);
      LOG.error("Failed query " + id + " against " + shuffled_endpoints.get(index), ex);
    }

    @Override
    public void cancelled() {
      stop(0);
    }
    
    void stop(final int status_code) {
      synchronized (this) {
        timer.stop("id", id, 
                   "endpoint", shuffled_endpoints.get(index), 
                   "status", Integer.toString(status_code));
      }
      if (latch.decrementAndGet() <= 0) {
        outstanding.set(false);
      } else {
        final String endpoint = shuffled_endpoints.get(index + 1);
        try {
          final HttpPost post = new HttpPost(endpoint);
          post.addHeader("Content-Type", "application/json");
          post.setEntity(new StringEntity(JSON.serializeToString(
              query_builder.build())));
          ResponseCallback cb = callbacks.get(index + 1);
          cb.start();
          runner.client.execute(post, cb);
          LOG.debug("Sent next query " + id + " to endpoint " + endpoint);
        } catch (Exception e) {
          LOG.error("Failed to send query for: " + id + " and endpoint " 
              + endpoint, e);
          if (latch.decrementAndGet() == 0) {
            outstanding.set(false);
          }
        }
      }
    }
  }

  class CurlRunner implements Runnable {
    final int index;
    
    CurlRunner(final int index) {
      this.index = index;
    }
    
    @Override
    public void run() {
      try {
        final String temp_file = runner.curl_temp + id + "_" + index + ".log";
        File f = new File(temp_file);
        if (f.exists()) {
          try {
            f.delete();
          } catch (Exception e) {
            LOG.error("Failed to remove temp file: " + f);
          }
        }
        LOG.info("Sending CURL query " + id  + " to endpoint " 
            + shuffled_endpoints.get(index));
        final ProcessBuilder builder = new ProcessBuilder();
        final List<String> args = Lists.newArrayList();
        args.add(runner.curl_exec);
        args.add("-sS"); // suppress the progress bar but keep errors.
        //args.add("-vv"); // for debugging
        args.add("-w");
        args.add("\"" + TsdbQueryRunner.CURL_STATUS + runner.curl_metrics + "\"");
        args.add("-o");
        args.add(temp_file);
        args.add("-X");
        args.add("POST");
        args.add("-H");
        args.add("Content-Type: application/json");
        args.add("-d");
        args.add("@" + runner.curl_temp + id + ".json");
        args.add("-m");
        args.add(Integer.toString(runner.timeout / 1000));
        if (!Strings.isNullOrEmpty(runner.curl_flags)) {
          // TODO - handle quotes if we need em.
          final String[] parts = runner.curl_flags.split(" ");
          for (int i = 0; i < parts.length; i++) {
            args.add(parts[i]);
          }
        }
        args.add(shuffled_endpoints.get(index));
        builder.command(args);
        builder.redirectErrorStream(true);
        builder.directory(new File(runner.curl_temp));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Running command: " + Joiner.on(" ").join(builder.command()));
        }
        final Process process = builder.start();
        
        final Map<String, String> metrics = Maps.newHashMap();
        final StringBuilder buffer = new StringBuilder();
        String error = null;
        try (final InputStream is = process.getInputStream()) {
          try (final InputStreamReader isr = new InputStreamReader(is)) {
            try (final BufferedReader br = new BufferedReader(isr)) {
              String line;  
              while ((line = br.readLine()) != null) {
                buffer.append(line).append("\n");
                
                if (line.startsWith("curl:")) {
                  // error msg!
                  error = line;
                } else {
                  // metric!
                  final Matcher matcher = CURL_METRIC_PATTERN.matcher(line);
                  if (matcher.matches()) {
                    metrics.put(matcher.group(1), matcher.group(2));
                  }
                }
              }
            }
          }
        }
        if (LOG.isTraceEnabled()) {
          LOG.trace("ID: " + id + "\n" + buffer.toString());
        }
        
        final int status_code = Integer.parseInt(metrics.get("status"));
        if (status_code != 200 && status_code != 204) {
          LOG.error("Query " + id + " to endpoint " 
              + shuffled_endpoints.get(index) + " failed with code: " 
              + status_code + " and error: " + error);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Query " + id + " to endpoint " 
                + shuffled_endpoints.get(index) + " failed with code: " 
                + status_code + " and error: " + error + "\n" +
                (new File(temp_file).exists() ? 
                Files.readLines(new File(temp_file), Const.UTF8_CHARSET) :
                  "{no file at " + temp_file + "}"));
          }
        } else {
          LOG.info("Finished query " + id + " against " + shuffled_endpoints.get(index) 
            + " with code: " + status_code + " in " 
            + metrics.get("total.time") + "s for " + metrics.get("download.size") + " bytes");
        }
        
        final String[] tags = new String[] {
            "id", id, 
            "endpoint", shuffled_endpoints.get(index), 
            "status", Integer.toString(status_code)
        };
        
        for (final Entry<String, String> metric : metrics.entrySet()) {
          if (metric.getKey().equals("status")) {
            continue;
          }
          
          if (status_code == 0 && metric.getKey().equals("total.time")) {
            if (LOG.isTraceEnabled()) {
              LOG.trace(id + " " + METRIC_PREFIX + metric.getKey() + " 0");
            }
            TsdbQueryRunner.TSDB.getStatsCollector().setGauge(
                METRIC_PREFIX + metric.getKey(), 
                0.0, 
                tags);
            continue;
          }
          
          if (metric.getValue().contains(".")) {
            if (LOG.isTraceEnabled()) {
              LOG.trace(id + " " + METRIC_PREFIX + metric.getKey() + " " 
                  + Double.parseDouble(metric.getValue()));
            }
            TsdbQueryRunner.TSDB.getStatsCollector().setGauge(
                METRIC_PREFIX + metric.getKey(), 
                Double.parseDouble(metric.getValue()), 
                tags);
          } else {
            if (LOG.isTraceEnabled()) {
              LOG.trace(id + " " + METRIC_PREFIX + metric.getKey() + " " 
                  + Long.parseLong(metric.getValue()));
            }
            TsdbQueryRunner.TSDB.getStatsCollector().setGauge(
                METRIC_PREFIX + metric.getKey(), 
                Long.parseLong(metric.getValue()), 
                tags);
          }
        }
      } catch (Throwable t) {
        LOG.error("Failed to run query: " + shuffled_endpoints.get(index), t);
      } finally {
        if (index + 1 < shuffled_endpoints.size()) {
          TsdbQueryRunner.TSDB.getQueryThreadPool().submit(new CurlRunner(index + 1));
        }
        if (latch.decrementAndGet() <= 0) {
          outstanding.set(false);
        }
      }
    }
    
  }
  
  static class Builder {
    private String id;
    private List<String> endpoints;
    private String frequency;
    private SemanticQuery.Builder query_builder;
    private TsdbQueryRunner runner;
    
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
    
    public Builder setRunner(final TsdbQueryRunner runner) {
      this.runner = runner;
      return this;
    }
    
    public QueryConfig build() {
      return new QueryConfig(this);
    }
  }
}
