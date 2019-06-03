// This file is part of OpenTSDB.
// Copyright (C) 2018-2019  The OpenTSDB Authors.
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
package net.opentsdb.query.execution;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import net.opentsdb.configuration.ConfigurationCallback;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.utils.Pair;
import net.opentsdb.utils.SharedHttpClient;

/**
 * The base factory for HTTP executors. It handles keeping track of host status'
 * and if a host is marked as down, it will make calls to the health endpoint
 * until it comes up.
 * 
 * @since 3.0
 */
public abstract class BaseHttpExecutorFactory implements 
    TimeSeriesDataSourceFactory, TimerTask {
  private static final Logger LOG = LoggerFactory.getLogger(
      BaseHttpExecutorFactory.class);
  
  public static final String MARKED_NEW_BAD_METRIC = "http.executor.health.new.failed";
  public static final String MARKED_STILL_BAD_METRIC = "http.executor.health.failed";
  public static final String MARKED_RECOVERED_METRIC = "http.executor.health.recovered";
  public static final String STATUS_METRIC = "http.executor.health.status";
  
  public static final String KEY_PREFIX = "opentsdb.http.executor.";
  public static final String RETRY_KEY = "retries";
  public static final String HOSTS_KEY = "hosts";
  public static final String HEADER_USER_KEY = "headers.user";
  public static final String ENDPOINT_KEY = "endpoint";
  public static final String HEALTH_ENDPOINT_KEY = "health.endpoint";
  public static final String HEALTH_INTERVAL_KEY = "health.interval";
  public static final String HEALTH_ENABLE_KEY = "health.enable";
  public static final String CLIENT_KEY = "client.id";
  public static final TypeReference<List<String>> TYPE_REF = 
      new TypeReference<List<String>>() { };
  
  /** The TSDB to pull configs from. */
  protected TSDB tsdb;
  
  /** Our ID. */
  protected String id;
  
  /** The list of hosts. */
  // TODO - atomic array to avoid syncing it
  protected List<Pair<String, Boolean>> hosts;
  
  /** The map of outstanding checks. */
  protected Map<String, TimerTask> checks; 
  
  /** Current index into the hosts list. */
  protected volatile int idx;
  
  /** Client for health checks and queries. */
  protected CloseableHttpAsyncClient client;
  
  /** How many times to retry a query on different hosts if they return bad. */
  protected int retries;
  
  BaseHttpExecutorFactory() {
    hosts = Lists.newArrayList();
    checks = Maps.newConcurrentMap();
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? null : id;
    this.tsdb = tsdb;
    registerConfigs(tsdb);
    
    final String client_id = tsdb.getConfig().getString(getConfigKey(CLIENT_KEY));
    final SharedHttpClient shared_client = tsdb.getRegistry().getPlugin(
        SharedHttpClient.class, client_id);
    if (shared_client == null) {
      throw new IllegalArgumentException("No shared HTTP client found "
          + "for ID: " + (Strings.isNullOrEmpty(client_id) ? 
              "Default" : client_id));
    }
    client = shared_client.getClient();
    retries = tsdb.getConfig().getInt(getConfigKey(RETRY_KEY));
    tsdb.getMaintenanceTimer().newTimeout(this, 60, TimeUnit.SECONDS);
    return Deferred.fromResult(null);
  }
  
  /** @return The number of times to retry bad hosts. */
  public int retries() {
    return retries;
  }
  
  /** 
   * Fetches the next "good" host if possible.
   * @return A non-null and non-empty string containing the host.
   * @throws IllegalStateException if all hosts have been iterated over and all
   * of them were bad.
   */
  public String nextHost() {
    synchronized (hosts) {
      for (int iterations = 0; iterations < hosts.size(); iterations++) {
        if (idx >= hosts.size()) {
          idx = 0;
        }
        final Pair<String, Boolean> entry = hosts.get(idx++);
        if (entry.getValue()) {
          return entry.getKey();
        }
      }
    }
    
    throw new IllegalStateException("No hosts were found in a healthy state "
        + "to satisfy the query.");
  }
  
  /**
   * Marks the host as "bad" in the list and fires off a timer task to check
   * the health endpoint periodically until the host is up.
   * @param host The non-null and non-empty host in the list.
   * @param code An HTTP status code for debugging.
   */
  public void markHostAsBad(final String host, final int code) {
    if (!tsdb.getConfig().getBoolean(getConfigKey(HEALTH_ENABLE_KEY))) {
      return;
    }
    
    // ignore all 400's but if the host is redirecting something is wrong.
    if (code >= 400 && code < 500) {
      return;
    }
    
    boolean marked = false;
    synchronized (hosts) {
      for (final Pair<String, Boolean> extant : hosts) {
        if (extant.getKey().equals(host)) {
          if (extant.getValue()) {
            marked = true;
            extant.setValue(false);
          }
        }
      }
    }
    
    // now we need to run a test to see if the host is up.
    class Check implements TimerTask {
      
      @Override
      public void run(final Timeout timeout) throws Exception {
        final String endpoint = tsdb.getConfig()
            .getString(getConfigKey(HEALTH_ENDPOINT_KEY));
        try {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Testing v3 HTTP host " + host + endpoint + " for health.");
          }
          class ResponseCallback implements FutureCallback<HttpResponse> {

            @Override
            public void completed(final HttpResponse result) {
              if (LOG.isTraceEnabled()) {
                try {
                  LOG.trace("Response from health check: " + host + " " 
                      + result + " => " + EntityUtils.toString(result.getEntity()));
                } catch (IOException e) { }
              }
              
              // TODO - redirects to a login page can cause a false positive here.
              // We may want to figure out what the content should be.
              if (result.getStatusLine().getStatusCode() == 200) {
                synchronized (hosts) {
                  for (final Pair<String, Boolean> extant : hosts) {
                    if (extant.getKey().equals(host)) {
                      if (!extant.getValue()) {
                        extant.setValue(true);
                        LOG.info("Recovered V3 HTTP host: " + host);
                        tsdb.getStatsCollector().incrementCounter(
                            MARKED_RECOVERED_METRIC, 
                            "remote", host, "status", "200");
                        // important to remove this from the checks map.
                        checks.remove(host);
                      }
                      break;
                    }
                  }
                }
              } else {
                if (LOG.isTraceEnabled()) {
                  LOG.trace("Host " + host + " is still down: " 
                      + result.getStatusLine().getStatusCode());
                }
                tsdb.getStatsCollector().incrementCounter(MARKED_STILL_BAD_METRIC, 
                    "remote", host, "status", 
                    Integer.toString(result.getStatusLine().getStatusCode()));
                reschedule();
              }
              
              try {
                EntityUtils.consume(result.getEntity());
              } catch (IOException e) { }
            }

            @Override
            public void failed(final Exception ex) {
              LOG.error("Failed to check host: " + host, ex);
              tsdb.getStatsCollector().incrementCounter(MARKED_STILL_BAD_METRIC, 
                  "remote", host, "status", "0");
              reschedule();
            }

            @Override
            public void cancelled() {
              LOG.error("Health checks are cancelled. We should be shutting "
                  + "down then.");
            }
            
          }
          
          final HttpGet request = new HttpGet(host + endpoint);
          client.execute(request, new ResponseCallback());
        } catch (Throwable t) {
          LOG.error("Failed executing check against host: " + host + endpoint, t);
          reschedule();
        }
      }
      
      void reschedule() {
        tsdb.getMaintenanceTimer().newTimeout(
            Check.this, 
            tsdb.getConfig().getLong(getConfigKey(HEALTH_INTERVAL_KEY)), 
            TimeUnit.MILLISECONDS);
        if (LOG.isTraceEnabled()) {
          LOG.trace("Rescheduling check for host " + host + " in " 
              + tsdb.getConfig().getLong(getConfigKey(HEALTH_INTERVAL_KEY)) + "ms.");
        }
      }
    }
    
    if (marked) {
      LOG.warn("Host " + host + " was marked as failed with code: " + code 
          + ". Will schedule health checks.");
      tsdb.getStatsCollector().incrementCounter(MARKED_NEW_BAD_METRIC, 
          "remote", host, "status", Integer.toString(code));
      TimerTask task = new Check();
      if (checks.putIfAbsent(host, task) == null) {
        tsdb.getMaintenanceTimer().newTimeout(
            task, 
            tsdb.getConfig().getLong(getConfigKey(HEALTH_INTERVAL_KEY)), 
            TimeUnit.MILLISECONDS);
      }
      // otherwise we lost a race so don't start it.
    }
  }
  
  @Override
  public void run(final Timeout timeout) {
    try {
      // ugg!!!!
      synchronized (hosts) {
        for (final Pair<String, Boolean> pair : hosts) {
          tsdb.getStatsCollector().setGauge(STATUS_METRIC, 
              pair.getValue() ? 1 : 0, 
              "remote", pair.getKey());
        }
      }
    } catch (Throwable t) {
      LOG.error("Unexpected exception processing stats", t);
    }
    tsdb.getMaintenanceTimer().newTimeout(this, 60, TimeUnit.SECONDS);
  }
  
  /**
   * Helper to build the config key with a factory id.
   * @param suffix The non-null and non-empty config suffix.
   * @return The key containing the id.
   */
  @VisibleForTesting
  String getConfigKey(final String suffix) {
    if (id == null) { 
      return KEY_PREFIX + suffix;
    } else {
      return KEY_PREFIX + id + "." + suffix;
    }
  }
  
  /** A callback for the sources that updates default_sources. */
  class SettingsCallback implements ConfigurationCallback<Object> {

    @Override
    public void update(final String key, final Object value) {
      if (key.equals(getConfigKey(HOSTS_KEY))) {
        if (value == null) {
          return;
        }
        
        final List<String> new_config = (List<String>) value;
        if (new_config == null || new_config.isEmpty()) {
          LOG.error("New config was null or empty.");
          return;
        }
        
        synchronized (hosts) {
          for (final String new_host : new_config) {
            if (Strings.isNullOrEmpty(new_host)) {
              LOG.error("Null or empty host name found in the config.");
              break;
            }
            
            boolean matched = false;
            for (final Pair<String, Boolean> extant : hosts) {
              if (extant.getKey().equals(new_host)) {
                matched = true;
                break;
              }
            }
            
            if (!matched) {
              hosts.add(new Pair<String, Boolean>(new_host, true));
              if (LOG.isDebugEnabled()) {
                LOG.debug("Adding new host: " + new_host);
              }
            }
          }
          
          final Iterator<Pair<String, Boolean>> iterator = hosts.iterator();
          while (iterator.hasNext()) {
            final Pair<String, Boolean> extant = iterator.next();
            if (!new_config.contains(extant.getKey())) {
              iterator.remove();
              if (LOG.isDebugEnabled()) {
                LOG.debug("Removing host: " + extant.getKey());
              }
            }
          }
        }
      }
    }
    
  }
  
  /**
   * Helper to register the configs.
   * @param tsdb A non-null TSDB.
   */
  @VisibleForTesting
  void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(getConfigKey(HOSTS_KEY))) {
      tsdb.getConfig().register(
          ConfigurationEntrySchema.newBuilder()
          .setKey(getConfigKey(HOSTS_KEY))
          .setType(TYPE_REF)
          .setDescription("The list of hosts including protocol, host and port.")
          .isDynamic()
          .isNullable()
          .setSource(getClass().getName())
          .build());
    }
    tsdb.getConfig().bind(getConfigKey(HOSTS_KEY), new SettingsCallback());
    
    if (!tsdb.getConfig().hasProperty(getConfigKey(RETRY_KEY))) {
      tsdb.getConfig().register(getConfigKey(RETRY_KEY), 3, true,
          "How many times to retry a query to bad hosts.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(HEALTH_ENDPOINT_KEY))) {
      tsdb.getConfig().register(getConfigKey(HEALTH_ENDPOINT_KEY), "/api/stats", true,
          "The endpoint to query for health status.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(HEALTH_INTERVAL_KEY))) {
      tsdb.getConfig().register(getConfigKey(HEALTH_INTERVAL_KEY), "5000", true,
          "The frequency, in milliseconds, to test the health when a host is "
          + "marked as down.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(HEALTH_ENABLE_KEY))) {
      tsdb.getConfig().register(getConfigKey(HEALTH_ENABLE_KEY), true, true,
          "Whether or not to enable the health checks.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(CLIENT_KEY))) {
      tsdb.getConfig().register(getConfigKey(CLIENT_KEY), 
          null, false,
          "The ID of the SharedHttpClient plugin to use. Defaults to `null`.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(HEADER_USER_KEY))) {
      tsdb.getConfig().register(getConfigKey(HEADER_USER_KEY), 
          "X-OpenTSDB-User", true,
          "An optional custom header to store the user in when making a "
          + "downstream request. If null or empty then the header will not "
          + "be sent.");
    }
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }
}
