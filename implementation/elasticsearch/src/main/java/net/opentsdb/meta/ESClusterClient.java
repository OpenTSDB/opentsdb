// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.meta;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Maps;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import net.opentsdb.configuration.ConfigurationException;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.stats.Span;

/**
 * A single cluster client.
 *
 * @since 3.0
 */
public class ESClusterClient extends BaseTSDBPlugin implements ESClient {
  private static final Logger LOG = LoggerFactory.getLogger(
      ESClusterClient.class);

  public static final String TYPE = "ESClusterClient";

  public static final String HOSTS_KEY = "es.hosts";
  public static final String CLUSTERS_KEY = "es.clusters";
  public static final String PING_TIMEOUT_KEY = "es.ping_timeout";
  public static final String QUERY_TIMEOUT_KEY = "es.query_timeout";
  public static final String EXCLUDES_KEY = "es.excludes";
  public static final String FALLBACK_ON_EX_KEY = "es.fallback.exception";
  public static final String FALLBACK_ON_NO_DATA_KEY = "es.fallback.nodata";
  public static final long QUERY_TIMEOUT_DEFAULT = 5000;
  public static final String EXCLUDES_DEFAULT = 
      "AM_nested,lastSeenTime,firstSeenTime,application.raw,timestamp";
  public static final String SNIFF_KEY = "es.sniff";
  public static final String PING_TIMEOUT_DEFAULT = "30s";
  public static final int DEFAULT_PORT = 9300;

  /** TSDB for stats. */
  private TSDB tsdb;

  /** The list of cluster clients, one per site. */
  protected List<TransportClient> clients;
  
  /** Cluster IDs to parse. */
  protected List<String> clusters;

  /** Fields to exclude from the results to save on serdes. */
  protected String[] excludes;
  
  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    this.tsdb = tsdb;
    registerConfigs(tsdb);
    
    String raw_hosts = tsdb.getConfig().getString(HOSTS_KEY);
    if (Strings.isNullOrEmpty(raw_hosts)) {
      return Deferred.fromError(new ConfigurationException(
          "Missing the hosts config '" + HOSTS_KEY + "'"));
    }
    final String[] hosts = raw_hosts.split(",");
    if (hosts.length < 1) {
      return Deferred.fromError(new ConfigurationException(
          "Must have at least one host in '" + HOSTS_KEY 
          + "'"));
    }
    String raw_clusters = tsdb.getConfig().getString(CLUSTERS_KEY);
    if (Strings.isNullOrEmpty(raw_clusters)) {
      return Deferred.fromError(new ConfigurationException(
          "Missing the clusters config '" + CLUSTERS_KEY + "'"));
    }
    final String[] clusters = raw_clusters.split(",");
    
    String temp = tsdb.getConfig().getString(EXCLUDES_KEY);
    if (!Strings.isNullOrEmpty(temp)) {
      excludes = temp.split(",");
    } else {
      excludes = null;
    }
    
    clients = Lists.newArrayListWithCapacity(hosts.length);
    try {
      for (int i = 0; i < hosts.length; i++) {
        final String host = hosts[i];
        final String[] host_and_port = host.split(":");
        if (host_and_port.length < 1) {
          throw new ConfigurationException("Failed to parse host: " + host);
        }
        final Settings settings = ImmutableSettings.settingsBuilder()
            .put("cluster.name", clusters[i])
            .put("client.transport.ping_timeout", 
                tsdb.getConfig().getString(PING_TIMEOUT_KEY))
            .put("client.transport.sniff", 
                tsdb.getConfig().getBoolean(SNIFF_KEY))
            .build();
        final TransportClient client = new TransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(host_and_port[0], 
              host_and_port.length == 1 ? DEFAULT_PORT : 
                Integer.parseInt(host_and_port[1])));
        clients.add(client);
        LOG.info("Instantiated ES client for " + host);
      }
      
      this.clusters = Lists.newArrayList(clusters);
      
      LOG.info("Finished initializing ES clients.");
      return Deferred.fromResult(null);
    } catch (Exception e) {
      LOG.error("Failed to initialize ES clients", e);
      return Deferred.fromError(e);
    }
  }

  @Override
  public Deferred<Object> shutdown() {
    try {
      for (final TransportClient client : clients) {
        if (client != null) {
          client.close();
        }
      }
      LOG.info("Finished shutting down ES clients.");
      return Deferred.fromResult(null);
    } catch (Exception e) {
      LOG.error("Failed to close ES client", e);
      return Deferred.fromError(e);
    }
  }

  @Override
  public String version() {
    return "3.0.0";
  }
  
  @Override
  public Deferred<Map<String, MultiSearchResponse>> runQuery(final Map<String,
          SearchSourceBuilder> query,
                                                 final QueryPipelineContext context,
                                                 final Span span) {
    if (query == null) {
      return Deferred.fromError(new IllegalArgumentException(
          "Query cannot be null."));
    }

    final Span child;
    if (span != null) {
      child = span.newChild(getClass() + "runQuery").start();
    } else {
      child = null;
    }

    final Deferred<Map<String, MultiSearchResponse>> deferred =  new
            Deferred<Map<String, MultiSearchResponse>>();
    final Map<String, MultiSearchResponse> results = Maps
            .newHashMapWithExpectedSize(clients.size());

    final AtomicInteger latch = new AtomicInteger(clients.size());
    final List<Timeout> timeouts = Lists.newArrayListWithExpectedSize(clients.size());
    
    for (int i = 0; i < clients.size(); i++) {
      final TransportClient client = clients.get(i);
      final int idx = i;
      final Span local;
      if (child != null) {
        local = child.newChild(getClass().getSimpleName() + ".runQuery" 
            + "." + clusters.get(i))
            .withTag("cluster", clusters.get(i))
            .start();
      } else {
        local = null;
      }
      
      class QueryTimer implements TimerTask {
        @Override
        public void run(final Timeout timeout) {
          synchronized (timeouts) {
            timeouts.set(idx, null);
            
            if (local != null) {
              local.setErrorTags()
                   .setTag("Error", "Query timed out against client: " 
                       + clusters.get(idx) + " after " 
                       + tsdb.getConfig().getInt(QUERY_TIMEOUT_KEY) + "ms")
                   .finish();
            }
            
            LOG.warn("Query timed out against ES client: " + clusters.get(idx));
            if (context != null) {
              context.queryContext().logError("Meta data query "
                  + "to Elastic Search timed out: " + clusters.get(idx) + " after " 
                  + tsdb.getConfig().getInt(QUERY_TIMEOUT_KEY) + "ms");
            }

            if (tsdb.getStatsCollector() != null) {
              tsdb.getStatsCollector().incrementCounter("es.client.query.exception", "cluster", clusters.get(idx),
                  "timeout", "true");
            }

            if (latch.decrementAndGet() < 1) {
              if (results.isEmpty()) {
                if (child != null) {
                  child.setErrorTags()
                    .setTag("Error", "Query timed out against client: " + clusters.get(idx))
                    .finish();
                }
                deferred.callback(new QueryExecutionException("Meta data query "
                    + "to Elastic Search timed out: " + clusters.get(idx) + " after " 
                        + tsdb.getConfig().getInt(QUERY_TIMEOUT_KEY) + "ms", 408));
              } else {
                if (child != null) {
                  child.setSuccessTags().finish();
                }
                deferred.callback(results);
              }
            }
          }
        }
      }
      
      class FutureCB implements ActionListener<MultiSearchResponse> {
        @Override
        public void onFailure(final Throwable e) {
          if (local != null) {
            local.setErrorTags(e).finish();
          }
          
          if (context != null) {
            context.queryContext().logError("Exception from ES cluster " 
                + clusters.get(idx) + ": " + e.getMessage());
          }

          if (tsdb.getStatsCollector() != null) {
            tsdb.getStatsCollector().incrementCounter("es.client.query.exception", "cluster", clusters.get(idx),
                "timeout", "false");
          }
          LOG.error("Unexpected failure from ES client", e);
          if (latch.decrementAndGet() < 1) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Failed to query ES cluster at: " + client, e);
            }
            if (results.isEmpty()) {
              if (child != null) {
                child.setErrorTags(e).finish();
              }
              deferred.callback(e);
            } else {
              if (child != null) {
                child.setSuccessTags().finish();
              }
              deferred.callback(results);
            }
          }
        }
        
        @Override
        public void onResponse(final MultiSearchResponse response) {
          synchronized (timeouts) {
            if (timeouts.get(idx) == null) {
              LOG.warn("ES client " + clusters.get(idx)
                  + " and responded after the timeout.");
              return;
            }
            timeouts.get(idx).cancel();
          }

          if (local != null) {
            local.setSuccessTags()
                 .setTag("responses", response.getResponses().length)
                 .finish();
          }
          
          if (context != null && context.query().isDebugEnabled()) {
            context.queryContext().logDebug("Response from cluster "
                + clusters.get(idx) + " had " + response.getResponses().length
                + " responses");
          }

          if (tsdb.getStatsCollector() != null) {
            for (MultiSearchResponse.Item each_response : response.getResponses()) {
            SearchHit[] hits = each_response.getResponse().getHits().hits();
            tsdb.getStatsCollector().incrementCounter("es.client.query.success", "namespace",
                 hits[0].getIndex(), "cluster", clusters.get(idx));
            }
          }
          synchronized (results) {
            results.put(clusters.get(idx), response);
          }
          if (latch.decrementAndGet() < 1) {
            if (child != null) {
              child.setSuccessTags().finish();
            }
            deferred.callback(results);
          }
        }
      }

      try {
        final MultiSearchRequestBuilder multi_search = client
                .prepareMultiSearch();
        for (final Map.Entry<String, SearchSourceBuilder>
                search_source_builder : query
                .entrySet()) {
          if (tsdb.getStatsCollector() != null) {
            tsdb.getStatsCollector().incrementCounter("es.client.query.new", "endpoint", "meta", "namespace",
                search_source_builder.getKey(), "cluster", clusters.get(idx));
          }
          final SearchRequestBuilder request_builder = client
                  .prepareSearch(search_source_builder.getKey())
                  .setSearchType(SearchType.DEFAULT)
                  .setExtraSource(search_source_builder.getValue().toString())
                  .setTimeout(TimeValue.timeValueMillis(
                          tsdb.getConfig().getLong(QUERY_TIMEOUT_KEY)));
          multi_search.add(request_builder);
        if (context != null) {
          if (excludes != null && excludes.length > 0) {
            request_builder.setFetchSource(null, excludes);
          }
        }
        if (LOG.isTraceEnabled()) {
          LOG.trace("[" + clusters.get(idx) + "] Sending ES Query: "
              + query.toString());
        }
        if (context != null && context.query().isTraceEnabled()) {
          context.queryContext().logTrace("[" + clusters.get(idx)
            + "] Sending ES Query: " + query.toString()
            + " to cluster: " + clusters.get(i));
        }

      }
        // start the timer first.
        timeouts.add(tsdb.getQueryTimer().newTimeout(new QueryTimer(),
            tsdb.getConfig().getLong(QUERY_TIMEOUT_KEY),
            TimeUnit.MILLISECONDS));

        multi_search.execute()
                .addListener(new FutureCB());
      } catch (Exception e) {
        LOG.error("Failed to execute query: " + query, e);
        deferred.callback(e);
        break;
      }
    }

    return deferred;
  }

  /**
   * Package private helper for clients to register their configs.
   * @param tsdb A non-null TSDB.
   */
  static void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(HOSTS_KEY)) {
      tsdb.getConfig().register(HOSTS_KEY, null, false, 
          "A colon separated ElasticSearch cluster address and port. E.g. "
          + "'https://es-site1:9300'. Comma separated list for multi-site "
          + "clients.");
    }
    if (!tsdb.getConfig().hasProperty(CLUSTERS_KEY)) {
      tsdb.getConfig().register(CLUSTERS_KEY, null, false, 
          "A comma separted list of cluster names. This must be the same "
          + "length as the '" + HOSTS_KEY + "'");
    }
    if (!tsdb.getConfig().hasProperty(PING_TIMEOUT_KEY)) {
      tsdb.getConfig().register(PING_TIMEOUT_KEY, PING_TIMEOUT_DEFAULT,
          false, "A timeout interval for a machine to fail pings before "
              + "taking it out of the query pipeline.");
    }
    if (!tsdb.getConfig().hasProperty(SNIFF_KEY)) {
      tsdb.getConfig().register(SNIFF_KEY, true,
          false, "TODO ??");
    }
    if (!tsdb.getConfig().hasProperty(QUERY_TIMEOUT_KEY)) {
      tsdb.getConfig().register(QUERY_TIMEOUT_KEY, QUERY_TIMEOUT_DEFAULT,
          true, "The number of milliseconds to wait on a query execution.");
    }
    if (!tsdb.getConfig().hasProperty(EXCLUDES_KEY)) {
      tsdb.getConfig().register(EXCLUDES_KEY, EXCLUDES_DEFAULT,
          false, "A comma separated list of fields to exclude to save "
              + "on serdes.");
    }
    if (!tsdb.getConfig().hasProperty(FALLBACK_ON_EX_KEY)) {
      tsdb.getConfig().register(FALLBACK_ON_EX_KEY, true,
          true, "Whether or not to fall back to scans when the meta "
              + "query returns an exception.");
    }
    if (!tsdb.getConfig().hasProperty(FALLBACK_ON_NO_DATA_KEY)) {
      tsdb.getConfig().register(FALLBACK_ON_NO_DATA_KEY, false,
          true, "Whether or not to fall back to scans when the query "
              + "was empty.");
    }
  }
}
