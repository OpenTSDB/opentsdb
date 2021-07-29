// This file is part of OpenTSDB.
// Copyright (C) 2017-2019  The OpenTSDB Authors.
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
package net.opentsdb.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import net.opentsdb.data.LowLevelMetricData;
import net.opentsdb.data.TimeSeriesDatumStringId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.storage.schemas.tsdb1x.BaseTsdb1xDataStore;
import net.opentsdb.storage.schemas.tsdb1x.Codec;
import org.hbase.async.AppendRequest;
import org.hbase.async.CallQueueTooBigException;
import org.hbase.async.ClientStats;
import org.hbase.async.HBaseClient;
import org.hbase.async.PleaseThrottleException;
import org.hbase.async.PutRequest;
import org.hbase.async.RecoverableException;
import org.hbase.async.RegionClientStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import net.opentsdb.auth.AuthState;
import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.LowLevelTimeSeriesData;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xDataStore;
import net.opentsdb.uid.IdOrError;
import net.opentsdb.uid.UniqueIdStore;
import net.opentsdb.utils.Pair;

/**
 * TODO - complete.
 * 
 * @since 3.0
 */
public class Tsdb1xHBaseDataStore extends BaseTsdb1xDataStore implements TimerTask {
  private static final Logger LOG = LoggerFactory.getLogger(
      Tsdb1xHBaseDataStore.class);
  
  /** Config keys */
  public static final String CONFIG_PREFIX = "tsd.storage.";
  public static final String DATA_TABLE_KEY = "data_table";
  public static final String UID_TABLE_KEY = "uid_table";
  public static final String TREE_TABLE_KEY = "tree_table";
  public static final String META_TABLE_KEY = "meta_table";
  
  /** AsyncHBase config. */
  public static final String ZNODE_PARENT_KEY = "zookeeper.znode.parent";
  public static final String ZK_QUORUM_KEY = "zookeeper.quorum";
  public static final String AUTH_ENABLED_KEY = "auth.enable";
  public static final String KB_PRINCIPAL_KEY = "kerberos.principal";
  public static final String KB_ENABLED_KEY = "kerberos.enable";
  public static final String SASL_CLIENT_KEY = "sasl.clientconfig";
  public static final String META_SPLIT_KEY = "meta.split";
  public static final String MTLS_REGISTRY_KEY = "mtls.token.registry";
  public static final String MTLS_CERT_KEY = "mtls.certificate";
  public static final String MTLS_KEY_KEY = "mtls.key";
  public static final String MTLS_CA_KEY = "mtls.ca";
  public static final String MTLS_REFRESH_KEY = "mtls.refresh.interva";
  public static final String MTLS_TOKEN_RENEWAL_KEY = "mtls.token.renewalPeriod";
  
  public static final String MULTI_GET_CONCURRENT_KEY = "tsd.query.multiget.concurrent";
  public static final String MULTI_GET_BATCH_KEY = "tsd.query.multiget.batch_size";
  public static final String EXPANSION_LIMIT_KEY = 
      "tsd.query.filter.expansion_limit";
  public static final String ROLLUP_USAGE_KEY = 
      "tsd.query.rollups.default_usage";
  public static final String SKIP_NSUN_TAGK_KEY = "tsd.query.skip_unresolved_tagks";
  public static final String SKIP_NSUN_TAGV_KEY = "tsd.query.skip_unresolved_tagvs";
  public static final String SKIP_NSUN_METRIC_KEY = "tsd.query.skip_unresolved_metrics";
  public static final String SKIP_NSUI_KEY = "tsd.query.skip_unresolved_ids";
  public static final String ALLOW_DELETE_KEY = "tsd.query.allow_delete";
  public static final String DELETE_KEY = "tsd.query.delete";
  public static final String PRE_AGG_KEY = "tsd.query.pre_agg";
  public static final String FUZZY_FILTER_KEY = "tsd.query.enable_fuzzy_filter";
  public static final String ROWS_PER_SCAN_KEY = "tsd.query.rows_per_scan";
  public static final String MAX_MG_CARDINALITY_KEY = "tsd.query.multiget.max_cardinality";
  public static final String ENABLE_APPENDS_KEY = "tsd.storage.enable_appends";
  public static final String ENABLE_COPROC_APPENDS_KEY = "tsd.storage.enable_appends_coproc";
  public static final String ENABLE_PUSH_KEY = "tsd.storage.enable_push";
  public static final String ENABLE_DP_TIMESTAMP = "tsd.storage.use_otsdb_timestamp";
  
  public static final byte[] DATA_FAMILY = 
      "t".getBytes(Const.ISO_8859_CHARSET);

  /** The AsyncHBase client. */
  private HBaseClient client;

  /** Name of the table in which timeseries are stored.  */
  private final byte[] data_table;
  
  /** Name of the table in which UID information is stored. */
  private final byte[] uid_table;
  
  /** Name of the table where tree data is stored. */
  private final byte[] tree_table;
  
  /** Name of the table where meta data is stored. */
  private final byte[] meta_table;

  private ClientStatsWrapper client_stats;
  private Map<String, RegionClientStatsWrapper> region_client_stats;

  public Tsdb1xHBaseDataStore(final Tsdb1xHBaseFactory factory,
                              final String id,
                              final Schema schema) {
    super(id, factory.tsdb(), schema);
    
    client_stats = new ClientStatsWrapper();
    region_client_stats = Maps.newHashMap();
    
    // TODO - flatten the config and pass it down to the client lib.
    final org.hbase.async.Config async_config = new org.hbase.async.Config();
    
    // We'll sync on the config object to avoid race conditions if 
    // multiple instances of this client are being loaded.
    final Configuration config = tsdb.getConfig();
    synchronized(config) {
      if (!config.hasProperty(getConfigKey(DATA_TABLE_KEY))) {
        config.register(getConfigKey(DATA_TABLE_KEY), "tsdb", false, 
            "The name of the raw data table for OpenTSDB.");
      }
      data_table = config.getString(getConfigKey(DATA_TABLE_KEY))
          .getBytes(Const.ISO_8859_CHARSET);
      if (!config.hasProperty(getConfigKey(UID_TABLE_KEY))) {
        config.register(getConfigKey(UID_TABLE_KEY), "tsdb-uid", false, 
            "The name of the UID mapping table for OpenTSDB.");
      }
      uid_table = config.getString(getConfigKey(UID_TABLE_KEY))
          .getBytes(Const.ISO_8859_CHARSET);
      if (!config.hasProperty(getConfigKey(TREE_TABLE_KEY))) {
        config.register(getConfigKey(TREE_TABLE_KEY), "tsdb-tree", false, 
            "The name of the Tree table for OpenTSDB.");
      }
      tree_table = config.getString(getConfigKey(TREE_TABLE_KEY))
          .getBytes(Const.ISO_8859_CHARSET);
      if (!config.hasProperty(getConfigKey(META_TABLE_KEY))) {
        config.register(getConfigKey(META_TABLE_KEY), "tsdb-meta", false, 
            "The name of the Meta data table for OpenTSDB.");
      }
      meta_table = config.getString(getConfigKey(META_TABLE_KEY))
          .getBytes(Const.ISO_8859_CHARSET);
      
      // asynchbase flags
      if (!config.hasProperty(getConfigKey(ZK_QUORUM_KEY))) {
        config.register(getConfigKey(ZK_QUORUM_KEY), "localhost:2181", false, 
            "The comma separated list of Zookeeper servers and ports.");
      }
      if (!config.hasProperty(getConfigKey(ZNODE_PARENT_KEY))) {
        config.register(getConfigKey(ZNODE_PARENT_KEY), "/hbase", false, 
            "The base znode for HBase.");
      }
      if (!config.hasProperty(getConfigKey(AUTH_ENABLED_KEY))) {
        config.register(getConfigKey(AUTH_ENABLED_KEY), "false", false, 
            "Whether or not authentication is required to connect to "
            + "HBase region servers.");
      }
      if (!config.hasProperty(getConfigKey(KB_PRINCIPAL_KEY))) {
        config.register(getConfigKey(KB_PRINCIPAL_KEY), null, false, 
            "The principal template for kerberos authentication.");
      }
      if (!config.hasProperty(getConfigKey(KB_ENABLED_KEY))) {
        config.register(getConfigKey(KB_ENABLED_KEY), "false", false, 
            "Whether or not kerberos is enabled for authentication.");
      }
      if (!config.hasProperty(getConfigKey(SASL_CLIENT_KEY))) {
        config.register(getConfigKey(SASL_CLIENT_KEY), "Client", false, 
            "The SASL entry for the client in the JAAS config.");
      }
      if (!config.hasProperty(getConfigKey(META_SPLIT_KEY))) {
        config.register(getConfigKey(META_SPLIT_KEY), "false", false, 
            "Whether or not the meta table is split.");
      }
      if (!config.hasProperty(getConfigKey(MTLS_REGISTRY_KEY))) {
        config.register(getConfigKey(MTLS_REGISTRY_KEY), null, false, 
            "TODO");
      }
      if (!config.hasProperty(getConfigKey(MTLS_CERT_KEY))) {
        config.register(getConfigKey(MTLS_CERT_KEY), null, false, 
            "TODO");
      }
      if (!config.hasProperty(getConfigKey(MTLS_KEY_KEY))) {
        config.register(getConfigKey(MTLS_KEY_KEY), null, false, 
            "TODO");
      }
      if (!config.hasProperty(getConfigKey(MTLS_CA_KEY))) {
        config.register(getConfigKey(MTLS_CA_KEY), null, false, 
            "TODO");
      }
      if (!config.hasProperty(getConfigKey(MTLS_REFRESH_KEY))) {
        config.register(getConfigKey(MTLS_REFRESH_KEY), 300000, false, 
            "TODO");
      }
      if (!config.hasProperty(getConfigKey(MTLS_TOKEN_RENEWAL_KEY))) {
        config.register(getConfigKey(MTLS_TOKEN_RENEWAL_KEY), 3600, false, 
            "TODO");
      }
      
      // more bits
      if (!config.hasProperty(EXPANSION_LIMIT_KEY)) {
        config.register(EXPANSION_LIMIT_KEY, 4096, true,
            "The maximum number of UIDs to expand in a literal filter "
            + "for HBase scanners.");
      }
      if (!config.hasProperty(ROLLUP_USAGE_KEY)) {
        config.register(ROLLUP_USAGE_KEY, "rollup_fallback", true,
            "The default fallback operation for queries involving rollup tables.");
      }
      if (!config.hasProperty(SKIP_NSUN_TAGK_KEY)) {
        config.register(SKIP_NSUN_TAGK_KEY, "false", true,
            "Whether or not to simply drop tag keys (names) from query filters "
            + "that have not been assigned UIDs and try to fetch data anyway.");
      }
      if (!config.hasProperty(SKIP_NSUN_TAGV_KEY)) {
        config.register(SKIP_NSUN_TAGV_KEY, "false", true,
            "Whether or not to simply drop tag values from query filters "
            + "that have not been assigned UIDs and try to fetch data anyway.");
      }
      if (!config.hasProperty(SKIP_NSUN_METRIC_KEY)) {
        config.register(SKIP_NSUN_METRIC_KEY, "false", true,
                "Whether or not to throw an error if a metric fails to " +
              "resolve to a UID (false) or return an empty response (true).");
      }
      if (!config.hasProperty(SKIP_NSUI_KEY)) {
        config.register(SKIP_NSUI_KEY, "false", true,
            "Whether or not to ignore data from storage that did not "
            + "resolve from a UID to a string. If not ignored, "
            + "exceptions are thrown when the data is read.");
      }
      if (!config.hasProperty(ALLOW_DELETE_KEY)) {
        config.register(ALLOW_DELETE_KEY, "false", true,
            "TODO");
      }
      if (!config.hasProperty(DELETE_KEY)) {
        config.register(DELETE_KEY, "false", true,
            "TODO");
      }
      if (!config.hasProperty(PRE_AGG_KEY)) {
        config.register(PRE_AGG_KEY, "false", true,
            "TODO");
      }
      if (!config.hasProperty(FUZZY_FILTER_KEY)) {
        config.register(FUZZY_FILTER_KEY, "true", true,
            "TODO");
      }
      if (!config.hasProperty(ROWS_PER_SCAN_KEY)) {
        config.register(ROWS_PER_SCAN_KEY, "128", true,
            "TODO");
      }
      
      if (!config.hasProperty(MULTI_GET_CONCURRENT_KEY)) {
        config.register(MULTI_GET_CONCURRENT_KEY, "20", true,
            "TODO");
      }
      if (!config.hasProperty(MULTI_GET_BATCH_KEY)) {
        config.register(MULTI_GET_BATCH_KEY, "1024", true,
            "TODO");
      }
      if (!config.hasProperty(MAX_MG_CARDINALITY_KEY)) {
        config.register(MAX_MG_CARDINALITY_KEY, "128", true,
            "TODO");
      }
      if (!config.hasProperty(ENABLE_APPENDS_KEY)) {
        config.register(ENABLE_APPENDS_KEY, false, false,
            "TODO");
      }
      if (!config.hasProperty(ENABLE_COPROC_APPENDS_KEY)) {
        config.register(ENABLE_COPROC_APPENDS_KEY, false, false,
            "TODO");
      }
      if (!config.hasProperty(ENABLE_PUSH_KEY)) {
        config.register(ENABLE_PUSH_KEY, false, false,
            "TODO");
      }
      if (!config.hasProperty(ENABLE_DP_TIMESTAMP)) {
        config.register(ENABLE_DP_TIMESTAMP, true, false,
            "TODO");
      }
    }
    
    /** Copy all configs, then we'll override with node specific entries. */
    final Map<String, String> flat = config.asRawUnsecuredMap();
    for (final Entry<String, String> entry : flat.entrySet()) {
      async_config.overrideConfig(entry.getKey(), entry.getValue());
    }
    
    async_config.overrideConfig("hbase.zookeeper.quorum", 
        config.getString(getConfigKey(ZK_QUORUM_KEY)));
    async_config.overrideConfig("hbase.zookeeper.znode.parent", 
        config.getString(getConfigKey(ZNODE_PARENT_KEY)));
    async_config.overrideConfig("hbase.security.auth.enable", 
        config.getString(getConfigKey(AUTH_ENABLED_KEY)));
    async_config.overrideConfig("hbase.kerberos.regionserver.principal", 
        config.getString(getConfigKey(KB_PRINCIPAL_KEY)));
    if (config.getBoolean(getConfigKey(KB_ENABLED_KEY))) {
      async_config.overrideConfig("hbase.security.authentication", 
          "kerberos");
    }
    
    async_config.overrideConfig("hbase.sasl.clientconfig", 
        config.getString(getConfigKey(SASL_CLIENT_KEY)));
    async_config.overrideConfig("hbase.meta.split", 
        config.getString(getConfigKey(META_SPLIT_KEY)));
    if (config.getString(getConfigKey(MTLS_REGISTRY_KEY)) != null && 
        config.getString(getConfigKey(MTLS_CERT_KEY)) != null) {
      async_config.overrideConfig("hbase.client.mtls.token.registry", 
          config.getString(getConfigKey(MTLS_REGISTRY_KEY)));
      async_config.overrideConfig("hbase.client.mtls.certificate", 
          config.getString(getConfigKey(MTLS_CERT_KEY)));
      async_config.overrideConfig("hbase.client.mtls.key", 
          config.getString(getConfigKey(MTLS_KEY_KEY)));
      async_config.overrideConfig("hbase.client.mtls.ca", 
          config.getString(getConfigKey(MTLS_CA_KEY)));
      async_config.overrideConfig("hbase.client.mtls.refresh.interval", 
          config.getString(getConfigKey(MTLS_REFRESH_KEY)));
      async_config.overrideConfig("hbase.client.mtls.token.renewalPeriod", 
          config.getString(getConfigKey(MTLS_TOKEN_RENEWAL_KEY)));
      async_config.overrideConfig("hbase.security.authentication", "mtlstemp");
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("AsyncHBase Config: " + async_config.dumpConfiguration());
    }
    
    write_appends = config.getBoolean(ENABLE_APPENDS_KEY);
    encode_as_appends = config.getBoolean(ENABLE_COPROC_APPENDS_KEY);
    use_dp_timestamp = config.getBoolean(ENABLE_DP_TIMESTAMP);

    // TODO - shared client!
    client = new HBaseClient(async_config);
    
    // TODO - probably a better way. We may want to make the UniqueIdStore
    // it's own self-contained storage system.
    uid_store = new Tsdb1xUniqueIdStore(this, id);
    tsdb.getRegistry().registerSharedObject(Strings.isNullOrEmpty(id) ? 
        "default_uidstore" : id + "_uidstore", uid_store);

    // TODO - have to call them all out.
    retryExceptions = Sets.newHashSet(
            PleaseThrottleException.class,
            CallQueueTooBigException.class
    );
    // start the stats timer.
    tsdb.getMaintenanceTimer().newTimeout(this, 60, TimeUnit.SECONDS);
  }
  
  @Override
  public String id() {
    return "AsyncHBaseDataStore";
  }
  
  @Override
  public Tsdb1xHBaseQueryNode newNode(final QueryPipelineContext context,
                           final TimeSeriesDataSourceConfig config) {
    return new Tsdb1xHBaseQueryNode(this, context, config);
  }

  public Deferred<Object> shutdown() {
    return Deferred.fromResult(client.shutdown());
  }
  
  /**
   * Prepends the {@link #CONFIG_PREFIX} and the current data store ID to
   * the given suffix.
   * @param suffix A non-null and non-empty suffix.
   * @return A non-null and non-empty config string.
   */
  public String getConfigKey(final String suffix) {
    if (Strings.isNullOrEmpty(suffix)) {
      throw new IllegalArgumentException("Suffix cannot be null.");
    }
    if (Strings.isNullOrEmpty(id)) {
      return CONFIG_PREFIX + suffix;
    } else {
      return CONFIG_PREFIX + id + "." + suffix;
    }
  }
  
  /** @return The schema assigned to this store. */
  Schema schema() {
    return schema;
  }
  
  /** @return The data table. */
  byte[] dataTable() {
    return data_table;
  }
  
  /** @return The UID table. */
  byte[] uidTable() {
    return uid_table;
  }
  
  /** @return The HBase client. */
  HBaseClient client() {
    return client;
  }

  /** @return The TSDB reference. */
  TSDB tsdb() {
    return tsdb;
  }
  
  /** @return The UID store. */
  UniqueIdStore uidStore() {
    return uid_store;
  }

  String dynamicString(final String key) {
    return tsdb.getConfig().getString(key);
  }
  
  int dynamicInt(final String key) {
    return tsdb.getConfig().getInt(key);
  }
  
  boolean dynamicBoolean(final String key) {
    return tsdb.getConfig().getBoolean(key);
  }
  
  /** The metric timer task */
  public void run(final Timeout ignored) {
    try {
      final StatsCollector stats = tsdb.getStatsCollector();
      client_stats.run(client.stats(), stats);
      final Set<String> region_clients = Sets.newHashSet();
      for (final RegionClientStats rc_stats : client.regionStats()) {
        RegionClientStatsWrapper wrapper = region_client_stats.get(rc_stats.remoteEndpoint());
        if (wrapper == null) {
          wrapper = new RegionClientStatsWrapper();
          region_client_stats.put(rc_stats.remoteEndpoint(), wrapper);
        }
        wrapper.run(rc_stats, stats);
        region_clients.add(rc_stats.remoteEndpoint());
      }
      
      // now clean out stale entries.
      final Iterator<Entry<String, RegionClientStatsWrapper>> iterator = 
          region_client_stats.entrySet().iterator();
      while (iterator.hasNext()) {
        final Entry<String, RegionClientStatsWrapper> entry = iterator.next();
        if (!region_clients.contains(entry.getKey())) {
          iterator.remove();
        }
      }
      
    } catch (Throwable t) {
      LOG.error("Failed to fetch stats for HBase store: " + id, t);
    } finally {
      tsdb.getMaintenanceTimer().newTimeout(this, 60, TimeUnit.SECONDS);
    }
  }

  @Override
  protected Deferred<WriteStatus> write(final byte[] key,
                                        final byte[] qualifier,
                                        final byte[] value,
                                        final TimeStamp timestamp,
                                        final Span span) {
    return write(data_table, key, qualifier, value, timestamp, span);
  }

  @Override
  protected Deferred<WriteStatus> write(final byte[] table,
                                        final byte[] key,
                                        final byte[] qualifier,
                                        final byte[] value,
                                        final TimeStamp timestamp,
                                        final Span span) {
    return client.put(new PutRequest(table,
            key,
            DATA_FAMILY,
            qualifier,
            value,
            use_dp_timestamp ? timestamp.msEpoch() : Long.MAX_VALUE))
            .addCallbacks(new SuccessCB(span), new WriteErrorCB(null, span));
  }

  @Override
  protected Deferred<WriteStatus> writeAppend(final byte[] key,
                                              final byte[] qualifier,
                                              final byte[] value,
                                              final TimeStamp timestamp,
                                              final Span span) {
    return writeAppend(data_table, key, qualifier, value, timestamp, span);
  }

  @Override
  protected Deferred<WriteStatus> writeAppend(final byte[] table,
                                              final byte[] key,
                                              final byte[] qualifier,
                                              final byte[] value,
                                              final TimeStamp timestamp,
                                              final Span span) {
    return client.append(new AppendRequest(table,
            key,
            DATA_FAMILY,
            qualifier,
            value))
            .addCallbacks(new SuccessCB(span), new WriteErrorCB(null, span));
  }

  class ClientStatsWrapper {
    private long num_connections_created;
    private long root_lookups;
    private long meta_lookups_with_permit;
    private long meta_lookups_wo_permit;
    private long num_flushes;
    private long num_nsres;
    private long num_nsre_rpcs;
    private long num_multi_rpcs;
    private long num_gets;
    private long num_scanners_opened;
    private long num_scans;
    private long num_puts;
    private long num_appends;
    private long num_row_locks;
    private long num_deletes;
    private long num_atomic_increments;
    private long bytes_read;
    private long bytes_written;
    private long rpcs_aged_out;
    private long idle_connections_closed;
    private Map<String, Long> exception_counters = Maps.newHashMap();
    
    void run(final ClientStats client_stats, final StatsCollector stats) {
      long delta = client_stats.connectionsCreated() - num_connections_created;
      if (delta > 0) {
        stats.incrementCounter("hbase.connections.created", delta, "id", id);
        num_connections_created = client_stats.connectionsCreated();
      }
      
      delta = client_stats.idleConnectionsClosed() - idle_connections_closed;
      if (delta > 0) {
        stats.incrementCounter("hbase.connections.closed", delta, 
            "id", id, "reason", "idle");
        idle_connections_closed = client_stats.idleConnectionsClosed();
      }

      delta = client_stats.rootLookups() - root_lookups;
      if (delta > 0) {
        stats.incrementCounter("hbase.rootLookups", delta, "id", id);
        root_lookups = client_stats.rootLookups();
      }
      
      delta = client_stats.uncontendedMetaLookups() - meta_lookups_with_permit;
      if (delta > 0) {
        stats.incrementCounter("hbase.meta.lookupsWithPermit", delta, "id", id);
        meta_lookups_with_permit = client_stats.uncontendedMetaLookups();
      }
      
      delta = client_stats.contendedMetaLookups() - meta_lookups_wo_permit;
      if (delta > 0) {
        stats.incrementCounter("hbase.meta.lookupsWOPermit", delta, "id", id);
        meta_lookups_wo_permit = client_stats.contendedMetaLookups();
      }
      
      delta = client_stats.flushes() - num_flushes;
      if (delta > 0) {
        stats.incrementCounter("hbase.flushes", delta, "id", id);
        num_flushes = client_stats.flushes();
      }
      
      delta = client_stats.noSuchRegionExceptions() - num_nsres;
      if (delta > 0) {
        stats.incrementCounter("hbase.nsre.totalHandled", delta, "id", id);
        num_nsres = client_stats.noSuchRegionExceptions();
      }
      
      delta = client_stats.numRpcDelayedDueToNSRE() - num_nsre_rpcs;
      if (delta > 0) {
        stats.incrementCounter("hbase.nsre.rpcsHandled", delta, "id", id);
        num_nsre_rpcs = client_stats.numRpcDelayedDueToNSRE();
      }
      
      delta = client_stats.numBatchedRpcSent() - num_multi_rpcs;
      if (delta > 0) {
        stats.incrementCounter("hbase.rpcs.batched", delta, "id", id);
        num_multi_rpcs = client_stats.numBatchedRpcSent();
      }
      
      delta = client_stats.gets() - num_gets;
      if (delta > 0) {
        stats.incrementCounter("hbase.rpcs", delta, "id", id, "type", "get");
        num_gets = client_stats.gets();
      }
      
      delta = client_stats.scans() - num_scans;
      if (delta > 0) {
        stats.incrementCounter("hbase.rpcs", delta, "id", id, "type", "scans");
        num_scans = client_stats.scans();
      }
      
      delta = client_stats.puts() - num_puts;
      if (delta > 0) {
        stats.incrementCounter("hbase.rpcs", delta, "id", id, "type", "puts");
        num_puts = client_stats.puts();
      }
      
      delta = client_stats.appends() - num_appends;
      if (delta > 0) {
        stats.incrementCounter("hbase.rpcs", delta, "id", id, "type", "appends");
        num_appends = client_stats.appends();
      }
      
      delta = client_stats.rowLocks() - num_row_locks;
      if (delta > 0) {
        stats.incrementCounter("hbase.rpcs", delta, "id", id, "type", "locks");
        num_row_locks = client_stats.rowLocks();
      }
      
      delta = client_stats.deletes() - num_deletes;
      if (delta > 0) {
        stats.incrementCounter("hbase.rpcs", delta, "id", id, "type", "deletes");
        num_deletes = client_stats.deletes();
      }
      
      delta = client_stats.atomicIncrements() - num_atomic_increments;
      if (delta > 0) {
        stats.incrementCounter("hbase.rpcs", delta, "id", id, "type", 
            "atomicIncrements");
        num_atomic_increments = client_stats.atomicIncrements();
      }
      
      delta = client_stats.scannersOpened() - num_scanners_opened;
      if (delta > 0) {
        stats.incrementCounter("hbase.scanners.opened", delta, "id", id);
        num_scanners_opened = client_stats.scannersOpened();
      }
      
      if (client_stats.extendedStats() != null) {
        // TODO - bytes are oddballs in that they are, currently, aggregating the
        // counters for each instantiated region client so the delta can be
        // negative. Fix this in asynchbase client.
        delta = client_stats.extendedStats().bytesRead() - bytes_read;
        if (delta > 0) {
          stats.incrementCounter("hbase.bytes.read", delta, "id", id);
        }
        bytes_read = client_stats.extendedStats().bytesRead();
        
        delta = client_stats.extendedStats().bytesWritten() - bytes_written;
        if (delta > 0) {
          stats.incrementCounter("hbase.bytes.written", delta, "id", id);
        }
        bytes_written = client_stats.extendedStats().bytesWritten();
        
        delta = client_stats.extendedStats().rpcsAgedOut() - rpcs_aged_out;
        if (delta > 0) {
          stats.incrementCounter("hbase.scanners.opened", delta, "id", id);
          rpcs_aged_out = client_stats.extendedStats().rpcsAgedOut();
        }
      }
      
      stats.setGauge("hbase.rpcs.inflight", client_stats.inflightRPCs(), "id", id);
      stats.setGauge("hbase.rpcs.pending", client_stats.pendingRPCs(), "id", id);
      stats.setGauge("hbase.rpcs.pendingBatched", client_stats.pendingBatchedRPCs(), 
          "id", id);
      stats.setGauge("hbase.regionClients.open", client_stats.regionClients(), "id", id);
      stats.setGauge("hbase.regionClients.dead", client_stats.deadRegionClients(), "id", id);
      
      if (client_stats.extendedStats() != null) {
        for (final Entry<String, Long> entry : 
            client_stats.extendedStats().exceptionCounters().entrySet()) {
          Long extant = exception_counters.get(entry.getKey());
          if (extant == null) {
            exception_counters.put(entry.getKey(), (long) entry.getValue());
          } else {
            delta = entry.getValue() - extant;
            if (delta > 0) {
              stats.incrementCounter("hbase.exceptions", delta, "id", id, 
                  "type", entry.getKey());
              exception_counters.put(entry.getKey(), (long) entry.getValue());
            }
          }
        }
      }
    }
  }
  
  class RegionClientStatsWrapper {
    private long rpcs_sent;
    private long rpcs_timedout;
    private long writes_blocked;
    private int writes_blocked_by_rate_limiter;
    private long rpc_response_timedout;
    private long rpc_response_unknown;
    private long inflight_breached;
    private Map<String, Long> exception_counters = Maps.newHashMap();
    private int decode_called;
    private int replays;
    private int nsre_exceptions;
    private int probes_sent;
    private int probes_succeeded;
    private int probes_nsred;
    private int probes_with_exception;
    private int probes_timedout;
    private int cqtbes;
    private long bytes_read;
    private long bytes_written;
    
    void run(final RegionClientStats region_stats, final StatsCollector stats) {
      String region_server = region_stats.remoteEndpoint(); // TODO
      
      long delta = region_stats.rpcsSent() - rpcs_sent;
      if (delta > 0) {
        stats.incrementCounter("hbase.regionServer.rpcs.sent", delta, "id", id,
            "regionServer", region_server);
        rpcs_sent = region_stats.rpcsSent();
      }
      
      delta = region_stats.rpcsTimedout() - rpcs_timedout;
      if (delta > 0) {
        stats.incrementCounter("hbase.regionServer.rpcs.timedout", delta, "id", id,
            "regionServer", region_server);
        rpcs_timedout = region_stats.rpcsTimedout();
      }
      
      delta = region_stats.writesBlocked() - writes_blocked;
      if (delta > 0) {
        stats.incrementCounter("hbase.regionServer.rpcs.writeBlocked", delta, "id", id,
            "regionServer", region_server);
        writes_blocked = region_stats.writesBlocked();
      }
      
      delta = region_stats.writesBlockedByRateLimiter() - writes_blocked_by_rate_limiter;
      if (delta > 0) {
        stats.incrementCounter("hbase.regionServer.rpcs.writeBlockedDueToRateLimiter", 
            delta, "id", id, "regionServer", region_server);
        writes_blocked_by_rate_limiter = region_stats.writesBlockedByRateLimiter();
      }
      
      delta = region_stats.rpcResponsesTimedout() - rpc_response_timedout;
      if (delta > 0) {
        stats.incrementCounter("hbase.regionServer.rpcs.responseAfterTimeout", 
            delta, "id", id, "regionServer", region_server);
        rpc_response_timedout = region_stats.rpcResponsesTimedout();
      }
      
      delta = region_stats.rpcResponsesUnknown() - rpc_response_unknown;
      if (delta > 0) {
        stats.incrementCounter("hbase.regionServer.rpcs.responseUnknown", 
            delta, "id", id, "regionServer", region_server);
        rpc_response_unknown = region_stats.rpcResponsesUnknown();
      }
      
      delta = region_stats.inflightBreached() - inflight_breached;
      if (delta > 0) {
        stats.incrementCounter("hbase.regionServer.rpcs.inflightBreached", 
            delta, "id", id, "regionServer", region_server);
        inflight_breached = region_stats.inflightBreached();
      }
      
      if (region_stats.extendedStats() != null) {
        delta = region_stats.extendedStats().decodeCalled() - decode_called;
        if (delta > 0) {
          stats.incrementCounter("hbase.regionServer.decodeCalled", 
              delta, "id", id, "regionServer", region_server);
          decode_called = region_stats.extendedStats().decodeCalled();
        }
        
        delta = region_stats.extendedStats().replays() - replays;
        if (delta > 0) {
          stats.incrementCounter("hbase.regionServer.replays", 
              delta, "id", id, "regionServer", region_server);
          replays = region_stats.extendedStats().replays();
        }
        
        delta = region_stats.extendedStats().NSREExceptions() - nsre_exceptions;
        if (delta > 0) {
          stats.incrementCounter("hbase.regionServer.nsreExceptions", 
              delta, "id", id, "regionServer", region_server);
          nsre_exceptions = region_stats.extendedStats().NSREExceptions();
        }
        
        delta = region_stats.extendedStats().probesSent() - probes_sent;
        if (delta > 0) {
          stats.incrementCounter("hbase.regionServer.probesSent", 
              delta, "id", id, "regionServer", region_server);
          probes_sent = region_stats.extendedStats().probesSent();
        }
        
        delta = region_stats.extendedStats().probesSucceeded() - probes_succeeded;
        if (delta > 0) {
          stats.incrementCounter("hbase.regionServer.probesSucceeded", 
              delta, "id", id, "regionServer", region_server);
          probes_succeeded = region_stats.extendedStats().probesSucceeded();
        }
        
        delta = region_stats.extendedStats().probesNsred() - probes_nsred;
        if (delta > 0) {
          stats.incrementCounter("hbase.regionServer.probesNSREd", 
              delta, "id", id, "regionServer", region_server);
          probes_nsred = region_stats.extendedStats().probesNsred();
        }
        
        delta = region_stats.extendedStats().probesWithException() - probes_with_exception;
        if (delta > 0) {
          stats.incrementCounter("hbase.regionServer.probesWithException", 
              delta, "id", id, "regionServer", region_server);
          probes_with_exception = region_stats.extendedStats().probesWithException();
        }
        
        delta = region_stats.extendedStats().probesTimedout() - probes_timedout;
        if (delta > 0) {
          stats.incrementCounter("hbase.regionServer.probesTimedout", 
              delta, "id", id, "regionServer", region_server);
          probes_timedout = region_stats.extendedStats().probesTimedout();
        }
        
        delta = region_stats.extendedStats().CQTBEs() - cqtbes;
        if (delta > 0) {
          stats.incrementCounter("hbase.regionServer.CQTBEs", 
              delta, "id", id, "regionServer", region_server);
          cqtbes = region_stats.extendedStats().CQTBEs();
        }
        
        delta = region_stats.extendedStats().bytesRead() - bytes_read;
        if (delta > 0) {
          stats.incrementCounter("hbase.regionServer.bytes.read", 
              delta, "id", id, "regionServer", region_server);
          bytes_read = region_stats.extendedStats().bytesRead();
        }
        
        delta = region_stats.extendedStats().bytesWritten() - bytes_written;
        if (delta > 0) {
          stats.incrementCounter("hbase.regionServer.bytes.written", 
              delta, "id", id, "regionServer", region_server);
          bytes_written = region_stats.extendedStats().bytesWritten();
        }
      }
      
      stats.setGauge("hbase.regionServer.rpcs.inflight", region_stats.inflightRPCs(),
          "id", id, "regionServer", region_server);
      stats.setGauge("hbase.regionServer.rpcs.pending", region_stats.pendingRPCs(),
          "id", id, "regionServer", region_server);
      stats.setGauge("hbase.regionServer.rpcs.pendingBatched", 
          region_stats.pendingBatchedRPCs(), "id", id, "regionServer", region_server);
      stats.setGauge("hbase.regionServer.rpcId", region_stats.rpcID(),
          "id", id, "regionServer", region_server);
      stats.setGauge("hbase.regionServer.dead", region_stats.isDead() ? 1 : 0,
          "id", id, "regionServer", region_server);
      stats.setGauge("hbase.regionServer.rateLimit", 
          !Double.isFinite(region_stats.rateLimit()) ? -1 : region_stats.rateLimit(),
          "id", id, "regionServer", region_server);
      
      if (region_stats.extendedStats() != null) {
        for (final Entry<String, Long> entry : 
            region_stats.extendedStats().exceptionCounters().entrySet()) {
          Long extant = exception_counters.get(entry.getKey());
          if (extant == null) {
            exception_counters.put(entry.getKey(), (long) entry.getValue());
          } else {
            delta = entry.getValue() - extant;
            if (delta > 0) {
              stats.incrementCounter("hbase.regionServer.exceptions", delta, "id", id, 
                  "regionServer", region_server, "type", entry.getKey());
              exception_counters.put(entry.getKey(), (long) entry.getValue());
          }
        }
      }
    }
    }
  }
}
