// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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

import org.hbase.async.HBaseClient;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xDataStore;
import net.opentsdb.uid.UniqueIdStore;

/**
 * TODO - complete.
 * 
 * @since 3.0
 */
public class Tsdb1xHBaseDataStore implements Tsdb1xDataStore {

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
  
  public static final String MULTI_GET_CONCURRENT_KEY = "tsd.query.multiget.concurrent";
  public static final String MULTI_GET_BATCH_KEY = "tsd.query.multiget.batch_size";
  public static final String EXPANSION_LIMIT_KEY = 
      "tsd.query.filter.expansion_limit";
  public static final String ROLLUP_USAGE_KEY = 
      "tsd.query.rollups.default_usage";
  public static final String SKIP_NSUN_TAGK_KEY = "tsd.query.skip_unresolved_tagks";
  public static final String SKIP_NSUN_TAGV_KEY = "tsd.query.skip_unresolved_tagvs";
  public static final String SKIP_NSUI_KEY = "tsd.query.skip_unresolved_ids";
  public static final String ALLOW_DELETE_KEY = "tsd.query.allow_delete";
  public static final String DELETE_KEY = "tsd.query.delete";
  public static final String PRE_AGG_KEY = "tsd.query.pre_agg";
  public static final String FUZZY_FILTER_KEY = "tsd.query.enable_fuzzy_filter";
  public static final String ROWS_PER_SCAN_KEY = "tsd.query.rows_per_scan";
  public static final String MAX_MG_CARDINALITY_KEY = "tsd.query.multiget.max_cardinality";
  
  public static final byte[] DATA_FAMILY = 
      "t".getBytes(Const.ISO_8859_CHARSET);
  
  private final TSDB tsdb;
  private final String id;
  
  /** The AsyncHBase client. */
  private HBaseClient client;
  
  private Schema schema;
  
  private final Tsdb1xUniqueIdStore uid_store;
  
  /** Name of the table in which timeseries are stored.  */
  private final byte[] data_table;
  
  /** Name of the table in which UID information is stored. */
  private final byte[] uid_table;
  
  /** Name of the table where tree data is stored. */
  private final byte[] tree_table;
  
  /** Name of the table where meta data is stored. */
  private final byte[] meta_table;
  
  public Tsdb1xHBaseDataStore(final Tsdb1xHBaseFactory factory,
                              final String id,
                              final Schema schema) {
    this.tsdb = factory.tsdb();
    this.id = id;
    this.schema = schema;
    
    // TODO - flatten the config and pass it down to the client lib.
    final org.hbase.async.Config async_config = new org.hbase.async.Config();
    
    // We'll sync on the config object to avoid race conditions if 
    // multiple instances of this client are being loaded.
    final Configuration config = tsdb.getConfig();
    synchronized(config) {
      if (!config.hasProperty(getConfigKey(DATA_TABLE_KEY))) {
        System.out.println("REGISTERED: " + getConfigKey(DATA_TABLE_KEY));
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
    
//    Map<String, ConfigurationEntryWrapper> cfg = tsdb.getConfig().getView().getEntries();
//    for (final Entry<String, ConfigurationEntryWrapper> entry : cfg.entrySet()) {
//      if (entry.getValue().getOverrides() != null && 
//          !entry.getValue().getOverrides().isEmpty()) {
//        final Object obj = entry.getValue().getOverrides().get(
//            entry.getValue().getOverrides().size() - 1).getValue();
//        async_config.overrideConfig(entry.getKey(), obj == null ? null : obj.toString());
//        System.out.println("KEY: " + entry.getKey() + "  V: " + (obj == null ? null : obj.toString()));
//      } else {
//        final Object obj = entry.getValue().getSchema().getDefaultValue();
//        async_config.overrideConfig(entry.getKey(),
//            obj == null ? null : obj.toString());
//        System.out.println("KEY: " + entry.getKey() + "  V: " + (obj == null ? null : obj.toString()));
//      }
//    }
    // TODO - shared client!
    client = new HBaseClient(async_config);
    
    // TODO - probably a better way. We may want to make the UniqueIdStore
    // it's own self-contained storage system.
    uid_store = new Tsdb1xUniqueIdStore(this);
    tsdb.getRegistry().registerSharedObject(Strings.isNullOrEmpty(id) ? 
        "default_uidstore" : id + "_uidstore", uid_store);
  }
  
  @Override
  public String id() {
    return "AsyncHBaseDataStore";
  }
  
  @Override
  public Deferred<Object> write(final TimeSeriesStringId id, 
                                final TimeSeriesValue<?> value, 
                                final Span span) {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public QueryNode newNode(final QueryPipelineContext context,
                           final String id,
                           final QueryNodeConfig config) {
    return new Tsdb1xQueryNode(this, context, id, (QuerySourceConfig) config);
  }
  
  public Deferred<Object> shutdown() {
    // TODO - implement
    return Deferred.fromResult(null);
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

  
}
