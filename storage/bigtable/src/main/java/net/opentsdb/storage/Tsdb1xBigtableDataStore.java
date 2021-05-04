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
package net.opentsdb.storage;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.bigtable.v2.ReadModifyWriteRule;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.UnsafeByteOperations;
import com.stumbleupon.async.Deferred; 

import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.schemas.tsdb1x.BaseTsdb1xDataStore;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.uid.UniqueIdStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the OpenTSDB 1x schema on Google's hosted
 * Bigtable. This is a lower level replacement for AsyncBigtable in that
 * it avoids the HBase API shims and uses the Bigtable client library for
 * Java directly.
 * 
 * @since 3.0
 */
public class Tsdb1xBigtableDataStore extends BaseTsdb1xDataStore {
  private static final Logger LOG = LoggerFactory.getLogger(
          Tsdb1xBigtableDataStore.class);

  /** Config keys */
  public static final String CONFIG_PREFIX = "google.bigtable.";
  public static final String DATA_TABLE_KEY = "data_table";
  public static final String UID_TABLE_KEY = "uid_table";
  public static final String TREE_TABLE_KEY = "tree_table";
  public static final String META_TABLE_KEY = "meta_table";
  
  /** Bigtable config keys. */
  public static final String PROJECT_ID_KEY = "project.id";
  public static final String INSTANCE_ID_KEY = "instance.id";
  public static final String ZONE_ID_KEY = "zone.id";
  public static final String SERVICE_ACCOUNT_ENABLE_KEY = 
      "google.bigtable.auth.service.account.enable";
  public static final String JSON_KEYFILE_KEY = "auth.json.keyfile";
  public static final String CHANNEL_COUNT_KEY = "grpc.channel.count";
  
  // TODO  - move to common location
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
  public static final String ENABLE_APPENDS_KEY = "tsd.storage.enable_appends";

  public static final byte[] DATA_FAMILY = 
      "t".getBytes(Const.ISO_8859_CHARSET);

  /** The namer for tables that converts basic HBase style names to 
   * bigtable names with the project and instance ID. */
  protected final BigtableInstanceName table_namer;
  
  /** The Bigtable session. */
  protected final BigtableSession session;
  
  /** An async executor to share. TODO is this ok? */
  protected AsyncExecutor executor;
  
  /** The executor response pool. */
  protected ExecutorService pool;

  /** A buffer for mutations like in AsyncHBase. */
  private final BulkMutation mutation_buffer;

  /** Name of the table in which timeseries are stored.  */
  protected final byte[] data_table;
  
  /** Name of the table in which UID information is stored. */
  protected final byte[] uid_table;
  
  /** Name of the table where tree data is stored. */
  protected final byte[] tree_table;
  
  /** Name of the table where meta data is stored. */
  protected final byte[] meta_table;
  
  Tsdb1xBigtableDataStore(final Tsdb1xBigtableFactory factory,
                          final String id,
                          final Schema schema) {
    super(id, factory.tsdb(), schema);
    pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    registerConfigs(id, tsdb);
    
    final Configuration config = tsdb.getConfig();
    table_namer = new BigtableInstanceName(
        config.getString(getConfigKey(id, PROJECT_ID_KEY)), 
        config.getString(getConfigKey(id, INSTANCE_ID_KEY)));
    
    data_table = (table_namer.toTableNameStr(
        config.getString(getConfigKey(id, DATA_TABLE_KEY))))
          .getBytes(Const.ISO_8859_CHARSET);
    uid_table = (table_namer.toTableNameStr(
        config.getString(getConfigKey(id, UID_TABLE_KEY))))
          .getBytes(Const.ISO_8859_CHARSET);
    tree_table = (table_namer.toTableNameStr(
        config.getString(getConfigKey(id, TREE_TABLE_KEY))))
          .getBytes(Const.ISO_8859_CHARSET);
    meta_table = (table_namer.toTableNameStr(
        config.getString(getConfigKey(id, META_TABLE_KEY))))
          .getBytes(Const.ISO_8859_CHARSET);
    write_appends = config.getBoolean(ENABLE_APPENDS_KEY);
    
    try {
      final CredentialOptions creds = CredentialOptions.jsonCredentials(
          new FileInputStream(
              config.getString(getConfigKey(id, JSON_KEYFILE_KEY))));
      
      session = new BigtableSession(new BigtableOptions.Builder()
          .setProjectId(config.getString(getConfigKey(id, PROJECT_ID_KEY)))
          .setInstanceId(config.getString(getConfigKey(id, INSTANCE_ID_KEY)))
          .setCredentialOptions(creds)
          .setUserAgent("OpenTSDB_3x")
          .setAdminHost(BigtableOptions.BIGTABLE_ADMIN_HOST_DEFAULT)
          .setAppProfileId(BigtableOptions.BIGTABLE_APP_PROFILE_DEFAULT)
          .setPort(BigtableOptions.BIGTABLE_PORT_DEFAULT)
          .setDataHost(BigtableOptions.BIGTABLE_DATA_HOST_DEFAULT)
          .setBulkOptions(new BulkOptions.Builder()
              .setBulkMutationRpcTargetMs(1000)
              .setBulkMaxRequestSize(1024)
              .setAutoflushMs(1000)
              .build())
          .build());
      
      executor = session.createAsyncExecutor();
      
      final BigtableTableName data_table_name = new BigtableTableName(
          table_namer.toTableNameStr(
              config.getString(getConfigKey(id, DATA_TABLE_KEY))));
      mutation_buffer = session.createBulkMutation(data_table_name);
    } catch (IOException e) {
      throw new StorageException("Unexpected exception: " + e.getMessage(), e);
    }
    
    uid_store = new Tsdb1xBigtableUniqueIdStore(this);
    tsdb.getRegistry().registerSharedObject(Strings.isNullOrEmpty(id) ? 
        "default_uidstore" : id + "_uidstore", uid_store);
  }
  
  @Override
  public Tsdb1xBigtableQueryNode newNode(final QueryPipelineContext context,
                           final TimeSeriesDataSourceConfig config) {
    return new Tsdb1xBigtableQueryNode(this, context, config);
  }

  @Override
  public String id() {
    return "Bigtable";
  }

  public Deferred<Object> shutdown() {
    try {
      session.close();
    } catch (IOException e) {
      Deferred.fromError(e);
    }
    return Deferred.fromResult(null);
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
  
  ExecutorService pool() {
    return pool;
  }
  
  /** @return The Bigtable executor. */
  AsyncExecutor executor() {
    return executor;
  }

  /** @return The session. */
  BigtableSession session() {
    return session;
  }
  
  BigtableInstanceName tableNamer() {
    return table_namer;
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

  @Override
  protected Deferred<WriteStatus> write(final byte[] key,
                                        final byte[] qualifier,
                                        final byte[] value,
                                        final Span span) {
    final MutateRowRequest mutate_row_request =
        MutateRowRequest.newBuilder()
          .setTableNameBytes(UnsafeByteOperations.unsafeWrap(data_table))
          .setRowKey(UnsafeByteOperations.unsafeWrap(key))
          .addMutations(Mutation.newBuilder()
                .setSetCell(SetCell.newBuilder()
                    .setFamilyNameBytes(UnsafeByteOperations.unsafeWrap(DATA_FAMILY))
                    .setColumnQualifier(UnsafeByteOperations.unsafeWrap(qualifier))
                    .setValue(UnsafeByteOperations.unsafeWrap(value))
                    .setTimestampMicros(-1)))
                .build();

    final Deferred<WriteStatus> deferred = new Deferred<WriteStatus>();
    class PutCB implements FutureCallback<MutateRowResponse> {

      @Override
      public void onSuccess(final MutateRowResponse result) {
        if (span != null) {
          span.setSuccessTags().finish();
        }
        deferred.callback(WriteStatus.OK);
      }

      @Override
      public void onFailure(Throwable t) {
        // TODO log?
        // TODO - how do we retry?
//              if (ex instanceof PleaseThrottleException ||
//                  ex instanceof RecoverableException) {
//                if (child != null) {
//                  child.setErrorTags(ex)
//                       .finish();
//                }
//                return WriteStatus.retry("Please retry at a later time.");
//              }
        if (span != null) {
          span.setErrorTags(t)
                  .finish();
        }
        deferred.callback(WriteStatus.error(t.getMessage(), t));
      }

    }

    try {
      Futures.addCallback(
              mutation_buffer.add(mutate_row_request),
              new PutCB(),
              pool);
      return deferred;
    } catch (Throwable t) {
      LOG.error("Unexpected exception", t);
      throw t;
    }
  }

  @Override
  protected Deferred<WriteStatus> writeAppend(final byte[] key,
                                              final byte[] qualifier,
                                              final byte[] value,
                                              final Span span) {
    final ReadModifyWriteRowRequest append_request =
        ReadModifyWriteRowRequest.newBuilder()
            .setTableNameBytes(UnsafeByteOperations.unsafeWrap(data_table))
            .setRowKey(UnsafeByteOperations.unsafeWrap(key))
            .addRules(ReadModifyWriteRule.newBuilder()
                .setFamilyNameBytes(UnsafeByteOperations.unsafeWrap(DATA_FAMILY))
                .setColumnQualifier(UnsafeByteOperations.unsafeWrap(qualifier))
                .setAppendValue(UnsafeByteOperations.unsafeWrap(value)))
            .build();

    final Deferred<WriteStatus> deferred = new Deferred<WriteStatus>();
    class AppendCB implements FutureCallback<ReadModifyWriteRowResponse> {

      @Override
      public void onSuccess(
              final ReadModifyWriteRowResponse result) {
        if (span != null) {
          span.setSuccessTags().finish();
        }
        deferred.callback(WriteStatus.OK);
      }

      @Override
      public void onFailure(final Throwable t) {
        // TODO log?
        // TODO - how do we retry?
//              if (ex instanceof PleaseThrottleException ||
//                  ex instanceof RecoverableException) {
//                if (child != null) {
//                  child.setErrorTags(ex)
//                       .finish();
//                }
//                return WriteStatus.retry("Please retry at a later time.");
//              }
        if (span != null) {
          span.setErrorTags(t)
                  .finish();
        }
        deferred.callback(WriteStatus.error(t.getMessage(), t));
      }

    }

    try {
      Futures.addCallback(
              executor.readModifyWriteRowAsync(append_request),
              new AppendCB(),
              pool);
      return deferred;
    } catch (InterruptedException e) {
      LOG.error("Interrupted", e);
      return Deferred.fromError(e);
    } catch (Throwable t) {
      LOG.error("Unexpected exception", t);
      throw t;
    }
  }

  /**
   * Prepends the {@link #CONFIG_PREFIX} and the current data store ID to
   * the given suffix.
   * @param id The optional ID. May be null or empty.
   * @param suffix A non-null and non-empty suffix.
   * @return A non-null and non-empty config string.
   */
  public static String getConfigKey(final String id, final String suffix) {
    if (Strings.isNullOrEmpty(suffix)) {
      throw new IllegalArgumentException("Suffix cannot be null.");
    }
    if (Strings.isNullOrEmpty(id)) {
      return CONFIG_PREFIX + suffix;
    } else {
      return CONFIG_PREFIX + id + "." + suffix;
    }
  }
  
  static void registerConfigs(final String id, final TSDB tsdb) {
    // We'll sync on the config object to avoid race conditions if 
    // multiple instances of this client are being loaded.
    final Configuration config = tsdb.getConfig();
    synchronized(config) {
      if (!config.hasProperty(getConfigKey(id, DATA_TABLE_KEY))) {
        config.register(getConfigKey(id, DATA_TABLE_KEY), "tsdb", false, 
            "The name of the raw data table for OpenTSDB.");
      }
      
      if (!config.hasProperty(getConfigKey(id, UID_TABLE_KEY))) {
        config.register(getConfigKey(id, UID_TABLE_KEY), "tsdb-uid", false, 
            "The name of the UID mapping table for OpenTSDB.");
      }
      
      if (!config.hasProperty(getConfigKey(id, TREE_TABLE_KEY))) {
        config.register(getConfigKey(id, TREE_TABLE_KEY), "tsdb-tree", false, 
            "The name of the Tree table for OpenTSDB.");
      }
      
      if (!config.hasProperty(getConfigKey(id, META_TABLE_KEY))) {
        config.register(getConfigKey(id, META_TABLE_KEY), "tsdb-meta", false, 
            "The name of the Meta data table for OpenTSDB.");
      }
      if (!config.hasProperty(ENABLE_APPENDS_KEY)) {
        config.register(ENABLE_APPENDS_KEY, false, false,
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
      
      // bigtable configs
      if (!config.hasProperty(getConfigKey(id, PROJECT_ID_KEY))) {
        config.register(getConfigKey(id, PROJECT_ID_KEY), null, false, 
            "The project ID hosting the Bigtable cluster");
      }
      if (!config.hasProperty(getConfigKey(id, INSTANCE_ID_KEY))) {
        config.register(getConfigKey(id, INSTANCE_ID_KEY), null, false, 
            "The cluster ID assigned to the Bigtable cluster at creation.");
      }
      if (!config.hasProperty(getConfigKey(id, ZONE_ID_KEY))) {
        config.register(getConfigKey(id, ZONE_ID_KEY), null, false, 
            "The name of the zone where the Bigtable cluster is operating.");
      }
      if (!config.hasProperty(getConfigKey(id, SERVICE_ACCOUNT_ENABLE_KEY))) {
        config.register(getConfigKey(id, SERVICE_ACCOUNT_ENABLE_KEY), true, false, 
            "Whether or not to use a Google cloud service account to connect.");
      }
      if (!config.hasProperty(getConfigKey(id, JSON_KEYFILE_KEY))) {
        config.register(getConfigKey(id, JSON_KEYFILE_KEY), null, false, 
            "The full path to the JSON formatted key file associated with "
            + "the service account you want to use for Bigtable access. "
            + "Download this from your cloud console.");
      }
      if (!config.hasProperty(getConfigKey(id, CHANNEL_COUNT_KEY))) {
        config.register(getConfigKey(id, CHANNEL_COUNT_KEY), 
            Runtime.getRuntime().availableProcessors(), false, 
            "The number of sockets opened to the Bigtable API for handling "
            + "RPCs. For higher throughput consider increasing the "
            + "channel count.");
      }
    }
  }
  
  /**
   * <p>wasMutationApplied.</p>
   *<b>NOTE</b> Shamelessly cribbed from Bigtable client.
   * @param request a {@link com.google.bigtable.v2.CheckAndMutateRowRequest} object.
   * @param response a {@link com.google.bigtable.v2.CheckAndMutateRowResponse} object.
   * @return a boolean.
   */
  public static boolean wasMutationApplied(
      CheckAndMutateRowRequest request,
      CheckAndMutateRowResponse response) {

    // If we have true mods, we want the predicate to have matched.
    // If we have false mods, we did not want the predicate to have matched.
    return (request.getTrueMutationsCount() > 0
        && response.getPredicateMatched())
        || (request.getFalseMutationsCount() > 0
        && !response.getPredicateMatched());
  }
}
