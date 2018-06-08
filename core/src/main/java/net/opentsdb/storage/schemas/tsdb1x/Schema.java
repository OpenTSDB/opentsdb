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
package net.opentsdb.storage.schemas.tsdb1x;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.common.Const;
import net.opentsdb.configuration.ConfigurationException;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.meta.MetaDataStorageSchema;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.filter.TagVLiteralOrFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.StorageException;
import net.opentsdb.storage.TimeSeriesDataStore;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdFactory;
import net.opentsdb.uid.UniqueIdStore;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Exceptions;
import net.opentsdb.utils.JSON;

/**
 * The interface for an OpenTSDB version 1 and version 2 schema where 
 * we supported HBase/Bigtable style data stores with row keys 
 * consisting of optional salt bucket, metric UID, normalized timestamp
 * and tag key + tag value UIDs. Columns encode various data types with
 * qualifiers and value byte arrays.
 * 
 * @since 3.0
 */
public class Schema implements TimeSeriesDataStore {

  public static final byte APPENDS_PREFIX = 5;
  
  public static final String QUERY_BYTE_LIMIT_KEY = "tsd.query.limits.bytes";
  public static final long QUERY_BYTE_LIMIT_DEFAULT = 0;
  public static final String QUERY_DP_LIMIT_KEY = "tsd.query.limits.data_points";
  public static final long QUERY_DP_LIMIT_DEFAULT = 0;
  public static final String QUERY_REVERSE_KEY = "tsd.query.time.descending";
  public static final String QUERY_KEEP_FIRST_KEY = "tsd.query.duplicates.keep_earliest";
  public static final String TIMELESS_SALTING_KEY = "tsd.storage.salt.timeless";
  public static final String OLD_SALTING_KEY = "tsd.storage.salt.old";
  
  /** Max time delta (in seconds) we can store in a column qualifier.  */
  public static final short MAX_RAW_TIMESPAN = 3600;
  
  /** Number of bytes on which a timestamp is encoded.  */
  public static final short TIMESTAMP_BYTES = 4;
  public static final String METRIC_TYPE = "metric";
  public static final String TAGK_TYPE = "tagk";
  public static final String TAGV_TYPE = "tagv";
  
  private final TSDB tsdb;
  private final String id;
  private final Tsdb1xDataStore data_store;
  private final UniqueIdStore uid_store;
  
  private final UniqueId metrics;
  private final UniqueId tag_names;
  private final UniqueId tag_values;
  
  protected int metric_width;
  protected int tagk_width;
  protected int tagv_width;
  protected int salt_buckets;
  protected int salt_width;
  
  protected final boolean timeless_salting;
  protected final boolean old_salting;
  protected final DefaultRollupConfig rollup_config;
  
  protected Map<TypeToken<?>, Codec> codecs;
  
  protected MetaDataStorageSchema meta_schema;
  
  public Schema(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = id;
    setConfig();
    
    String key = configKey("data.store");
    final String store_name = tsdb.getConfig().getString(key);
    
    final Tsdb1xDataStoreFactory store_factory = tsdb.getRegistry()
          .getDefaultPlugin(Tsdb1xDataStoreFactory.class);
    if (store_factory == null) {
      throw new ConfigurationException("No factory found for: " + store_name);
    }
    data_store = store_factory.newInstance(tsdb, id, this);
    if (data_store == null) {
      throw new IllegalStateException("Store factory " + store_factory 
          + " returned a null data store instance.");
    }
    
    // TODO there's a better way. For now the data store will dump it
    // in the shared objects map.
    key = Strings.isNullOrEmpty(id) ? "default_uidstore" : id + "_uidstore";
    uid_store = (UniqueIdStore) tsdb.getRegistry().getSharedObject(key);
    if (uid_store == null) {
      throw new IllegalStateException("Unable to locate a UID store in "
          + "the shared object repo with the ID: " + key);
    }
    
    key = configKey("uid.cache.type.metric");
    String value = tsdb.getConfig().getString(key);
    if (Strings.isNullOrEmpty(value)) { 
      throw new ConfigurationException("Null value for config key: " + key);
    }
    UniqueIdFactory uid_factory = (UniqueIdFactory) tsdb.getRegistry()
        .getPlugin(UniqueIdFactory.class, value);
    if (uid_factory == null) {
      throw new IllegalStateException("Unable to locate a "
          + "registered UniqueIdFactory with the ID: " + value);
    }
    metrics = uid_factory.newInstance(tsdb, id, UniqueIdType.METRIC, uid_store);
    if (metrics == null) {
      throw new IllegalStateException("Factory " + uid_factory 
          + " returned a null UniqueId instance.");
    }
    
    key = configKey("uid.cache.type.tagk");
    value = tsdb.getConfig().getString(key);
    if (Strings.isNullOrEmpty(value)) { 
      throw new ConfigurationException("Null value for config key: " + key);
    }
    uid_factory = (UniqueIdFactory) tsdb.getRegistry()
        .getPlugin(UniqueIdFactory.class, value);
    if (uid_factory == null) {
      throw new IllegalStateException("Unable to locate a "
          + "registered UniqueIdFactory with the ID: " + value);
    }
    tag_names = uid_factory.newInstance(tsdb, id, UniqueIdType.TAGK, uid_store);
    if (tag_names == null) {
      throw new IllegalStateException("Factory " + uid_factory 
          + " returned a null UniqueId instance.");
    }
    
    key = configKey("uid.cache.type.tagv");
    value = tsdb.getConfig().getString(key);
    if (Strings.isNullOrEmpty(value)) { 
      throw new ConfigurationException("Null value for config key: " + key);
    }
    uid_factory = (UniqueIdFactory) tsdb.getRegistry()
        .getPlugin(UniqueIdFactory.class, value);
    if (uid_factory == null) {
      throw new IllegalStateException("Unable to locate a "
          + "registered UniqueIdFactory with the ID: " + value);
    }
    tag_values = uid_factory.newInstance(tsdb, id, UniqueIdType.TAGV, uid_store);
    if (tag_values == null) {
      throw new IllegalStateException("Factory " + uid_factory 
          + " returned a null UniqueId instance.");
    }
    
    key = configKey("rollups.enable");
    final boolean rollups_enabled = tsdb.getConfig().getBoolean(key);
    if (rollups_enabled) {
      key = configKey("rollups.config");
      value = tsdb.getConfig().getString(key);
      if (Strings.isNullOrEmpty(value)) { 
        throw new ConfigurationException("Null value for config key: " + key);
      }
      
      if (value.endsWith(".json")) {
        try {
          value = Files.toString(new File(value), Const.UTF8_CHARSET);
        } catch (IOException e) {
          throw new IllegalArgumentException("Failed to open conf file: " 
              + value, e);
        }
      }
      rollup_config = JSON.parseToObject(value, DefaultRollupConfig.class);
    } else {
      rollup_config = null;
    }
    
    if (!tsdb.getConfig().hasProperty(QUERY_BYTE_LIMIT_KEY)) {
      tsdb.getConfig().register(QUERY_BYTE_LIMIT_KEY, 
          QUERY_BYTE_LIMIT_DEFAULT, true, 
          "The number of bytes allowed in a single query result or segment");
    }
    if (!tsdb.getConfig().hasProperty(QUERY_DP_LIMIT_KEY)) {
      tsdb.getConfig().register(QUERY_DP_LIMIT_KEY, 
          QUERY_DP_LIMIT_DEFAULT, true, 
          "The number of data points or values allowed in a single "
          + "query result or segment");
    }
    if (!tsdb.getConfig().hasProperty(QUERY_REVERSE_KEY)) {
      tsdb.getConfig().register(QUERY_REVERSE_KEY, false, true,
          "Results are iterated and returned in descending time order "
          + "instead of ascending time order.");
    }
    if (!tsdb.getConfig().hasProperty(QUERY_KEEP_FIRST_KEY)) {
      tsdb.getConfig().register(QUERY_KEEP_FIRST_KEY, false, true,
          "Whether or not to keep the earliest value (true) when "
          + "de-duplicating or to keep the latest version (false).");
    }
    if (!tsdb.getConfig().hasProperty(TIMELESS_SALTING_KEY)) {
      tsdb.getConfig().register(TIMELESS_SALTING_KEY, true, false,
          "Whether or not timestamps are incorporated into the salting "
          + "calculations. When true, time is not incorporated, when false "
          + "it is included. NOTE: For almost all uses, leave this as true.");
    }
    if (!tsdb.getConfig().hasProperty(OLD_SALTING_KEY)) {
      tsdb.getConfig().register(OLD_SALTING_KEY, false, false,
          "Whether or not to enable the old, stringified salting "
          + "calculation. DO NOT SET THIS TO TRUE!");
    }
    
    timeless_salting = tsdb.getConfig().getBoolean(TIMELESS_SALTING_KEY);
    old_salting = tsdb.getConfig().getBoolean(OLD_SALTING_KEY);
    codecs = Maps.newHashMapWithExpectedSize(2);
    codecs.put(NumericType.TYPE, new NumericCodec());
    codecs.put(NumericSummaryType.TYPE, new NumericSummaryCodec());
    
    meta_schema = tsdb.getRegistry()
        .getDefaultPlugin(MetaDataStorageSchema.class);
  }
  
  @Override
  public QueryNode newNode(final QueryPipelineContext context,
                           final String id,
                           final QueryNodeConfig config) {
    return data_store.newNode(context, id, config);
  }

  @Override
  public QueryNode newNode(QueryPipelineContext context, String id) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Class<? extends QueryNodeConfig> nodeConfigClass() {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public Deferred<List<byte[]>> encodeJoinKeys(final List<String> join_keys, final Span span) {
    return getIds(UniqueIdType.TAGK, join_keys, span);
  }
  
  /**
   * Strips the salt and timestamp out of a key to get the TSUID of the
   * series.
   * @param key A non-null and non-empty byte array.
   * @return A non-null byte array without the salt and timestamp.
   * @throws IllegalArgumentException if the key was null or encoded 
   * improperly.
   */
  public byte[] getTSUID(final byte[] key) {
    if (Bytes.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    int offset = salt_width + metric_width + TIMESTAMP_BYTES;
    if (key.length <= offset) {
      throw new IllegalArgumentException("Key was too short.");
    }
    final byte[] timeless = new byte[key.length - salt_width - TIMESTAMP_BYTES];
    System.arraycopy(key, salt_width, timeless, 0, metric_width);
    System.arraycopy(key, offset, timeless, metric_width, key.length - offset);
    return timeless;
  }
  
  /**
   * Retrieve the row timestamp from the row key.
   * @param key A non-null and non-empty byte array.
   * @param timestamp The non-null timestamp to update.
   * @return A non-null timestamp object.
   * @throws IllegalArgumentException if the key was null or encoded 
   * improperly.
   */
  public void baseTimestamp(final byte[] key, final TimeStamp timestamp) {
    if (Bytes.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null");
    }
    if (key.length < salt_width + metric_width + TIMESTAMP_BYTES) {
      throw new IllegalArgumentException("Key was too short.");
    }
    timestamp.update(Bytes.getUnsignedInt(key, salt_width + metric_width), 0);
  }
  
  public long baseTimestamp(final byte[] key) {
    return Bytes.getUnsignedInt(key, salt_width + metric_width);
  }
  
  /**
   * The width of the requested UID type in bytes.
   * @param type A non-null type.
   * @return The width.
   * @throws IllegalArgumentException if the type was null or of a type
   * not supported by this schema.
   */
  public int uidWidth(final UniqueIdType type) {
    if (type == null) {
      throw new IllegalArgumentException("The type cannot be null.");
    }
    switch(type) {
    case METRIC:
      return metric_width;
    case TAGK:
      return tagk_width;
    case TAGV:
      return tagv_width;
    default:
      throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }
  
  /**
   * Resolves the given V2 filter, parsing the tag keys and optional tag
   * value literals. The resulting list is populated in the same order as
   * the TagVFilters in the given filter set.
   * @param filter A non-null filter to resolve.
   * @param span An optional tracing span.
   * @return A deferred resolving to a non-null list of resolved filters
   * if successful or an exception if something went wrong. The list may
   * be empty if the filter didn't have any TagVFilters. (In which case
   * this shouldn't be called.)
   * @throws IllegalArgumentException if the filter was null.
   */
  public Deferred<List<ResolvedFilter>> resolveUids(final Filter filter, 
                                                    final Span span) {
    if (filter == null) {
      throw new IllegalArgumentException("Filter cannot be null.");
    }
    
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".resolveUids")
          .withTag("dataStore", data_store.id())
          .withTag("filter", filter.toString())
          .start();
    } else {
      child = null;
    }
    
    if (filter.getTags() == null || filter.getTags().isEmpty()) {
      if (child != null) {
        child.setSuccessTags()
          .finish();
      }
      return Deferred.fromResult(Collections.emptyList());
    }
    
    final List<ResolvedFilter> resolutions = 
        Lists.newArrayListWithCapacity(filter.getTags().size());
    for (int i = 0; i < filter.getTags().size(); i++) {
      resolutions.add(null);
    }
    
    class TagVCB implements Callback<Object, List<byte[]>> {
      final int idx;
      
      TagVCB(final int idx) {
        this.idx = idx;
      }

      @Override
      public Object call(final List<byte[]> uids) throws Exception {
        // since  we're working on an array list and only one index per
        // callback, we can avoid synchronizing on it.
        ((ResolvedFilterImplementation) resolutions.get(idx))
          .tag_values = uids;
        return null;
      }
      
    }
    
    class TagKCB implements Callback<Deferred<Object>, byte[]> {
      final int idx;
      final TagVFilter f;
      
      TagKCB(final int idx, final TagVFilter f) {
        this.idx = idx;
        this.f = f;
      }

      @Override
      public Deferred<Object> call(final byte[] uid) throws Exception {
        final ResolvedFilterImplementation resolved = 
            new ResolvedFilterImplementation();
        resolved.tag_key = uid;
        
        // since  we're working on an array list and only one index per
        // callback, we can avoid synchronizing on it.
        resolutions.set(idx, resolved);
        
        if (f instanceof TagVLiteralOrFilter) {
          final List<String> tags = Lists.newArrayList(
              ((TagVLiteralOrFilter) f).literals());
          return tag_values.getIds(tags, 
              child != null ? child : span)
              .addCallback(new TagVCB(idx));
        } else {
          return Deferred.fromResult(null);
        }
      }
    }
    
    // Start the resolution chain here.
    final List<Deferred<Object>> deferreds = 
        Lists.newArrayListWithCapacity(filter.getTags().size());
    for (int i = 0; i < filter.getTags().size(); i++) {
      final TagVFilter f = filter.getTags().get(i);
      deferreds.add(tag_names.getId(f.getTagk(), 
          child != null ? child : span)
          .addCallbackDeferring(new TagKCB(i, f)));
    }
    
    /** Error callback to cast the exception to a StorageException */
    class ErrorCB implements Callback<List<ResolvedFilter>, Exception> {
      @Override
      public List<ResolvedFilter> call(final Exception ex) throws Exception {
        Throwable t = ex;
        if (t instanceof DeferredGroupException) {
          t = Exceptions.getCause((DeferredGroupException) ex);
        }
        if (child != null) {
          child.setErrorTags()
            .log("Exception", t)
            .finish();
        }
        throw new StorageException("Failed to resolve IDs: " 
            + t.getMessage(), t);
      }
    }
    
    class FinalCB implements Callback<List<ResolvedFilter>, ArrayList<Object>> {
      @Override
      public List<ResolvedFilter> call(final ArrayList<Object> ignored)
          throws Exception {
        if (child != null) {
          child.setSuccessTags()
            .finish();
        }
        return resolutions;
      }
    }
    
    return Deferred.group(deferreds)
        .addCallbacks(new FinalCB(), new ErrorCB());
  }
  
  /**
   * Converts the given string to it's UID value based on the type.
   * @param type A non-null UID type.
   * @param name A non-null and non-empty string.
   * @param span An optional tracing span.
   * @return A deferred resolving to the UID if successful or an exception.
   * @throws IllegalArgumentException if the type was null or the string
   * was null or empty.
   */
  public Deferred<byte[]> getId(final UniqueIdType type, 
                                final String name,
                                final Span span) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null");
    }
    if (Strings.isNullOrEmpty(name)) {
      throw new IllegalArgumentException("Name cannot be null or empty.");
    }
    
    // not tracing here since we're just a router
    switch(type) {
    case METRIC:
      return metrics.getId(name, span);
    case TAGK:
      return tag_names.getId(name, span);
    case TAGV:
      return tag_values.getId(name, span);
    default:
      throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }
  
  /**
   * Converts the list of strings to their IDs, maintaining order.
   * @param type A non-null UID type.
   * @param names A non-null and non-empty list of strings.
   * @param span An optional tracing span.
   * @return A deferred resolving to the list of UIDs in order if 
   * successful or an exception.
   * @throws IllegalArgumentException if the type was null or the
   * IDs was null or an ID in the list was null or empty.
   */
  public Deferred<List<byte[]>> getIds(final UniqueIdType type, 
                                       final List<String> names,
                                       final Span span) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null");
    }
    if (names == null || names.isEmpty()) {
      throw new IllegalArgumentException("Names cannot be null or empty.");
    }
    
    // not tracing here since we're just a router
    switch(type) {
    case METRIC:
      return metrics.getIds(names, span);
    case TAGK:
      return tag_names.getIds(names, span);
    case TAGV:
      return tag_values.getIds(names, span);
    default:
      throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }
  
  /**
   * Converts the UID to the equivalent string name.
   * @param type A non-null UID type.
   * @param id A non-null and non-empty byte array UID.
   * @param span An optional tracing span.
   * @return A deferred resolving to the string if successful or an
   * exception.
   * @throws IllegalArgumentException if the type was null or the ID 
   * null or empty.
   */
  public Deferred<String> getName(final UniqueIdType type, 
                                  final byte[] id,
                                  final Span span) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null");
    }
    if (Bytes.isNullOrEmpty(id)) {
      throw new IllegalArgumentException("ID cannot be null or empty.");
    }
 
    // not tracing here since we're just a router
    switch(type) {
    case METRIC:
      return metrics.getName(id, span);
    case TAGK:
      return tag_names.getName(id, span);
    case TAGV:
      return tag_values.getName(id, span);
    default:
      throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }
  
  /**
   * Converts the list of UIDs to the equivalent string name maintaining
   * order.
   * @param type A non-null UID type.
   * @param ids A deferred resolving to a list of the strings in order
   * if successful or an exception.
   * @param span An optional tracing span.
   * @throws IllegalArgumentException if the type was null or the strings
   * list was null or any string in the list was null or empty.
   * @return
   */
  public Deferred<List<String>> getNames(final UniqueIdType type, 
                                         final List<byte[]> ids,
                                         final Span span) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null");
    }
    if (ids == null || ids.isEmpty()) {
      throw new IllegalArgumentException("IDs cannot be null or empty.");
    }
 
    // not tracing here since we're just a router
    switch(type) {
    case METRIC:
      return metrics.getNames(ids, span);
    case TAGK:
      return tag_names.getNames(ids, span);
    case TAGV:
      return tag_values.getNames(ids, span);
    default:
      throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }

  @Override
  public String id() {
    return id;
  }
  
  /** @return The number of buckets to spread data into. */
  public int saltBuckets() {
    return salt_buckets;
  }
  
  /** @return The width of the salt prefix in row keys. */
  public int saltWidth() {
    return salt_width;
  }
  
  /** @return The width of metric UIDs. */
  public int metricWidth() {
    return metric_width;
  }
  
  /** @return The width of tag key UIDs. */
  public int tagkWidth() {
    return tagk_width;
  }
  
  /** @return The width of tag value UIDs. */
  public int tagvWidth() {
    return tagv_width;
  }
  
  /**
   * Sets the proper salt bucket in the row key byte array. The array
   * must be pre-configured with the proper number of bytes for the salt.
   * @param row_key A non-null and non-empty row key.
   */
  public void prefixKeyWithSalt(final byte[] row_key) {
    if (salt_width < 1) {
      return;
    }
    if ((row_key.length < salt_width + metric_width) ||    
        (Bytes.memcmp(row_key, new byte[salt_width + metric_width], 
            salt_width, metric_width) == 0)) {    
        //Metric id is 0, which means it is a global row. Don't prefix it with salt   
        return;
      }
    
    if (timeless_salting) {
      // we want the metric and tags, not the timestamp
      int hash = 1;
      for (int i = salt_width; i < row_key.length; i++) {
        hash = 31 * hash + row_key[i];
        if (i + 1 == salt_width + metric_width) {
          i = salt_width + metric_width + Const.TIMESTAMP_BYTES - 1;
        }
      }
      int modulo = hash % salt_buckets;
      if (modulo < 0) {
        // make sure we return a positive salt.
        modulo = modulo * -1;
      }
      prefixKeyWithSalt(row_key, modulo);
    } else if (old_salting) {
      // DON'T DO THIS! Don't USE IT!
      int modulo = (new String(Arrays.copyOfRange(row_key, salt_width,    
          row_key.length), Const.ASCII_US_CHARSET)).hashCode() % salt_buckets;   
      if (modulo < 0) {
        // make sure we return a positive salt.
        modulo = modulo * -1;   
      }
      prefixKeyWithSalt(row_key, modulo);
    } else {
      int hash = 1;
      for (int i = salt_width; i < row_key.length; i++) {
        hash = 31 * hash + row_key[i];
      }
      int modulo = hash % salt_buckets;
      if (modulo < 0) {
        // make sure we return a positive salt.
        modulo = modulo * -1;
      }
      prefixKeyWithSalt(row_key, modulo);
    }
  }
  
  /**
   * Sets the bucket on the row key. The row key must be pre-allocated
   * the proper number of salt bytes at the start of the array.
   * @param row_key A non-null and non-empty row key.
   * @param bucket A 0 based positive bucket ID.
   */
  public void prefixKeyWithSalt(final byte[] row_key, final int bucket) {
    if (salt_width == 1) {
      row_key[0] = (byte) bucket;
      return;
    }
    
    int shift = 0;
    for (int i = 0; i < salt_width; i++) {
      row_key[salt_width - i - 1] = 0;
      row_key[salt_width - i - 1] = (byte) (bucket >>> shift);
      shift += 8;
    }
  }
  
  public DefaultRollupConfig rollupConfig() {
    return rollup_config;
  }
  
  /**
   * Sets the time in a raw data table row key.
   * 
   * @param row The row to modify.
   * @param base_time The base time to store.
   * @throws IllegalArgumentException if the row was null, empty or too
   * short.
   * @since 2.3
   */
  public void setBaseTime(final byte[] row, final int base_time) {
    if (Bytes.isNullOrEmpty(row)) {
      throw new IllegalArgumentException("Row cannot be null or empty.");
    }
    if (row.length < salt_width + metric_width + TIMESTAMP_BYTES) {
      throw new IllegalArgumentException("Row is too short.");
    }
    Bytes.setInt(row, base_time, salt_width + metric_width);
  }
  
  /** @return Whether or not timeless salting is enabled. */
  public boolean timelessSalting() {
    return timeless_salting;
  }
  
  public net.opentsdb.storage.schemas.tsdb1x.Span<? extends TimeSeriesDataType> newSpan(
      final TypeToken<? extends TimeSeriesDataType> type, 
      final boolean reversed) {
    final Codec codec = codecs.get(type);
    if (codec == null) {
      throw new IllegalArgumentException("No codec loaded for type: " + type);
    }
    return codec.newSequences(reversed);
  }
  
  public RowSeq newRowSeq(final byte prefix, 
      final long base_time) {
    // TODO - implement
    return null;
//    final Codec codec = codecs.get(type);
//    if (codec == null) {
//      throw new IllegalArgumentException("No codec loaded for type: " + type);
//    }
//    return codec.newRowSeq(base_time);
  }
  
  /** @return The meta schema if implemented and assigned, null if not. */
  public MetaDataStorageSchema metaSchema() {
    return meta_schema;
  }
  
  static class ResolvedFilterImplementation implements ResolvedFilter {
    protected byte[] tag_key;
    protected List<byte[]> tag_values;
    
    @Override
    public byte[] getTagKey() {
      return tag_key;
    }

    @Override
    public List<byte[]> getTagValues() {
      return tag_values;
    }
  }
  
  String configKey(final String suffix) {
    return "tsd.storage." + (Strings.isNullOrEmpty(id) ? "" : id + ".")
      + suffix;
  }
  
  void setConfig() {
    String key = configKey("uid.width.metric");
    if (!tsdb.getConfig().hasProperty(key)) {
      tsdb.getConfig().register(key, 3, false, 
          "The width, in bytes, of UIDs for metrics.");
    }
    metric_width = tsdb.getConfig().getInt(key);
    
    key = configKey("uid.width.tagk");
    if (!tsdb.getConfig().hasProperty(key)) {
      tsdb.getConfig().register(key, 3, false, 
          "The width, in bytes, of UIDs for tag keys.");
    }
    tagk_width = tsdb.getConfig().getInt(key);
    
    key = configKey("uid.width.tagv");
    if (!tsdb.getConfig().hasProperty(key)) {
      tsdb.getConfig().register(key, 3, false, 
          "The width, in bytes, of UIDs for tag values.");
    }
    tagv_width = tsdb.getConfig().getInt(key);
    
    key = configKey("uid.cache.type.metric");
    if (!tsdb.getConfig().hasProperty(key)) {
      tsdb.getConfig().register(key, "LRU", false, 
          "The name of the UniqueId factory used for caching metric UIDs.");
    }
    
    key = configKey("uid.cache.type.tagk");
    if (!tsdb.getConfig().hasProperty(key)) {
      tsdb.getConfig().register(key, "LRU", false, 
          "The name of the UniqueId factory used for caching tagk UIDs.");
    }
    
    key = configKey("uid.cache.type.tagv");
    if (!tsdb.getConfig().hasProperty(key)) {
      tsdb.getConfig().register(key, "LRU", false, 
          "The name of the UniqueId factory used for caching tagv UIDs.");
    }
    
    key = configKey("salt.buckets");
    if (!tsdb.getConfig().hasProperty(key)) {
      tsdb.getConfig().register(key, 20, false, 
          "The number of salt buckets to spread data into.");
    }
    salt_buckets = tsdb.getConfig().getInt(key);
    
    key = configKey("salt.width");
    if (!tsdb.getConfig().hasProperty(key)) {
      tsdb.getConfig().register(key, 0, false, 
          "The width, in bytes, of the salt prefix in row keys.");
    }
    salt_width = tsdb.getConfig().getInt(key);
    
    key = configKey("data.store");
    if (!tsdb.getConfig().hasProperty(key)) {
      tsdb.getConfig().register(key, null, false, 
          "The name of the data store factory to load and associate "
              + "with this schema.");
    }
    
    key = configKey("rollups.enable");
    if (!tsdb.getConfig().hasProperty(key)) {
      tsdb.getConfig().register(key, false, false, 
          "Whether or not rollups are enabled for this schema.");
    }
    
    key = configKey("rollups.config");
    if (!tsdb.getConfig().hasProperty(key)) {
      tsdb.getConfig().register(key, null, false, 
          "The path to a JSON file containing the rollup configuration.");
    }
  }
  
  @VisibleForTesting
  Tsdb1xDataStore dataStore() {
    return data_store;
  }
  
  @VisibleForTesting
  UniqueIdStore uidStore() {
    return uid_store;
  }
  
  @VisibleForTesting
  UniqueId metrics() {
    return metrics;
  }
  
  @VisibleForTesting
  UniqueId tagNames() {
    return tag_names;
  }
  
  @VisibleForTesting
  UniqueId tagValues() {
    return tag_values;
  }
  
  @Override
  public Deferred<Object> write(TimeSeriesStringId id, TimeSeriesValue<?> value,
      Span span) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<TimeSeriesStringId> resolveByteId(TimeSeriesByteId id, final Span span) {
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".decode")
          .start();
    } else {
      child = null;
    }
    
    final List<byte[]> tagks = Lists.newArrayListWithCapacity(id.tags().size());
    final List<byte[]> tagvs = Lists.newArrayListWithCapacity(id.tags().size());
    final List<String> tagk_strings = Lists.newArrayListWithCapacity(id.tags().size());
    final List<String> tagv_strings = Lists.newArrayListWithCapacity(id.tags().size());
    for (final Entry<byte[], byte[]> pair : id.tags().entrySet()) {
      tagks.add(pair.getKey());
      tagvs.add(pair.getValue());
    }
    
    // resolve the tag keys
    final BaseTimeSeriesStringId.Builder builder = 
        BaseTimeSeriesStringId.newBuilder();
    class FinalCB implements Callback<TimeSeriesStringId, ArrayList<Object>> {
      @Override
      public TimeSeriesStringId call(final ArrayList<Object> ignored) throws Exception {
        if (tagk_strings != null) {
          for (int i = 0; i < tagk_strings.size(); i++) {
            builder.addTags(tagk_strings.get(i), tagv_strings.get(i));
          }
        }
        
        final TimeSeriesStringId id = builder.build();
        if (child != null) {
          child.setSuccessTags()
               .finish();
        }
        return id;
      }
    }
    
    class AggDisjointTagCB implements Callback<Object, List<String>> {
      final boolean is_disjoint; // false => agg tags
      
      AggDisjointTagCB(final boolean is_disjoint) {
        this.is_disjoint = is_disjoint;
      }
      
      @Override
      public Object call(final List<String> names) throws Exception {
        for (int i = 0; i < names.size(); i++) {
          if (Strings.isNullOrEmpty(names.get(i))) {
            throw new NoSuchUniqueId(Schema.TAGK_TYPE, 
                is_disjoint ? id.disjointTags().get(i) : id.aggregatedTags().get(i));
          }
          if (is_disjoint) {
            builder.addDisjointTag(names.get(i));
          } else {
            builder.addAggregatedTag(names.get(i));
          }
        }
        return null;
      }
    }
    
    class TagKeyCB implements Callback<Object, List<String>> {
      @Override
      public Object call(final List<String> names) throws Exception {
        for (int i = 0; i < names.size(); i++) {
          if (Strings.isNullOrEmpty(names.get(i))) {
            throw new NoSuchUniqueId(Schema.TAGK_TYPE, tagks.get(i));
          }
          tagk_strings.add(names.get(i));
        }
        return null;
      }
    }
    
    class TagValueCB implements Callback<Object, List<String>> {
      @Override
      public Object call(final List<String> values) throws Exception {
        for (int i = 0; i < values.size(); i++) {
          if (Strings.isNullOrEmpty(values.get(i))) {
            throw new NoSuchUniqueId(Schema.TAGV_TYPE, tagvs.get(i));
          }
          tagv_strings.add(values.get(i));
        }
        return null;
      }
    }
    
    class MetricCB implements Callback<Object, String> {
      @Override
      public Object call(final String metric) throws Exception {
        if (Strings.isNullOrEmpty(metric)) {
          throw new NoSuchUniqueId(Schema.METRIC_TYPE, id.metric());
        }
        builder.setMetric(metric);
        return null;
      }
    }
    
    class ErrCB implements Callback<Object, Exception> {
      @Override
      public Object call(final Exception ex) throws Exception {
        if (child != null) {
          child.setErrorTags()
               .log("Exception", 
                   (ex instanceof DeferredGroupException) ? 
                       Exceptions.getCause((DeferredGroupException) ex) : ex)
               .finish();
        }
        if (ex instanceof DeferredGroupException) {
          final Exception t = (Exception) Exceptions
              .getCause((DeferredGroupException) ex);
          throw t;
        }
        throw ex;
      }
    }
    
    final List<Deferred<Object>> deferreds = 
        Lists.newArrayListWithCapacity(3);
    try {
      // resolve the metric
      deferreds.add(getName(UniqueIdType.METRIC, id.metric(), 
          child != null ? child : span)
            .addCallback(new MetricCB()));
      
      if (!id.tags().isEmpty()) {
        deferreds.add(this.getNames(UniqueIdType.TAGK, tagks, child)
            .addCallback(new TagKeyCB()));
        deferreds.add(this.getNames(UniqueIdType.TAGV, tagvs, child)
            .addCallback(new TagValueCB()));
      }
      
      if (!id.aggregatedTags().isEmpty()) {
        deferreds.add(this.getNames(UniqueIdType.TAGK, id.aggregatedTags(), child)
            .addCallback(new AggDisjointTagCB(false)));
      }
      
      if (!id.disjointTags().isEmpty()) {
        deferreds.add(this.getNames(UniqueIdType.TAGK, id.disjointTags(), child)
            .addCallback(new AggDisjointTagCB(true)));
      }
      
      return Deferred.group(deferreds)
          .addCallbacks(new FinalCB(), new ErrCB());
    } catch (Exception e) {
      return Deferred.fromError(e);
    }
  }

  @Override
  public Deferred<Object> shutdown() {
    // TODO Auto-generated method stub
    return null;
  }
  
}
