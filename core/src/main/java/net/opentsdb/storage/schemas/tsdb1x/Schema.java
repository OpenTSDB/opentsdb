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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.configuration.ConfigurationException;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.meta.MetaDataStorageSchema;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.filter.TagVLiteralOrFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.StorageException;
import net.opentsdb.storage.StorageSchema;
import net.opentsdb.storage.TimeSeriesDataStore;
import net.opentsdb.storage.TimeSeriesDataStoreFactory;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdFactory;
import net.opentsdb.uid.UniqueIdStore;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Exceptions;

/**
 * The interface for an OpenTSDB version 1 and version 2 schema where 
 * we supported HBase/Bigtable style data stores with row keys 
 * consisting of optional salt bucket, metric UID, normalized timestamp
 * and tag key + tag value UIDs. Columns encode various data types with
 * qualifiers and value byte arrays.
 * 
 * @since 3.0
 */
public class Schema implements StorageSchema {

  public static final byte APPENDS_PREFIX = 5;
  
  public static final String QUERY_BYTE_LIMIT_KEY = "tsd.query.limits.bytes";
  public static final long QUERY_BYTE_LIMIT_DEFAULT = 0;
  public static final String QUERY_DP_LIMIT_KEY = "tsd.query.limits.data_points";
  public static final long QUERY_DP_LIMIT_DEFAULT = 0;
  public static final String QUERY_REVERSE_KEY = "tsd.query.time.descending";
  public static final String QUERY_KEEP_FIRST_KEY = "tsd.query.duplicates.keep_earliest";
  
  /** Max time delta (in seconds) we can store in a column qualifier.  */
  public static final short MAX_RAW_TIMESPAN = 3600;
  
  /** Number of bytes on which a timestamp is encoded.  */
  public static final short TIMESTAMP_BYTES = 4;
  public static final String METRIC_TYPE = "metric";
  public static final String TAGK_TYPE = "tagk";
  public static final String TAGV_TYPE = "tagv";
  
  private final TSDB tsdb;
  private final String id;
  private final TimeSeriesDataStore data_store;
  private final UniqueIdStore uid_store;
  
  private final UniqueId metrics;
  private final UniqueId tag_names;
  private final UniqueId tag_values;
  
  protected int metric_width;
  protected int tagk_width;
  protected int tagv_width;
  protected int salt_buckets;
  protected int salt_width;
  
  protected Map<TypeToken<?>, Codec> codecs;
  
  public Schema(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = id;
    setConfig();
    
    String key = configKey("data.store");
    final String store_name = tsdb.getConfig().getString(key);
    
    final TimeSeriesDataStoreFactory store_factory = 
        (TimeSeriesDataStoreFactory) tsdb.getRegistry()
          .getPlugin(TimeSeriesDataStoreFactory.class, store_name);
    if (store_factory == null) {
      throw new ConfigurationException("No factory found for: " + store_name);
    }
    data_store = store_factory.newInstance(tsdb, id);
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
    
    codecs = Maps.newHashMapWithExpectedSize(1);
    codecs.put(NumericType.TYPE, new NumericCodec());
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
        if (child != null) {
          child.setErrorTags()
            .log("Exception", 
                (ex instanceof DeferredGroupException) ? 
                    Exceptions.getCause((DeferredGroupException) ex) : ex)
            .finish();
        }
        throw new StorageException("Failed to fetch IDs.", 
            (ex instanceof DeferredGroupException) ? 
                Exceptions.getCause((DeferredGroupException) ex) : ex);
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
  
  @Override
  public Deferred<TimeSeriesStringId> resolveByteId(final TimeSeriesByteId id,
                                                    final Span span) {
    // TODO Auto-generated method stub
    return null;
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
    // TODO - implement
  }
  
  public void prefixKeyWithSalt(final byte[] row_key, final int bucket) {
    // TODO - implement
  }
  
  public RollupConfig rollupConfig() {
    // TODO - implement
    return null;
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
  
  public boolean timelessSalting() {
    // TODO - implement
    return true;
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
    // TODO - implement
    return null;
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
  }
  
  @VisibleForTesting
  TimeSeriesDataStore dataStore() {
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
  
}
