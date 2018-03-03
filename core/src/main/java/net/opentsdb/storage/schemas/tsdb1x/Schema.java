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

import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.configuration.ConfigurationException;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.StorageSchema;
import net.opentsdb.storage.TimeSeriesDataStore;
import net.opentsdb.storage.TimeSeriesDataStoreFactory;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdFactory;
import net.opentsdb.uid.UniqueIdStore;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Bytes;

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

  /** Number of bytes on which a timestamp is encoded.  */
  public static final short TIMESTAMP_BYTES = 4;
  
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
   * @return A non-null timestamp object.
   * @throws IllegalArgumentException if the key was null or encoded 
   * improperly.
   */
  public TimeStamp baseTimestamp(final byte[] key) {
    if (Bytes.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    if (key.length < salt_width + metric_width + TIMESTAMP_BYTES) {
      throw new IllegalArgumentException("Key was too short.");
    }
    return new MillisecondTimeStamp(Bytes.getUnsignedInt(key, 
        salt_width + metric_width) * 1000);
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
    // TODO
    return null;
  }
  
  /**
   * Converts the given string to it's UID value based on the type.
   * @param type A non-null UID type.
   * @param id A non-null and non-empty string.
   * @param span An optional tracing span.
   * @return A deferred resolving to the UID if successful or an exception.
   * @throws IllegalArgumentException if the type was null or the string
   * was null or empty.
   */
  public Deferred<byte[]> stringToId(final UniqueIdType type, 
                                     final String id,
                                     final Span span) {
    // TODO
    return null;
  }
  
  /**
   * Converts the list of strings to their IDs, maintaining order.
   * @param type A non-null UID type.
   * @param ids A non-null and non-empty list of strings.
   * @param span An optional tracing span.
   * @return A deferred resolving to the list of UIDs in order if 
   * successful or an exception.
   * @throws IllegalArgumentException if the type was null or the
   * IDs was null or an ID in the list was null or empty.
   */
  public Deferred<List<byte[]>> stringsToId(final UniqueIdType type, 
                                            final List<String> ids,
                                            final Span span) {
    // TODO
    return null;
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
  public Deferred<String> idToString(final UniqueIdType type, 
                                     final byte[] id,
                                     final Span span) {
    // TODO
    return null;
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
  public Deferred<List<String>> idsToString(final UniqueIdType type, 
                                            final List<byte[]> ids,
                                            final Span span) {
    // TODO
    return null;
  }

  @Override
  public String id() {
    return id;
  }
  
  @Override
  public Deferred<TimeSeriesStringId> resolveByteId(TimeSeriesByteId id,
                                                    Span span) {
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
