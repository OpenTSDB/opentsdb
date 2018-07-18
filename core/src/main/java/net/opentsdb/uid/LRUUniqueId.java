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
package net.opentsdb.uid;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import net.opentsdb.auth.AuthState;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesDatumId;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.StorageException;
import net.opentsdb.utils.Bytes;

/**
 * 
 * TODO - add a negative cache
 *
 */
public class LRUUniqueId implements UniqueId, TimerTask {
  private static final Logger LOG = LoggerFactory.getLogger(
      LRUUniqueId.class);
  
  /** Cache for forward mappings (name to ID). */
  private final Cache<String, byte[]> name_cache;
  
  /** Cache for backward mappings (ID to name).
   * The ID in the key is a byte[] converted to a String to be Comparable. */
  private final Cache<String, String> id_cache;
  
  private final TSDB tsdb;
  
  /** The underlying data store implementation to call on cache miss. */
  private final UniqueIdStore store;
  
  
  private final UniqueIdType type;
  private final CacheMode mode;
  private final String id;
  
  public LRUUniqueId(final TSDB tsdb, 
                     final String id, 
                     final UniqueIdType type, 
                     final UniqueIdStore store) {
    this.tsdb = tsdb;
    this.store = store;
    this.id = id;
    this.type = type;
    
    final String prefix = "tsd.uid." + id + "." + 
        type.toString().toLowerCase(); 
    
    String key = prefix + ".mode";
    if (!tsdb.getConfig().hasProperty(key)) {
      tsdb.getConfig().register(key, "read_write", false, 
          "The mode of the cache, i.e. if it caches both forward and "
          + "reverse mappings or only one of those two. Values "
          + "supported include 'write_only', 'read_only' and 'read_write'.");
    }
    mode = CacheMode.fromString(tsdb.getConfig().getString(key));
    
    // record_stats are global
    key = "tsd.uid." + id + ".record_stats";
    if (!tsdb.getConfig().hasProperty(key)) {
      tsdb.getConfig().register(key, true, false, 
          "Whether or not to track statistics about cache hits, misses, etc.");
    }
    final boolean record_stats = tsdb.getConfig().getBoolean(key);
    
    key = prefix + ".lru.name.size";
    if (!tsdb.getConfig().hasProperty(key)) {
      tsdb.getConfig().register(key, 1048576L, false, 
          "Number of items to maintain in the name to UID cache.");
    }
    final long name_size = tsdb.getConfig().getLong(key);
    
    // id stats
    key = prefix + ".lru.id.size";
    if (!tsdb.getConfig().hasProperty(key)) {
      tsdb.getConfig().register(key, 1048576L, false, 
          "Number of items to maintain in the UID to name cache.");
    }
    final long id_size = tsdb.getConfig().getLong(key);
    
    // TODO - only one or the other depending on mode
    @SuppressWarnings("rawtypes")
    CacheBuilder builder = CacheBuilder.newBuilder()
        .maximumSize(name_size);
    if (record_stats) {
      builder = builder.recordStats();
    }
    name_cache = builder.<String, byte[]>build();
    
    builder = CacheBuilder.newBuilder()
        .maximumSize(id_size);
    if (record_stats) {
      builder = builder.recordStats();
    }
    id_cache = builder.<String, String>build();
    
    tsdb.getMaintenanceTimer().newTimeout(this, 
        tsdb.getConfig().getInt(DefaultTSDB.MAINT_TIMER_KEY), 
        TimeUnit.MILLISECONDS);
    
    LOG.info("Initialized LRU UniqueId cache for [" + type 
        + "] with a name limit of " + name_size 
        + " and an Id limit of " + id_size + " and mode " + mode);
  }

  @Override
  public UniqueIdType type() {
    return type;
  }
  
  @Override
  public void dropCaches(final Span span) {
    // TODO - only one or the other depending on mode
    name_cache.invalidateAll();
    id_cache.invalidateAll();
  }
  
  @Override
  public Deferred<String> getName(final byte[] id, final Span span) {
    if (Bytes.isNullOrEmpty(id)) {
      throw new IllegalArgumentException("The ID cannot be null or empty.");
    }
    
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".getName")
          .withTag("type", type.toString())
          .withTag("id", UniqueId.uidToString(id))
          .start();
    } else {
      child = null;
    }
    
    final String name = id_cache.getIfPresent(UniqueId.uidToString(id));
    if (name != null) {
      if (child != null) {
        child.setSuccessTags()
          .setTag("fromCache", "true")
          .finish();
      }
      return Deferred.fromResult(name);
    }
    
    class ErrorCB implements Callback<String, Exception> {
      @Override
      public String call(final Exception ex) throws Exception {
        if (child != null) {
          child.setErrorTags()
            .log("Exception", ex)
            .finish();
        }
        throw ex;
      }
    }
    
    class IdToString implements Callback<String, String> {
      @Override
      public String call(final String name) throws Exception {
        if (!Strings.isNullOrEmpty(name)) {
          switch (mode) {
          case WRITE_ONLY:
            break;
          case READ_ONLY:
            id_cache.put(UniqueId.uidToString(id), name);
            break;
          default:
            id_cache.put(UniqueId.uidToString(id), name);
            name_cache.put(name, id);
          }
        }
        
        if (child != null) {
          child.setSuccessTags()
          .setTag("fromCache", "false")
          .finish();
        }
        return name;
      }
    }
    
    try {
      return store.getName(type, id, child != null ? child : span)
          .addCallbacks(new IdToString(), new ErrorCB());
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
          .log("Exception", e)
          .finish();
      }
      return Deferred.fromError(new StorageException(
          "Unexpected exception from UID storage", e));
    }
  }

  @Override
  public Deferred<List<String>> getNames(final List<byte[]> ids, 
                                         final Span span) {
    if (ids == null || ids.isEmpty()) {
      throw new IllegalArgumentException("ID list cannot be null or empty.");
    }
    
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".getNames")
          .withTag("type", type.toString())
          .withTag("ids", ids.size())
          .start();
    } else {
      child = null;
    }
    
    // need the clone so we can dump cached copies
    final List<byte[]> misses = Lists.newArrayListWithCapacity(ids.size());
    final List<String> names = Lists.newArrayListWithCapacity(ids.size());

    int resolved = 0;
    for (int i = 0; i < ids.size(); i++) {
      final String name = id_cache.getIfPresent(
          UniqueId.uidToString(ids.get(i)));
      if (name != null) {
        names.add(name);
        resolved++;
      } else {
        names.add(null);
        misses.add(ids.get(i));
      }
    }
    
    // if it was a 100% cache hit, we're all done.
    if (resolved == ids.size()) {
      if (child != null) {
        child.setSuccessTags()
          .setTag("fromCache", "true")
          .setTag("cached", ids.size())
          .finish();
      }
      return Deferred.fromResult(names);
    }
    
    class ErrorCB implements Callback<String, Exception> {
      @Override
      public String call(final Exception ex) throws Exception {
        if (child != null) {
          child.setErrorTags()
            .log("Exception", ex)
            .finish();
        }
        throw ex;
      }
    }
    
    class IdsToStrings implements Callback<List<String>, List<String>> {
      @Override
      public List<String> call(final List<String> store_names) throws Exception {
        int ids_idx = 0;
        for (int i = 0; i < misses.size(); i++) {
          final String name = store_names.get(i);
          if (Strings.isNullOrEmpty(name)) {
            // this'll throw an exception later
            ids_idx++;
            continue;
          }
          
          while (Bytes.memcmp(ids.get(ids_idx), misses.get(i)) != 0) {
            ids_idx++;
            if (ids_idx >= ids.size()) {
              throw new IllegalStateException("WENT TOO FAR!");
            }
          }
          
          switch (mode) {
          case WRITE_ONLY:
            break;
          case READ_ONLY:
            id_cache.put(UniqueId.uidToString(misses.get(i)), name);
            break;
          default:
            id_cache.put(UniqueId.uidToString(misses.get(i)), name);
            name_cache.put(name, misses.get(i));
          }
          // always advance the index. If the same tag value is used multiple
          // times in one ID we don't want to be stuck writing to the same
          // index over and over.
          names.set(ids_idx++, name);
        }
        
        if (child != null) {
          child.setSuccessTags()
            .setTag("fromCache", "false")
            .setTag("cached", ids.size() - misses.size())
            .finish();
        }
        return names;
      }
    }
    
    try {
      return store.getNames(type, misses, child != null ? child : span)
          .addCallbacks(new IdsToStrings(), new ErrorCB());
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
          .log("Exception", e)
          .finish();
      }
      return Deferred.fromError(new StorageException(
          "Unexpected exception from UID storage", e));
    }
  }

  @Override
  public Deferred<byte[]> getId(final String name, final Span span) {
    if (Strings.isNullOrEmpty(name)) {
      throw new IllegalArgumentException("Name cannot be null or empty.");
    }
    
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".getId")
          .withTag("type", type.toString())
          .withTag("name", name)
          .start();
    } else {
      child = null;
    }
    
    final byte[] uid = name_cache.getIfPresent(name);
    if (uid != null) {
      if (child != null) {
        child.setSuccessTags()
          .setTag("fromCache", "true")
          .finish();
      }
      return Deferred.fromResult(uid);
    }
    
    class ErrorCB implements Callback<byte[], Exception> {
      @Override
      public byte[] call(final Exception ex) throws Exception {
        if (child != null) {
          child.setErrorTags()
            .log("Exception", ex)
            .finish();
        }
        throw ex;
      }
    }
    
    class StringToId implements Callback<byte[], byte[]> {
      @Override
      public byte[] call(final byte[] uid) throws Exception {
        if (!Bytes.isNullOrEmpty(uid)) {
          switch (mode) {
          case WRITE_ONLY:
            name_cache.put(name, uid);
            break;
          case READ_ONLY:
            break;
          default:
            id_cache.put(UniqueId.uidToString(uid), name);
            name_cache.put(name, uid);
          }
        }
        
        if (child != null) {
          child.setSuccessTags()
          .setTag("fromCache", "false")
          .finish();
        }
        return uid;
      }
    }
    
    try {
      return store.getId(type, name, child != null ? child : span)
          .addCallbacks(new StringToId(), new ErrorCB());
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
          .log("Exception", e)
          .finish();
      }
      return Deferred.fromError(new StorageException(
          "Unexpected exception from UID storage", e));
    }
  }

  @Override
  public Deferred<List<byte[]>> getIds(final List<String> names, 
                                       final Span span) {
    if (names == null || names.isEmpty()) {
      throw new IllegalArgumentException("Names cannot be null or empty.");
    }
    
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".getIds")
          .withTag("type", type.toString())
          .withTag("names", names.size())
          .start();
    } else {
      child = null;
    }
    
    // need the clone so we can dump cached copies
    final List<String> misses = Lists.newArrayListWithCapacity(names.size());
    final List<byte[]> uids = Lists.newArrayListWithCapacity(names.size());
    int resolved = 0;
    for (int i = 0; i < names.size(); i++) {
      final byte[] uid = name_cache.getIfPresent(names.get(i));
      if (uid != null) {
        uids.add(uid);
        resolved++;
      } else {
        uids.add(null);
        misses.add(names.get(i));
      }
    }
    
    // if it was a 100% cache hit, we're all done.
    if (resolved == names.size()) {
      if (child != null) {
        child.setSuccessTags()
          .setTag("fromCache", "true")
          .setTag("cached", uids.size())
          .finish();
      }
      return Deferred.fromResult(uids);
    }
    
    class ErrorCB implements Callback<String, Exception> {
      @Override
      public String call(final Exception ex) throws Exception {
        if (child != null) {
          child.setErrorTags()
            .log("Exception", ex)
            .finish();
        }
        throw ex;
      }
    }
    
    class StringsToIds implements Callback<List<byte[]>, List<byte[]>> {
      @Override
      public List<byte[]> call(final List<byte[]> store_uids) throws Exception {
        int names_idx = 0;
        for (int i = 0; i < misses.size(); i++) {
          final byte[] id = store_uids.get(i);
          if (Bytes.isNullOrEmpty(id)) {
            // this'll throw an exception later
            names_idx++;
            continue;
          }
          
          while (!names.get(names_idx).equals(misses.get(i))) {
            names_idx++;
            if (names_idx >= names.size()) {
              throw new IllegalStateException("WENT TOO FAR!");
            }
          }
          
          switch (mode) {
          case WRITE_ONLY:
            name_cache.put(misses.get(i), id);
            break;
          case READ_ONLY:
            break;
          default:
            name_cache.put(misses.get(i), id);
            id_cache.put(UniqueId.uidToString(id), misses.get(i));
          }
          
          // always advance the index. If the same tag value is used multiple
          // times in one ID we don't want to be stuck writing to the same
          // index over and over.
          uids.set(names_idx++, id);
        }
        
        if (child != null) {
          child.setSuccessTags()
            .setTag("fromCache", "false")
            .setTag("cached", names.size() - misses.size())
            .finish();
        }
        return uids;
      }
    }
    
    try {
      return store.getIds(type, misses, child != null ? child : span)
          .addCallbacks(new StringsToIds(), new ErrorCB());
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
          .log("Exception", e)
          .finish();
      }
      return Deferred.fromError(new StorageException(
          "Unexpected exception from UID storage", e));
    }
  }
  
  @Override
  public Deferred<IdOrError> getOrCreateId(final AuthState auth,
                                           final String name, 
                                           final TimeSeriesDatumId id,
                                           final Span span) {
    if (Strings.isNullOrEmpty(name)) {
      throw new IllegalArgumentException("Name cannot be null or empty.");
    }
    
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".getOrCreateId")
          .withTag("type", type.toString())
          .withTag("name", name)
          .start();
    } else {
      child = null;
    }
    
    final byte[] uid = name_cache.getIfPresent(name);
    if (uid != null) {
      if (child != null) {
        child.setSuccessTags()
             .setTag("fromCache", "true")
             .finish();
      }
      return Deferred.fromResult(IdOrError.wrapId(uid));
    }
    
    class ErrorCB implements Callback<byte[], Exception> {
      @Override
      public byte[] call(final Exception ex) throws Exception {
        if (child != null) {
          child.setErrorTags()
            .log("Exception", ex)
            .finish();
        }
        throw ex;
      }
    }
    
    class CacheCB implements Callback<IdOrError, IdOrError> {
      @Override
      public IdOrError call(final IdOrError response) throws Exception {
        if (response == null) {
          // WTF? 
          return null;
        }
        if (response.id() != null) {
          switch (mode) {
          case WRITE_ONLY:
            name_cache.put(name, response.id());
            break;
          case READ_ONLY:
            break;
          default:
            id_cache.put(UniqueId.uidToString(response.id()), name);
            name_cache.put(name, response.id());
          }
        }
        if (child != null) {
          child.setSuccessTags()
               .setTag("fromCache", "false")
               .finish();
        }
        return response;
      }
    }
    
    try {
      return store.getOrCreateId(auth, type, name, id, child)
          .addCallbacks(new CacheCB(), new ErrorCB());
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
          .log("Exception", e)
          .finish();
      }
      return Deferred.fromError(new StorageException(
          "Unexpected exception from UID storage", e));
    }
  }
  
  @Override
  public Deferred<List<IdOrError>> getOrCreateIds(final AuthState auth,
                                                  final List<String> names, 
                                                  final TimeSeriesDatumId id,
                                                  final Span span) {
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".getOrCreateIds")
          .withTag("type", type.toString())
          .withTag("names", names.size())
          .start();
    } else {
      child = null;
    }
    
    // need the clone so we can dump cached copies
    final List<String> misses = Lists.newArrayListWithCapacity(names.size());
    final List<IdOrError> uids = Lists.newArrayListWithCapacity(names.size());
    int resolved = 0;
    for (int i = 0; i < names.size(); i++) {
      final byte[] uid = name_cache.getIfPresent(names.get(i));
      if (uid != null) {
        uids.add(IdOrError.wrapId(uid));
        resolved++;
      } else {
        uids.add(null);
        misses.add(names.get(i));
      }
    }
    
    // if it was a 100% cache hit, we're all done.
    if (resolved == names.size()) {
      if (child != null) {
        child.setSuccessTags()
          .setTag("fromCache", "true")
          .setTag("cached", uids.size())
          .finish();
      }
      return Deferred.fromResult(uids);
    }
    
    class ErrorCB implements Callback<String, Exception> {
      @Override
      public String call(final Exception ex) throws Exception {
        if (child != null) {
          child.setErrorTags()
            .log("Exception", ex)
            .finish();
        }
        throw ex;
      }
    }
    
    class CacheCB implements Callback<List<IdOrError>, List<IdOrError>> {
      @Override
      public List<IdOrError> call(final List<IdOrError> response) throws Exception {
        Iterator<IdOrError> iterator = response.iterator();
        for (int i = 0; i < uids.size(); i++) {
          if (uids.get(i) == null) {
            final IdOrError id_or_error = iterator.next();
            if (id_or_error.id() != null) {
              switch (mode) {
              case WRITE_ONLY:
                name_cache.put(names.get(i), id_or_error.id());
                break;
              case READ_ONLY:
                break;
              default:
                name_cache.put(names.get(i), id_or_error.id());
                id_cache.put(UniqueId.uidToString(id_or_error.id()), names.get(i));
              }
            }
            
            // always advance the index. If the same tag value is used multiple
            // times in one ID we don't want to be stuck writing to the same
            // index over and over.
            uids.set(i, id_or_error);
          }
        }
        
        if (child != null) {
          child.setSuccessTags()
            .setTag("fromCache", "false")
            .setTag("cached", names.size() - misses.size())
            .finish();
        }
        
        return uids;
      }
    }
    
    try {
    return store.getOrCreateIds(auth, type, misses, id, child)
        .addCallbacks(new CacheCB(), new ErrorCB());
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
          .log("Exception", e)
          .finish();
      }
      return Deferred.fromError(new StorageException(
          "Unexpected exception from UID storage", e));
    }
  }

  @Override
  public Deferred<List<String>> suggest(String search, int max_results, final Span span) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Object> rename(String oldname, String newname, final Span span) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Object> delete(final String name, final Span span) {
    // TODO Auto-generated method stub
    return null;
  }
  
  @VisibleForTesting
  Cache<String, byte[]> nameCache() {
    return name_cache;
  }
  
  @VisibleForTesting
  Cache<String, String> idCache() {
    return id_cache;
  }

  
  @Override
  public void run(final Timeout ignored) throws Exception {
    String id = this.id == null ? "default" : this.id;
    try {
      if (name_cache != null) {
        final CacheStats stats = name_cache.stats();
        tsdb.getStatsCollector().setGauge("uid.cache.guava.lru.requestCount", 
            stats.requestCount(), "uid", type.toString(), "type", "name", "id", id);
        tsdb.getStatsCollector().setGauge("uid.cache.guava.lru.hitCount", 
            stats.hitCount(), "uid", type.toString(), "type", "name", "id", id);
        tsdb.getStatsCollector().setGauge("uid.cache.guava.lru.hitRate", 
            stats.hitRate(), "uid", type.toString(), "type", "name", "id", id);
        tsdb.getStatsCollector().setGauge("uid.cache.guava.lru.missCount", 
            stats.missCount(), "uid", type.toString(), "type", "name", "id", id);
        tsdb.getStatsCollector().setGauge("uid.cache.guava.lru.missRate", 
            stats.missRate(), "uid", type.toString(), "type", "name", "id", id);
        tsdb.getStatsCollector().setGauge("uid.cache.guava.lru.evictionCount", 
            stats.evictionCount(), "uid", type.toString(), "type", "name", "id", id);
      }
      
      if (id_cache != null) {
        final CacheStats stats = id_cache.stats();
        tsdb.getStatsCollector().setGauge("uid.cache.guava.lru.requestCount", 
            stats.requestCount(), "uid", type.toString(), "type", "uid", "id", id);
        tsdb.getStatsCollector().setGauge("uid.cache.guava.lru.hitCount", 
            stats.hitCount(), "uid", type.toString(), "type", "uid", "id", id);
        tsdb.getStatsCollector().setGauge("uid.cache.guava.lru.hitRate", 
            stats.hitRate(), "uid", type.toString(), "type", "uid", "id", id);
        tsdb.getStatsCollector().setGauge("uid.cache.guava.lru.missCount", 
            stats.missCount(), "uid", type.toString(), "type", "uid", "id", id);
        tsdb.getStatsCollector().setGauge("uid.cache.guava.lru.missRate", 
            stats.missRate(), "uid", type.toString(), "type", "uid", "id", id);
        tsdb.getStatsCollector().setGauge("uid.cache.guava.lru.evictionCount", 
            stats.evictionCount(), "uid", type.toString(), "type", "uid", "id", id);
      }
    } catch (Exception e) {
      LOG.error("Unexpected exception recording LRU stats", e);
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Wrote stats for UID cache " + type + " and ID " + id);
    }
    tsdb.getMaintenanceTimer().newTimeout(this, 
        tsdb.getConfig().getInt(DefaultTSDB.MAINT_TIMER_KEY), 
        TimeUnit.MILLISECONDS);
  }
}
