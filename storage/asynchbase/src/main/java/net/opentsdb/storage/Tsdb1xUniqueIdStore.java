// This file is part of OpenTSDB.
// Copyright (C) 2010-2018  The OpenTSDB Authors.
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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import javax.xml.bind.DatatypeConverter;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.hbase.async.GetRequest;
import org.hbase.async.GetResultOrException;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Const;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.filter.TagVLiteralOrFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.stats.Span;
import net.opentsdb.stats.TsdbTrace;
import net.opentsdb.storage.schemas.tsdb1x.ResolvedFilter;
import net.opentsdb.uid.UniqueIdStore;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Bytes;

/**
 * Represents a table of Unique IDs, manages the lookup and creation of IDs.
 * <p>
 * Don't attempt to use {@code equals()} or {@code hashCode()} on
 * this class.
 * 
 * @since 1.0
 */
public class Tsdb1xUniqueIdStore implements UniqueIdStore {
  private static final Logger LOG = LoggerFactory.getLogger(Tsdb1xUniqueIdStore.class);
  
  public static final String CHARACTER_SET_KEY = "character_set";
  public static final String CHARACTER_SET_DEFAULT = "ISO-8859-1";
  
  public static final byte[] METRICS_QUAL = 
      "metrics".getBytes(Const.ASCII_CHARSET);
  public static final byte[] TAG_NAME_QUAL = 
      "tagk".getBytes(Const.ASCII_CHARSET);
  public static final byte[] TAG_VALUE_QUAL = 
      "tagv".getBytes(Const.ASCII_CHARSET);

//  /** Charset used to convert Strings to byte arrays and back. */
//  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  /** The single column family used by this class. */
  public static final byte[] ID_FAMILY = "id".getBytes(Const.ASCII_CHARSET);
  /** The single column family used by this class. */
  public static final byte[] NAME_FAMILY = "name".getBytes(Const.ASCII_CHARSET);
//  /** Row key of the special row used to track the max ID already assigned. */
//  private static final byte[] MAXID_ROW = { 0 };
//  /** How many time do we try to assign an ID before giving up. */
//  private static final short MAX_ATTEMPTS_ASSIGN_ID = 3;
//  /** How many time do we try to apply an edit before giving up. */
//  private static final short MAX_ATTEMPTS_PUT = 6;
//  /** How many time do we try to assign a random ID before giving up. */
//  private static final short MAX_ATTEMPTS_ASSIGN_RANDOM_ID = 10;
//  /** Initial delay in ms for exponential backoff to retry failed RPCs. */
//  private static final short INITIAL_EXP_BACKOFF_DELAY = 800;
//  /** Maximum number of results to return in suggest(). */
//  private static final short MAX_SUGGESTIONS = 25;
  
  private final Tsdb1xHBaseDataStore data_store; 
  
  private final Charset character_set;
  
  private final Map<UniqueIdType, LongAdder> rejected_assignments;
  
  private final Map<UniqueIdType, Map<String, Deferred<byte[]>>> pending_assignments;

  public Tsdb1xUniqueIdStore(final Tsdb1xHBaseDataStore data_store) {
    this.data_store = data_store;
    if (!data_store.tsdb().getConfig().hasProperty(
        data_store.getConfigKey(CHARACTER_SET_KEY))) {
      data_store.tsdb().getConfig().register(
          data_store.getConfigKey(CHARACTER_SET_KEY), 
          CHARACTER_SET_DEFAULT, 
          false, 
          "The character set used for encoding/decoding all strings "
              + "to UIDs.");
    }
    character_set = Charset.forName(data_store.tsdb().getConfig()
        .getString(data_store.getConfigKey(CHARACTER_SET_KEY)));
    
    rejected_assignments = Maps.newHashMapWithExpectedSize(
        UniqueIdType.values().length);
    pending_assignments = Maps.newHashMapWithExpectedSize(
        UniqueIdType.values().length);
    
    for (final UniqueIdType type : UniqueIdType.values()) {
      rejected_assignments.put(type, new LongAdder());
      pending_assignments.put(type, Maps.newConcurrentMap());
    }
//    this.client = client;
//    this.table = table;
//    if (kind.isEmpty()) {
//      throw new IllegalArgumentException("Empty string as 'kind' argument!");
//    }
//    this.kind = toBytes(kind);
//    type = stringToUniqueIdType(kind);
//    if (width < 1 || width > 8) {
//      throw new IllegalArgumentException("Invalid width: " + width);
//    }
//    this.id_width = (short) width;
//    this.randomize_id = randomize_id;
    
    LOG.info("Initalized UniqueId store with characterset: " 
        + character_set);
  }
//  
//  /**
//   * Constructor.
//   * @param tsdb The TSDB this UID object belongs to
//   * @param table The name of the HBase table to use.
//   * @param kind The kind of Unique ID this instance will deal with.
//   * @param width The number of bytes on which Unique IDs should be encoded.
//   * @param Whether or not to randomize new UIDs
//   * @throws IllegalArgumentException if width is negative or too small/large
//   * or if kind is an empty string.
//   * @since 2.3
//   */
//  public UniqueId(final TSDB tsdb, final byte[] table, final String kind,
//                  final int width, final boolean randomize_id) {
//    this.client = tsdb.getClient();
//    this.tsdb = tsdb;
//    this.table = table;
//    if (kind.isEmpty()) {
//      throw new IllegalArgumentException("Empty string as 'kind' argument!");
//    }
//    this.kind = toBytes(kind);
//    type = stringToUniqueIdType(kind);
//    if (width < 1 || width > 8) {
//      throw new IllegalArgumentException("Invalid width: " + width);
//    }
//    this.id_width = (short) width;
//    this.randomize_id = randomize_id;
//  }
//
//  /** The number of times we avoided reading from HBase thanks to the cache. */
//  public int cacheHits() {
//    return cache_hits;
//  }
//
//  /** The number of times we had to read from HBase and populate the cache. */
//  public int cacheMisses() {
//    return cache_misses;
//  }
//
//  /** Returns the number of elements stored in the internal cache. */
//  public int cacheSize() {
//    return name_cache.size() + id_cache.size();
//  }
//
//  /** Returns the number of random UID collisions */
//  public int randomIdCollisions() {
//    return random_id_collisions;
//  }
//  
//  /** Returns the number of UID assignments rejected by the filter */
//  public int rejectedAssignments() {
//    return rejected_assignments;
//  }
//  public short width() {
//    return id_width;
//  }
//
//  /** @param tsdb Whether or not to track new UIDMeta objects */
//  public void setTSDB(final TSDB tsdb) {
//    this.tsdb = tsdb;
//  }
//  
//  /** The largest possible ID given the number of bytes the IDs are 
//   * represented on.
//   * @deprecated Use {@link Internal.getMaxUnsignedValueOnBytes}
//   */
//  public long maxPossibleId() {
//    return Internal.getMaxUnsignedValueOnBytes(id_width);
//  }
//  
//  /**
//   * Finds the name associated with a given ID.
//   *
//   * @param id The ID associated with that name.
//   * @see #getId(String)
//   * @see #getOrCreateIdAsync(String)
//   * @throws NoSuchUniqueId if the given ID is not assigned.
//   * @throws HBaseException if there is a problem communicating with HBase.
//   * @throws IllegalArgumentException if the ID given in argument is encoded
//   * on the wrong number of bytes.
//   * @since 1.1
//   */
//  public Deferred<String> getNameAsync(final byte[] id) {
//    if (id.length != id_width) {
//      throw new IllegalArgumentException("Wrong id.length = " + id.length
//                                         + " which is != " + id_width
//                                         + " required for '" + kind() + '\'');
//    }
//    final String name = getNameFromCache(id);
//    if (name != null) {
//      cache_hits++;
//      return Deferred.fromResult(name);
//    }
//    cache_misses++;
//    class GetNameCB implements Callback<String, String> {
//      public String call(final String name) {
//        if (name == null) {
//          throw new NoSuchUniqueId(kind(), id);
//        }
//        addNameToCache(id, name);
//        addIdToCache(name, id);        
//        return name;
//      }
//    }
//    return getNameFromHBase(id).addCallback(new GetNameCB());
//  }
//
//  private String getNameFromCache(final byte[] id) {
//    return id_cache.get(fromBytes(id));
//  }
//
  @Override
  public Deferred<String> getName(final UniqueIdType type, 
                                  final byte[] id,
                                  final Span span) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (Bytes.isNullOrEmpty(id)) {
      throw new IllegalArgumentException("ID cannot be null or empty.");
    }
    
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".getName")
          .withTag("dataStore", data_store.id())
          .withTag("type", type.toString())
          .withTag("id", net.opentsdb.uid.UniqueId.uidToString(id))
          .start();
    } else {
      child = null;
    }
    
    class ErrorCB implements Callback<String, Exception> {
      @Override
      public String call(final Exception ex) throws Exception {
        if (child != null) {
          child.setErrorTags()
            .log("Exception", ex)
            .finish();
        }
        throw new StorageException("Failed to fetch name.", ex);
      }
    }
    
    class NameFromHBaseCB implements Callback<String, byte[]> {
      @Override
      public String call(final byte[] name) {
        if (child != null) {
          child.setSuccessTags()
            .finish();
        }
        return name == null ? null : new String(name, character_set);
      }
    }
    
    try {
      return hbaseGet(type, id, NAME_FAMILY)
          .addCallbacks(new NameFromHBaseCB(), new ErrorCB());
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
          .log("Exception", e)
          .finish();
      }
      return Deferred.fromError(new StorageException(
          "Unexpected exception from storage", e));
    }
  }
  
  @Override
  public Deferred<List<String>> getNames(final UniqueIdType type, 
                                         final List<byte[]> ids,
                                         final Span span) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (ids == null || ids.isEmpty()) {
      throw new IllegalArgumentException("IDs cannot be null or empty.");
    }
    
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".getNames")
          .withTag("dataStore", data_store.id())
          .withTag("type", type.toString())
          //.withTag("ids", /* TODO - an array to hex method */ "")
          .start();
    } else {
      child = null;
    }
    
    final byte[] qualifier;
    switch(type) {
    case METRIC:
      qualifier = METRICS_QUAL;
      break;
    case TAGK:
      qualifier = TAG_NAME_QUAL;
      break;
    case TAGV:
      qualifier = TAG_VALUE_QUAL;
      break;
    default:
      throw new IllegalArgumentException("This data store does not "
          + "handle Unique IDs of type: " + type);
    }
    
    final List<GetRequest> requests = Lists.newArrayListWithCapacity(ids.size());
    for (final byte[] id : ids) {
      if (Bytes.isNullOrEmpty(id)) {
        throw new IllegalArgumentException("A null or empty ID was "
            + "found in the list.");
      }
      requests.add(new GetRequest(data_store.uidTable(), 
                                  id, 
                                  NAME_FAMILY, 
                                  qualifier));
    }
    
    class ErrorCB implements Callback<List<byte[]>, Exception> {
      @Override
      public List<byte[]> call(Exception ex) throws Exception {
        if (child != null) {
          child.setErrorTags()
            .log("Exception", ex)
            .finish();
        }
        throw new StorageException("Failed to fetch names.", ex);
      }
    }
    
    class ResultCB implements Callback<List<String>, List<GetResultOrException>> {
      @Override
      public List<String> call(final List<GetResultOrException> results)
          throws Exception {
        if (results == null) {
          throw new StorageException("Result list returned was null");
        }
        if (results.size() != ids.size()) {
          throw new StorageException("WTF? Result size was: " 
              + results.size() + " when the names size was: " 
              + ids.size() + ". Should never happen!");
        }
        
        final List<String> names = Lists.newArrayListWithCapacity(results.size());
        for (int i = 0; i < results.size(); i++) {
          if (results.get(i).getException() != null) {
            if (child != null) {
              child.setErrorTags()
                .log("Exception", results.get(i).getException())
                .finish();
            }
            throw new StorageException("UID resolution failed for ID " 
                + ids.get(i), results.get(i).getException());
          } else if (results.get(i).getCells() == null) {
            names.add(null);
          } else {
            names.add(new String(results.get(i).getCells().get(0).value(), 
                character_set));
          }
        }
        
        if (child != null) {
          child.setSuccessTags()
            .finish();
        }
        return names;
      }
    }
    
    try {
      return data_store.client().get(requests)
          .addCallbacks(new ResultCB(), new ErrorCB());
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
          .log("Exception", e)
          .finish();
      }
      return Deferred.fromError(new StorageException(
          "Unexpected exception from storage", e));
    }
  }
//
//  private void addNameToCache(final byte[] id, final String name) {
//    final String key = fromBytes(id);
//    String found = id_cache.get(key);
//    if (found == null) {
//      found = id_cache.putIfAbsent(key, name);
//    }
//    if (found != null && !found.equals(name)) {
//      throw new IllegalStateException("id=" + Arrays.toString(id) + " => name="
//          + name + ", already mapped to " + found);
//    }
//  }
//  public Deferred<byte[]> getIdAsync(final String name) {
//    final byte[] id = getIdFromCache(name);
//    if (id != null) {
//      cache_hits++;
//      return Deferred.fromResult(id);
//    }
//    cache_misses++;
//    class GetIdCB implements Callback<byte[], byte[]> {
//      public byte[] call(final byte[] id) {
//        if (id == null) {
//          throw new NoSuchUniqueName(kind(), name);
//        }
//        if (id.length != id_width) {
//          throw new IllegalStateException("Found id.length = " + id.length
//                                          + " which is != " + id_width
//                                          + " required for '" + kind() + '\'');
//        }
//        addIdToCache(name, id);
//        addNameToCache(id, name);
//        return id;
//      }
//    }
//    Deferred<byte[]> d = getIdFromHBase(name).addCallback(new GetIdCB());
//    return d;
//  }

  @Override
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
          return getIds(UniqueIdType.TAGV, tags, 
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
      deferreds.add(getId(UniqueIdType.TAGK, f.getTagk(), 
          child != null ? child : span)
          .addCallbackDeferring(new TagKCB(i, f)));
    }
    
    /** Error callback to cast the exception to a StorageException */
    class ErrorCB implements Callback<List<ResolvedFilter>, Exception> {
      @Override
      public List<ResolvedFilter> call(final Exception ex) throws Exception {
        if (child != null) {
          child.setErrorTags()
            .log("Exception", ex)
            .finish();
        }
        throw new StorageException("Failed to fetch IDs.", ex);
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
  
  @Override
  public Deferred<byte[]> getId(final UniqueIdType type, 
                                final String name,
                                final Span span) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (Strings.isNullOrEmpty(name)) {
      throw new IllegalArgumentException("Name cannot be null or empty.");
    }
    
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".getId")
          .withTag("dataStore", data_store.id())
          .withTag("type", type.toString())
          .withTag("name", name)
          .start();
    } else {
      child = null;
    }
    
    class ErrorCB implements Callback<byte[], Exception> {
      @Override
      public byte[] call(final Exception ex) throws Exception {
        if (child != null) {
          child.setErrorTags()
            .log("Exception", ex)
            .finish();
        }
        throw new StorageException("Failed to fetch ID.", ex);
      }
    }
    
    class SpanCB implements Callback<byte[], byte[]> {
      @Override
      public byte[] call(final byte[] uid) throws Exception {
        child.setSuccessTags()
          .finish();
        return uid;
      }
    }

    try {
      if (child != null) {
        return hbaseGet(type, name.getBytes(character_set), ID_FAMILY)
            .addCallbacks(new SpanCB(), new ErrorCB());
      }
      return hbaseGet(type, name.getBytes(character_set), ID_FAMILY)
          .addErrback(new ErrorCB());
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
          .log("Exception", e)
          .finish();
      }
      return Deferred.fromError(new StorageException(
          "Unexpected exception from storage", e));
    }
  }
  
  @Override
  public Deferred<List<byte[]>> getIds(final UniqueIdType type, 
                                       final List<String> names,
                                       final Span span) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (names == null || names.isEmpty()) {
      throw new IllegalArgumentException("Names cannot be null or empty.");
    }
    
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".getIds")
          .withTag("dataStore", data_store.id())
          .withTag("type", type.toString())
          .withTag("names", names.toString())
          .start();
    } else {
      child = null;
    }
    
    final byte[] qualifier;
    switch(type) {
    case METRIC:
      qualifier = METRICS_QUAL;
      break;
    case TAGK:
      qualifier = TAG_NAME_QUAL;
      break;
    case TAGV:
      qualifier = TAG_VALUE_QUAL;
      break;
    default:
      throw new IllegalArgumentException("This data store does not "
          + "handle Unique IDs of type: " + type);
    }
    
    final List<GetRequest> requests = Lists.newArrayListWithCapacity(names.size());
    for (final String name : names) {
      if (Strings.isNullOrEmpty(name)) {
        throw new IllegalArgumentException("A null or empty name was "
            + "found in the list.");
      }
      requests.add(new GetRequest(data_store.uidTable(), 
                                  name.getBytes(character_set), 
                                  ID_FAMILY, 
                                  qualifier));
    }
    
    class ErrorCB implements Callback<List<byte[]>, Exception> {
      @Override
      public List<byte[]> call(Exception ex) throws Exception {
        if (child != null) {
          child.setErrorTags()
            .log("Exception", ex)
            .finish();
        }
        throw new StorageException("Failed to fetch ID.", ex);
      }
    }
    
    class ResultCB implements Callback<List<byte[]>, List<GetResultOrException>> {
      @Override
      public List<byte[]> call(final List<GetResultOrException> results)
          throws Exception {
        if (results == null) {
          throw new StorageException("Result list returned was null");
        }
        if (results.size() != names.size()) {
          throw new StorageException("WTF? Result size was: " 
              + results.size() + " when the names size was: " 
              + names.size() + ". Should never happen!");
        }
        
        final List<byte[]> uids = Lists.newArrayListWithCapacity(results.size());
        for (int i = 0; i < results.size(); i++) {
          if (results.get(i).getException() != null) {
            if (child != null) {
              child.setErrorTags()
                .log("Exception", results.get(i).getException())
                .finish();
            }
            throw new StorageException("UID resolution failed for name " 
                + names.get(i), results.get(i).getException());
          } else if (results.get(i).getCells() != null) {
            uids.add(results.get(i).getCells().get(0).value());
          } else {
            uids.add(null);
          }
        }
        
        if (child != null) {
          child.setSuccessTags()
            .finish();
        }
        return uids;
      }
    }
    
    try {
      return data_store.client().get(requests)
          .addCallbacks(new ResultCB(), new ErrorCB());
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
          .log("Exception", e)
          .finish();
      }
      return Deferred.fromError(new StorageException(
          "Unexpected exception from storage", e));
    }
  }
  
//
//  private void addIdToCache(final String name, final byte[] id) {
//    byte[] found = name_cache.get(name);
//    if (found == null) {
//      found = name_cache.putIfAbsent(name,
//                                    // Must make a defensive copy to be immune
//                                    // to any changes the caller may do on the
//                                    // array later on.
//                                    Arrays.copyOf(id, id.length));
//    }
//    if (found != null && !Arrays.equals(found, id)) {
//      throw new IllegalStateException("name=" + name + " => id="
//          + Arrays.toString(id) + ", already mapped to "
//          + Arrays.toString(found));
//    }
//  }
//
//  /**
//   * Implements the process to allocate a new UID.
//   * This callback is re-used multiple times in a four step process:
//   *   1. Allocate a new UID via atomic increment.
//   *   2. Create the reverse mapping (ID to name).
//   *   3. Create the forward mapping (name to ID).
//   *   4. Return the new UID to the caller.
//   */
//  private final class UniqueIdAllocator implements Callback<Object, Object> {
//    private final String name;  // What we're trying to allocate an ID for.
//    private final Deferred<byte[]> assignment; // deferred to call back
//    private short attempt = randomize_id ?     // Give up when zero.
//        MAX_ATTEMPTS_ASSIGN_RANDOM_ID : MAX_ATTEMPTS_ASSIGN_ID;
//
//    private HBaseException hbe = null;  // Last exception caught.
//    // TODO(manolama) - right now if we retry the assignment it will create a 
//    // callback chain MAX_ATTEMPTS_* long and call the ErrBack that many times.
//    // This can be cleaned up a fair amount but it may require changing the 
//    // public behavior a bit. For now, the flag will prevent multiple attempts
//    // to execute the callback.
//    private boolean called = false; // whether we called the deferred or not
//
//    private long id = -1;  // The ID we'll grab with an atomic increment.
//    private byte row[];    // The same ID, as a byte array.
//
//    private static final byte ALLOCATE_UID = 0;
//    private static final byte CREATE_REVERSE_MAPPING = 1;
//    private static final byte CREATE_FORWARD_MAPPING = 2;
//    private static final byte DONE = 3;
//    private byte state = ALLOCATE_UID;  // Current state of the process.
//
//    UniqueIdAllocator(final String name, final Deferred<byte[]> assignment) {
//      this.name = name;
//      this.assignment = assignment;
//    }
//
//    Deferred<byte[]> tryAllocate() {
//      attempt--;
//      state = ALLOCATE_UID;
//      call(null);
//      return assignment;
//    }
//
//    @SuppressWarnings("unchecked")
//    public Object call(final Object arg) {
//      if (attempt == 0) {
//        if (hbe == null && !randomize_id) {
//          throw new IllegalStateException("Should never happen!");
//        }
//        LOG.error("Failed to assign an ID for kind='" + kind()
//                  + "' name='" + name + "'", hbe);
//        if (hbe == null) {
//          throw new FailedToAssignUniqueIdException(kind(), name, 
//              MAX_ATTEMPTS_ASSIGN_RANDOM_ID);
//        }
//        throw hbe;
//      }
//
//      if (arg instanceof Exception) {
//        final String msg = ("Failed attempt #" + (randomize_id
//                         ? (MAX_ATTEMPTS_ASSIGN_RANDOM_ID - attempt) 
//                         : (MAX_ATTEMPTS_ASSIGN_ID - attempt))
//                         + " to assign an UID for " + kind() + ':' + name
//                         + " at step #" + state);
//        if (arg instanceof HBaseException) {
//          LOG.error(msg, (Exception) arg);
//          hbe = (HBaseException) arg;
//          attempt--;
//          state = ALLOCATE_UID;;  // Retry from the beginning.
//        } else {
//          LOG.error("WTF?  Unexpected exception!  " + msg, (Exception) arg);
//          return arg;  // Unexpected exception, let it bubble up.
//        }
//      }
//
//      class ErrBack implements Callback<Object, Exception> {
//        public Object call(final Exception e) throws Exception {
//          if (!called) {
//            LOG.warn("Failed pending assignment for: " + name, e);
//            assignment.callback(e);
//            called = true;
//          }
//          return assignment;
//        }
//      }
//      
//      final Deferred d;
//      switch (state) {
//        case ALLOCATE_UID:
//          d = allocateUid();
//          break;
//        case CREATE_REVERSE_MAPPING:
//          d = createReverseMapping(arg);
//          break;
//        case CREATE_FORWARD_MAPPING:
//          d = createForwardMapping(arg);
//          break;
//        case DONE:
//          return done(arg);
//        default:
//          throw new AssertionError("Should never be here!");
//      }
//      return d.addBoth(this).addErrback(new ErrBack());
//    }
//
//    /** Generates either a random or a serial ID. If random, we need to
//     * make sure that there isn't a UID collision.
//     */
//    private Deferred<Long> allocateUid() {
//      LOG.info("Creating " + (randomize_id ? "a random " : "an ") + 
//          "ID for kind='" + kind() + "' name='" + name + '\'');
//
//      state = CREATE_REVERSE_MAPPING;
//      if (randomize_id) {
//        return Deferred.fromResult(RandomUniqueId.getRandomUID());
//      } else {
//        return client.atomicIncrement(new AtomicIncrementRequest(table, 
//                                      MAXID_ROW, ID_FAMILY, kind));
//      }
//    }
//
//    /**
//     * Create the reverse mapping.
//     * We do this before the forward one so that if we die before creating
//     * the forward mapping we don't run the risk of "publishing" a
//     * partially assigned ID.  The reverse mapping on its own is harmless
//     * but the forward mapping without reverse mapping is bad as it would
//     * point to an ID that cannot be resolved.
//     */
//    private Deferred<Boolean> createReverseMapping(final Object arg) {
//      if (!(arg instanceof Long)) {
//        throw new IllegalStateException("Expected a Long but got " + arg);
//      }
//      id = (Long) arg;
//      if (id <= 0) {
//        throw new IllegalStateException("Got a negative ID from HBase: " + id);
//      }
//      LOG.info("Got ID=" + id
//               + " for kind='" + kind() + "' name='" + name + "'");
//      row = Bytes.fromLong(id);
//      // row.length should actually be 8.
//      if (row.length < id_width) {
//        throw new IllegalStateException("OMG, row.length = " + row.length
//                                        + " which is less than " + id_width
//                                        + " for id=" + id
//                                        + " row=" + Arrays.toString(row));
//      }
//      // Verify that we're going to drop bytes that are 0.
//      for (int i = 0; i < row.length - id_width; i++) {
//        if (row[i] != 0) {
//          final String message = "All Unique IDs for " + kind()
//            + " on " + id_width + " bytes are already assigned!";
//          LOG.error("OMG " + message);
//          throw new IllegalStateException(message);
//        }
//      }
//      // Shrink the ID on the requested number of bytes.
//      row = Arrays.copyOfRange(row, row.length - id_width, row.length);
//
//      state = CREATE_FORWARD_MAPPING;
//      // We are CAS'ing the KV into existence -- the second argument is how
//      // we tell HBase we want to atomically create the KV, so that if there
//      // is already a KV in this cell, we'll fail.  Technically we could do
//      // just a `put' here, as we have a freshly allocated UID, so there is
//      // not reason why a KV should already exist for this UID, but just to
//      // err on the safe side and catch really weird corruption cases, we do
//      // a CAS instead to create the KV.
//      return client.compareAndSet(reverseMapping(), HBaseClient.EMPTY_ARRAY);
//    }
//
//    private PutRequest reverseMapping() {
//      return new PutRequest(table, row, NAME_FAMILY, kind, toBytes(name));
//    }
//
//    private Deferred<?> createForwardMapping(final Object arg) {
//      if (!(arg instanceof Boolean)) {
//        throw new IllegalStateException("Expected a Boolean but got " + arg);
//      }
//      if (!((Boolean) arg)) {  // Previous CAS failed. 
//        if (randomize_id) {
//          // This random Id is already used by another row
//          LOG.warn("Detected random id collision and retrying kind='" + 
//              kind() + "' name='" + name + "'");
//          random_id_collisions++;
//        } else {
//          // something is really messed up then
//          LOG.error("WTF!  Failed to CAS reverse mapping: " + reverseMapping()
//              + " -- run an fsck against the UID table!");
//        }
//        attempt--;
//        state = ALLOCATE_UID;
//        return Deferred.fromResult(false);
//      }
//
//      state = DONE;
//      return client.compareAndSet(forwardMapping(), HBaseClient.EMPTY_ARRAY);
//    }
//
//    private PutRequest forwardMapping() {
//        return new PutRequest(table, toBytes(name), ID_FAMILY, kind, row);
//    }
//
//    private Deferred<byte[]> done(final Object arg) {
//      if (!(arg instanceof Boolean)) {
//        throw new IllegalStateException("Expected a Boolean but got " + arg);
//      }
//      if (!((Boolean) arg)) {  // Previous CAS failed.  We lost a race.
//        LOG.warn("Race condition: tried to assign ID " + id + " to "
//                 + kind() + ":" + name + ", but CAS failed on "
//                 + forwardMapping() + ", which indicates this UID must have"
//                 + " been allocated concurrently by another TSD or thread. "
//                 + "So ID " + id + " was leaked.");
//        // If two TSDs attempted to allocate a UID for the same name at the
//        // same time, they would both have allocated a UID, and created a
//        // reverse mapping, and upon getting here, only one of them would
//        // manage to CAS this KV into existence.  The one that loses the
//        // race will retry and discover the UID assigned by the winner TSD,
//        // and a UID will have been wasted in the process.  No big deal.
//        if (randomize_id) {
//          // This random Id is already used by another row
//          LOG.warn("Detected random id collision between two tsdb "
//              + "servers kind='" + kind() + "' name='" + name + "'");
//          random_id_collisions++;
//        }
//        
//        class GetIdCB implements Callback<Object, byte[]> {
//          public Object call(final byte[] row) throws Exception {
//            assignment.callback(row);
//            return null;
//          }
//        }
//        getIdAsync(name).addCallback(new GetIdCB());
//        return assignment;
//      }
//
//      cacheMapping(name, row);
//      
//      if (tsdb != null && tsdb.getConfig().enable_realtime_uid()) {
//        final UIDMeta meta = new UIDMeta(type, row, name);
//        meta.storeNew(tsdb);
//        LOG.info("Wrote UIDMeta for: " + name);
//        tsdb.indexUIDMeta(meta);
//      }
//      
//      synchronized(pending_assignments) {
//        if (pending_assignments.remove(name) != null) {
//          LOG.info("Completed pending assignment for: " + name);
//        }
//      }
//      assignment.callback(row);
//      return assignment;
//    }
//
//  }
//
//  /** Adds the bidirectional mapping in the cache. */
//  private void cacheMapping(final String name, final byte[] id) {
//    addIdToCache(name, id);
//    addNameToCache(id, name);
//  } 
//  
//    try {
//      return getIdAsync(name).joinUninterruptibly();
//    } catch (NoSuchUniqueName e) {
//      if (tsdb != null && tsdb.getUidFilter() != null && 
//          tsdb.getUidFilter().fillterUIDAssignments()) {
//        try {
//          if (!tsdb.getUidFilter().allowUIDAssignment(type, name, null, null)
//                .join()) {
//            rejected_assignments++;
//            throw new FailedToAssignUniqueIdException(new String(kind), name, 0, 
//                "Blocked by UID filter.");
//          }
//        } catch (FailedToAssignUniqueIdException e1) {
//          throw e1;
//        } catch (InterruptedException e1) {
//          LOG.error("Interrupted", e1);
//          Thread.currentThread().interrupt();
//        } catch (Exception e1) {
//          throw new RuntimeException("Should never be here", e1);
//        }
//      }
//      
//      Deferred<byte[]> assignment = null;
//      boolean pending = false;
//      synchronized (pending_assignments) {
//        assignment = pending_assignments.get(name);
//        if (assignment == null) {
//          // to prevent UID leaks that can be caused when multiple time
//          // series for the same metric or tags arrive, we need to write a 
//          // deferred to the pending map as quickly as possible. Then we can 
//          // start the assignment process after we've stashed the deferred 
//          // and released the lock
//          assignment = new Deferred<byte[]>();
//          pending_assignments.put(name, assignment);
//        } else {
//          pending = true;
//        }
//      }
//      
//      if (pending) {
//        LOG.info("Already waiting for UID assignment: " + name);
//        try {
//          return assignment.joinUninterruptibly();
//        } catch (Exception e1) {
//          throw new RuntimeException("Should never be here", e1);
//        }
//      }
//      
//      // start the assignment dance after stashing the deferred
//      byte[] uid = null;
//      try {
//        uid = new UniqueIdAllocator(name, assignment).tryAllocate().joinUninterruptibly();
//      } catch (RuntimeException e1) {
//        throw e1;
//      } catch (Exception e1) {
//        throw new RuntimeException("Should never be here", e);
//      } finally {
//        synchronized (pending_assignments) {
//          if (pending_assignments.remove(name) != null) {
//            LOG.info("Completed pending assignment for: " + name);
//          }
//        }
//      }
//      return uid;
//    } catch (Exception e) {
//      throw new RuntimeException("Should never be here", e);
//    }
//  }
//  
//  /**
//   * Finds the ID associated with a given name or creates it.
//   * <p>
//   * The length of the byte array is fixed in advance by the implementation.
//   *
//   * @param name The name to lookup in the table or to assign an ID to.
//   * @throws HBaseException if there is a problem communicating with HBase.
//   * @throws IllegalStateException if all possible IDs are already assigned.
//   * @throws IllegalStateException if the ID found in HBase is encoded on the
//   * wrong number of bytes.
//   * @since 1.2
//   */
//  public Deferred<byte[]> getOrCreateIdAsync(final String name) {
//    return getOrCreateIdAsync(name, null, null);
//  }
//  
  @Override
  public Deferred<byte[]> getOrCreateId(final UniqueIdType type, 
                                        final String name,
                                        final TimeSeriesId id,
                                        final Span span) {
    // TODO - implement
    return null;
//    if (type == null) {
//      throw new IllegalArgumentException("Type cannot be null.");
//    }
//    if (Strings.isNullOrEmpty(name)) {
//      throw new IllegalArgumentException("Name cannot be null or empty.");
//    }
//    if (id == null) {
//      throw new IllegalArgumentException("The ID cannot be null.");
//    }
//    
//    final Span child;
//    if (span != null && span.isDebug()) {
//      child = span.newChild("Tsdb1x_getIds")
//          .withTag("dataStore", data_store.id())
//          .withTag("type", type.toString())
//          .withTag("name", name)
//          .withTag("id", id.toString())
//          .start();
//    } else {
//      child = null;
//    }
//    
//    /** Triggers the assignment if allowed through the filter */
//    class AssignmentAllowedCB implements  Callback<Deferred<byte[]>, Boolean> {
//      @Override
//      public Deferred<byte[]> call(final Boolean allowed) throws Exception {
//        if (!allowed) {
//          rejected_assignments.get(type).increment();
//          return Deferred.fromError(new FailedToAssignUniqueIdException(
//              type, name, 0, "Blocked by UID filter."));
//        }
//        
//        Deferred<byte[]> assignment = null;
//        synchronized (pending_assignments) {
//          assignment = pending_assignments.get(name);
//          if (assignment == null) {
//            // to prevent UID leaks that can be caused when multiple time
//            // series for the same metric or tags arrive, we need to write a 
//            // deferred to the pending map as quickly as possible. Then we can 
//            // start the assignment process after we've stashed the deferred 
//            // and released the lock
//            assignment = new Deferred<byte[]>();
//            pending_assignments.put(name, assignment);
//          } else {
//            LOG.info("Already waiting for UID assignment: " + name);
//            return assignment;
//          }
//        }
//        
//        // start the assignment dance after stashing the deferred
//        if (metric != null && LOG.isDebugEnabled()) {
//          LOG.debug("Assigning UID for '" + name + "' of type '" + type + 
//              "' for series '" + metric + ", " + tags + "'");
//        }
//        
//        // start the assignment dance after stashing the deferred
//        return new UniqueIdAllocator(name, assignment).tryAllocate();
//      }
//      @Override
//      public String toString() {
//        return "AssignmentAllowedCB";
//      }
//    }
//    
//    /** Triggers an assignment (possibly through the filter) if the exception 
//     * returned was a NoSuchUniqueName. */
//    class HandleNoSuchUniqueNameCB implements Callback<Object, Exception> {
//      public Object call(final Exception e) {
//        if (e instanceof NoSuchUniqueName) {
//          if (tsdb != null && tsdb.getUidFilter() != null && 
//              tsdb.getUidFilter().fillterUIDAssignments()) {
//            return tsdb.getUidFilter()
//                .allowUIDAssignment(type, name, metric, tags)
//                .addCallbackDeferring(new AssignmentAllowedCB());
//          } else {
//            return Deferred.fromResult(true)
//                .addCallbackDeferring(new AssignmentAllowedCB());
//          }
//        }
//        return e;  // Other unexpected exception, let it bubble up.
//      }
//    }
//
//    // Kick off the HBase lookup, and if we don't find it there either, start
//    // the process to allocate a UID.
//    return getId(name).addErrback(new HandleNoSuchUniqueNameCB());
  }

  @Override
  public Deferred<List<byte[]>> getOrCreateIds(final UniqueIdType type, 
                                               final List<String> names,
                                               final TimeSeriesId id,
                                               final Span span) {
    // TODO - implement
    return null;
  }
  
//  /**
//   * Attempts to find suggestions of names given a search term.
//   * <p>
//   * <strong>This method is blocking.</strong>  Its use within OpenTSDB itself
//   * is discouraged, please use {@link #suggestAsync} instead.
//   * @param search The search term (possibly empty).
//   * @return A list of known valid names that have UIDs that sort of match
//   * the search term.  If the search term is empty, returns the first few
//   * terms.
//   * @throws HBaseException if there was a problem getting suggestions from
//   * HBase.
//   */
//  public List<String> suggest(final String search) throws HBaseException {
//    return suggest(search, MAX_SUGGESTIONS);
//  }
//      
//  /**
//   * Attempts to find suggestions of names given a search term.
//   * @param search The search term (possibly empty).
//   * @param max_results The number of results to return. Must be 1 or greater
//   * @return A list of known valid names that have UIDs that sort of match
//   * the search term.  If the search term is empty, returns the first few
//   * terms.
//   * @throws HBaseException if there was a problem getting suggestions from
//   * HBase.
//   * @throws IllegalArgumentException if the count was less than 1
//   * @since 2.0
//   */
//  public List<String> suggest(final String search, final int max_results) 
//    throws HBaseException {
//    if (max_results < 1) {
//      throw new IllegalArgumentException("Count must be greater than 0");
//    }
//    try {
//      return suggestAsync(search, max_results).joinUninterruptibly();
//    } catch (HBaseException e) {
//      throw e;
//    } catch (Exception e) {  // Should never happen.
//      final String msg = "Unexpected exception caught by "
//        + this + ".suggest(" + search + ')';
//      LOG.error(msg, e);
//      throw new RuntimeException(msg, e);  // Should never happen.
//    }
//  }
//
//
//  /**
//   * Helper callback to asynchronously scan HBase for suggestions.
//   */
//  private final class SuggestCB
//    implements Callback<Object, ArrayList<ArrayList<KeyValue>>> {
//    private final LinkedList<String> suggestions = new LinkedList<String>();
//    private final Scanner scanner;
//    private final int max_results;
//
//    SuggestCB(final String search, final int max_results) {
//      this.max_results = max_results;
//      this.scanner = getSuggestScanner(client, table, search, kind, max_results);
//    }
//
//    @SuppressWarnings("unchecked")
//    Deferred<List<String>> search() {
//      return (Deferred) scanner.nextRows().addCallback(this);
//    }
//
//    public Object call(final ArrayList<ArrayList<KeyValue>> rows) {
//      if (rows == null) {  // We're done scanning.
//        return suggestions;
//      }
//      
//      for (final ArrayList<KeyValue> row : rows) {
//        if (row.size() != 1) {
//          LOG.error("WTF shouldn't happen!  Scanner " + scanner + " returned"
//                    + " a row that doesn't have exactly 1 KeyValue: " + row);
//          if (row.isEmpty()) {
//            continue;
//          }
//        }
//        final byte[] key = row.get(0).key();
//        final String name = fromBytes(key);
//        final byte[] id = row.get(0).value();
//        final byte[] cached_id = name_cache.get(name);
//        if (cached_id == null) {
//          cacheMapping(name, id); 
//        } else if (!Arrays.equals(id, cached_id)) {
//          throw new IllegalStateException("WTF?  For kind=" + kind()
//            + " name=" + name + ", we have id=" + Arrays.toString(cached_id)
//            + " in cache, but just scanned id=" + Arrays.toString(id));
//        }
//        suggestions.add(name);
//        if ((short) suggestions.size() >= max_results) {  // We have enough.
//          return scanner.close().addCallback(new Callback<Object, Object>() {
//            @Override
//            public Object call(Object ignored) throws Exception {
//              return suggestions;
//            }
//          });
//        }
//        row.clear();  // free()
//      }
//      return search();  // Get more suggestions.
//    }
//  }
//
//    final byte[] row = getId(oldname);
//    final String row_string = fromBytes(row);
//    {
//      byte[] id = null;
//      try {
//        id = getId(newname);
//      } catch (NoSuchUniqueName e) {
//        // OK, we don't want the new name to be assigned.
//      }
//      if (id != null) {
//        throw new IllegalArgumentException("When trying rename(\"" + oldname
//          + "\", \"" + newname + "\") on " + this + ": new name already"
//          + " assigned ID=" + Arrays.toString(id));
//      }
//    }
//
//    if (renaming_id_names.contains(row_string)
//        || renaming_id_names.contains(newname)) {
//      throw new IllegalArgumentException("Ongoing rename on the same ID(\""
//        + Arrays.toString(row) + "\") or an identical new name(\"" + newname
//        + "\")");
//    }
//    renaming_id_names.add(row_string);
//    renaming_id_names.add(newname);
//
//    final byte[] newnameb = toBytes(newname);
//
//    // Update the reverse mapping first, so that if we die before updating
//    // the forward mapping we don't run the risk of "publishing" a
//    // partially assigned ID.  The reverse mapping on its own is harmless
//    // but the forward mapping without reverse mapping is bad.
//    try {
//      final PutRequest reverse_mapping = new PutRequest(
//        table, row, NAME_FAMILY, kind, newnameb);
//      hbasePutWithRetry(reverse_mapping, MAX_ATTEMPTS_PUT,
//                        INITIAL_EXP_BACKOFF_DELAY);
//    } catch (HBaseException e) {
//      LOG.error("When trying rename(\"" + oldname
//        + "\", \"" + newname + "\") on " + this + ": Failed to update reverse"
//        + " mapping for ID=" + Arrays.toString(row), e);
//      renaming_id_names.remove(row_string);
//      renaming_id_names.remove(newname);
//      throw e;
//    }
//
//    // Now create the new forward mapping.
//    try {
//      final PutRequest forward_mapping = new PutRequest(
//        table, newnameb, ID_FAMILY, kind, row);
//      hbasePutWithRetry(forward_mapping, MAX_ATTEMPTS_PUT,
//                        INITIAL_EXP_BACKOFF_DELAY);
//    } catch (HBaseException e) {
//      LOG.error("When trying rename(\"" + oldname
//        + "\", \"" + newname + "\") on " + this + ": Failed to create the"
//        + " new forward mapping with ID=" + Arrays.toString(row), e);
//      renaming_id_names.remove(row_string);
//      renaming_id_names.remove(newname);
//      throw e;
//    }
//
//    // Update cache.
//    addIdToCache(newname, row);            // add     new name -> ID
//    id_cache.put(fromBytes(row), newname);  // update  ID -> new name
//    name_cache.remove(oldname);             // remove  old name -> ID
//
//    // Delete the old forward mapping.
//    try {
//      final DeleteRequest old_forward_mapping = new DeleteRequest(
//        table, toBytes(oldname), ID_FAMILY, kind);
//      client.delete(old_forward_mapping).joinUninterruptibly();
//    } catch (HBaseException e) {
//      LOG.error("When trying rename(\"" + oldname
//        + "\", \"" + newname + "\") on " + this + ": Failed to remove the"
//        + " old forward mapping for ID=" + Arrays.toString(row), e);
//      throw e;
//    } catch (Exception e) {
//      final String msg = "Unexpected exception when trying rename(\"" + oldname
//        + "\", \"" + newname + "\") on " + this + ": Failed to remove the"
//        + " old forward mapping for ID=" + Arrays.toString(row);
//      LOG.error("WTF?  " + msg, e);
//      throw new RuntimeException(msg, e);
//    } finally {
//      renaming_id_names.remove(row_string);
//      renaming_id_names.remove(newname);
//    }
//    // Success!
//  }
//
//    if (tsdb == null) {
//      throw new IllegalStateException("The TSDB is null for this UID object.");
//    }
//    final byte[] uid = new byte[id_width];
//    final ArrayList<Deferred<Object>> deferreds = 
//        new ArrayList<Deferred<Object>>(2);
//    
//    /** Catches errors and still cleans out the cache */
//    class ErrCB implements Callback<Object, Exception> {
//      @Override
//      public Object call(final Exception ex) throws Exception {
//        name_cache.remove(name);
//        id_cache.remove(fromBytes(uid));
//        LOG.error("Failed to delete " + fromBytes(kind) + " UID " + name 
//            + " but still cleared the cache", ex);
//        return ex;
//      }
//    }
//    
//    /** Used to wait on the group of delete requests */
//    class GroupCB implements Callback<Deferred<Object>, ArrayList<Object>> {
//      @Override
//      public Deferred<Object> call(final ArrayList<Object> response) 
//          throws Exception {
//        name_cache.remove(name);
//        id_cache.remove(fromBytes(uid));
//        LOG.info("Successfully deleted " + fromBytes(kind) + " UID " + name);
//        return Deferred.fromResult(null);
//      }
//    }
//    
//    /** Called after fetching the UID from storage */
//    class LookupCB implements Callback<Deferred<Object>, byte[]> {
//      @Override
//      public Deferred<Object> call(final byte[] stored_uid) throws Exception {
//        if (stored_uid == null) {
//          return Deferred.fromError(new NoSuchUniqueName(kind(), name));
//        }
//        System.arraycopy(stored_uid, 0, uid, 0, id_width);
//        final DeleteRequest forward = 
//            new DeleteRequest(table, toBytes(name), ID_FAMILY, kind);
//        deferreds.add(tsdb.getClient().delete(forward));
//        
//        final DeleteRequest reverse = 
//            new DeleteRequest(table, uid, NAME_FAMILY, kind);
//        deferreds.add(tsdb.getClient().delete(reverse));
//        
//        final DeleteRequest meta = new DeleteRequest(table, uid, NAME_FAMILY, 
//            toBytes((type.toString().toLowerCase() + "_meta")));
//        deferreds.add(tsdb.getClient().delete(meta));
//        return Deferred.group(deferreds).addCallbackDeferring(new GroupCB());
//      }
//    }
//    
//    final byte[] cached_uid = name_cache.get(name);
//    if (cached_uid == null) {
//      return getIdFromHBase(name).addCallbackDeferring(new LookupCB())
//          .addErrback(new ErrCB());
//    }
//    System.arraycopy(cached_uid, 0, uid, 0, id_width);
//    final DeleteRequest forward = 
//        new DeleteRequest(table, toBytes(name), ID_FAMILY, kind);
//    deferreds.add(tsdb.getClient().delete(forward));
//    
//    final DeleteRequest reverse = 
//        new DeleteRequest(table, uid, NAME_FAMILY, kind);
//    deferreds.add(tsdb.getClient().delete(reverse));
//    
//    final DeleteRequest meta = new DeleteRequest(table, uid, NAME_FAMILY, 
//        toBytes((type.toString().toLowerCase() + "_meta")));
//    deferreds.add(tsdb.getClient().delete(meta));
//    return Deferred.group(deferreds).addCallbackDeferring(new GroupCB())
//        .addErrback(new ErrCB());
//  }
//  
//  /** The start row to scan on empty search strings.  `!' = first ASCII char. */
//  private static final byte[] START_ROW = new byte[] { '!' };
//
//  /** The end row to scan on empty search strings.  `~' = last ASCII char. */
//  private static final byte[] END_ROW = new byte[] { '~' };
//
//  /**
//   * Creates a scanner that scans the right range of rows for suggestions.
//   * @param client The HBase client to use.
//   * @param tsd_uid_table Table where IDs are stored.
//   * @param search The string to start searching at
//   * @param kind_or_null The kind of UID to search or null for any kinds.
//   * @param max_results The max number of results to return
//   */
//  private static Scanner getSuggestScanner(final HBaseClient client,
//      final byte[] tsd_uid_table, final String search,
//      final byte[] kind_or_null, final int max_results) {
//    final byte[] start_row;
//    final byte[] end_row;
//    if (search.isEmpty()) {
//      start_row = START_ROW;
//      end_row = END_ROW;
//    } else {
//      start_row = toBytes(search);
//      end_row = Arrays.copyOf(start_row, start_row.length);
//      end_row[start_row.length - 1]++;
//    }
//    final Scanner scanner = client.newScanner(tsd_uid_table);
//    scanner.setStartKey(start_row);
//    scanner.setStopKey(end_row);
//    scanner.setFamily(ID_FAMILY);
//    if (kind_or_null != null) {
//      scanner.setQualifier(kind_or_null);
//    }
//    scanner.setMaxNumRows(max_results <= 4096 ? max_results : 4096);
//    return scanner;
//  }
//
  /** Returns the cell of the specified row key, using family:kind. */
  private Deferred<byte[]> hbaseGet(final UniqueIdType type, 
                                    final byte[] key, 
                                    final byte[] family) {
    final GetRequest get = new GetRequest(
        data_store.uidTable(), key, family);
    
    switch(type) {
    case METRIC:
      get.qualifier(METRICS_QUAL);
      break;
    case TAGK:
      get.qualifier(TAG_NAME_QUAL);
      break;
    case TAGV:
      get.qualifier(TAG_VALUE_QUAL);
      break;
    default:
      throw new IllegalArgumentException("This data store does not "
          + "handle Unique IDs of type: " + type);
    }
    
    class GetCB implements Callback<byte[], ArrayList<KeyValue>> {
      public byte[] call(final ArrayList<KeyValue> row) {
        if (row == null || row.isEmpty()) {
          return null;
        }
        return row.get(0).value();
      }
      
      @Override
      public String toString() {
        return "HBase UniqueId Get Request Callback";
      }
    }
    return data_store.client().get(get).addCallback(new GetCB());
  }
//
//  /**
//   * Attempts to run the PutRequest given in argument, retrying if needed.
//   *
//   * Puts are synchronized.
//   *
//   * @param put The PutRequest to execute.
//   * @param attempts The maximum number of attempts.
//   * @param wait The initial amount of time in ms to sleep for after a
//   * failure.  This amount is doubled after each failed attempt.
//   * @throws HBaseException if all the attempts have failed.  This exception
//   * will be the exception of the last attempt.
//   */
//  private void hbasePutWithRetry(final PutRequest put, short attempts, short wait)
//    throws HBaseException {
//    put.setBufferable(false);  // TODO(tsuna): Remove once this code is async.
//    while (attempts-- > 0) {
//      try {
//        client.put(put).joinUninterruptibly();
//        return;
//      } catch (HBaseException e) {
//        if (attempts > 0) {
//          LOG.error("Put failed, attempts left=" + attempts
//                    + " (retrying in " + wait + " ms), put=" + put, e);
//          try {
//            Thread.sleep(wait);
//          } catch (InterruptedException ie) {
//            throw new RuntimeException("interrupted", ie);
//          }
//          wait *= 2;
//        } else {
//          throw e;
//        }
//      } catch (Exception e) {
//        LOG.error("WTF?  Unexpected exception type, put=" + put, e);
//      }
//    }
//    throw new IllegalStateException("This code should never be reached!");
//  }
//
//  private static byte[] toBytes(final String s) {
//    return s.getBytes(CHARSET);
//  }
//
//  private static String fromBytes(final byte[] b) {
//    return new String(b, CHARSET);
//  }
//
//  /** Returns a human readable string representation of the object. */
//  public String toString() {
//    return "UniqueId(" + fromBytes(table) + ", " + kind() + ", " + id_width + ")";
//  }
//
//  /**
//   * Extracts the TSUID from a storage row key that includes the timestamp.
//   * @param row_key The row key to process
//   * @param metric_width The width of the metric
//   * @param timestamp_width The width of the timestamp
//   * @return The TSUID as a byte array
//   * @throws IllegalArgumentException if the row key is missing tags or it is
//   * corrupt such as a salted key when salting is disabled or vice versa.
//   */
//  public static byte[] getTSUIDFromKey(final byte[] row_key, 
//      final short metric_width, final short timestamp_width) {
//    int idx = 0;
//    // validation
//    final int tag_pair_width = TSDB.tagk_width() + TSDB.tagv_width();
//    final int tags_length = row_key.length - 
//        (Const.SALT_WIDTH() + metric_width + timestamp_width);
//    if (tags_length < tag_pair_width || (tags_length % tag_pair_width) != 0) {
//      throw new IllegalArgumentException(
//          "Row key is missing tags or it is corrupted " + Arrays.toString(row_key));
//    }
//    final byte[] tsuid = new byte[
//                 row_key.length - timestamp_width - Const.SALT_WIDTH()];
//    for (int i = Const.SALT_WIDTH(); i < row_key.length; i++) {
//      if (i < Const.SALT_WIDTH() + metric_width || 
//          i >= (Const.SALT_WIDTH() + metric_width + timestamp_width)) {
//        tsuid[idx] = row_key[i];
//        idx++;
//      }
//    }
//    return tsuid;
//  }
//  
//  /**
//   * Extracts a list of tagks and tagvs as individual values in a list
//   * @param tsuid The tsuid to parse
//   * @return A list of tagk/tagv UIDs alternating with tagk, tagv, tagk, tagv
//   * @throws IllegalArgumentException if the TSUID is malformed
//   * @since 2.1
//   */
//  public static List<byte[]> getTagsFromTSUID(final String tsuid) {
//    if (tsuid == null || tsuid.isEmpty()) {
//      throw new IllegalArgumentException("Missing TSUID");
//    }
//    if (tsuid.length() <= TSDB.metrics_width() * 2) {
//      throw new IllegalArgumentException(
//          "TSUID is too short, may be missing tags");
//    }
//     
//    final List<byte[]> tags = new ArrayList<byte[]>();
//    final int pair_width = (TSDB.tagk_width() * 2) + (TSDB.tagv_width() * 2);
//    
//    // start after the metric then iterate over each tagk/tagv pair
//    for (int i = TSDB.metrics_width() * 2; i < tsuid.length(); i+= pair_width) {
//      if (i + pair_width > tsuid.length()){
//        throw new IllegalArgumentException(
//            "The TSUID appears to be malformed, improper tag width");
//      }
//      String tag = tsuid.substring(i, i + (TSDB.tagk_width() * 2));
//      tags.add(UniqueId.stringToUid(tag));
//      tag = tsuid.substring(i + (TSDB.tagk_width() * 2), i + pair_width);
//      tags.add(UniqueId.stringToUid(tag));
//    }
//    return tags;
//  }
//   
//  /**
//   * Extracts a list of tagk/tagv pairs from a tsuid
//   * @param tsuid The tsuid to parse
//   * @return A list of tagk/tagv UID pairs
//   * @throws IllegalArgumentException if the TSUID is malformed
//   * @since 2.0
//   */
//  public static List<byte[]> getTagPairsFromTSUID(final String tsuid) {
//     if (tsuid == null || tsuid.isEmpty()) {
//       throw new IllegalArgumentException("Missing TSUID");
//     }
//     if (tsuid.length() <= TSDB.metrics_width() * 2) {
//       throw new IllegalArgumentException(
//           "TSUID is too short, may be missing tags");
//     }
//      
//     final List<byte[]> tags = new ArrayList<byte[]>();
//     final int pair_width = (TSDB.tagk_width() * 2) + (TSDB.tagv_width() * 2);
//     
//     // start after the metric then iterate over each tagk/tagv pair
//     for (int i = TSDB.metrics_width() * 2; i < tsuid.length(); i+= pair_width) {
//       if (i + pair_width > tsuid.length()){
//         throw new IllegalArgumentException(
//             "The TSUID appears to be malformed, improper tag width");
//       }
//       String tag = tsuid.substring(i, i + pair_width);
//       tags.add(UniqueId.stringToUid(tag));
//     }
//     return tags;
//   }
//  
//  /**
//   * Extracts a list of tagk/tagv pairs from a tsuid
//   * @param tsuid The tsuid to parse
//   * @return A list of tagk/tagv UID pairs
//   * @throws IllegalArgumentException if the TSUID is malformed
//   * @since 2.0
//   */
//  public static List<byte[]> getTagPairsFromTSUID(final byte[] tsuid) {
//    if (tsuid == null) {
//      throw new IllegalArgumentException("Missing TSUID");
//    }
//    if (tsuid.length <= TSDB.metrics_width()) {
//      throw new IllegalArgumentException(
//          "TSUID is too short, may be missing tags");
//    }
//     
//    final List<byte[]> tags = new ArrayList<byte[]>();
//    final int pair_width = TSDB.tagk_width() + TSDB.tagv_width();
//    
//    // start after the metric then iterate over each tagk/tagv pair
//    for (int i = TSDB.metrics_width(); i < tsuid.length; i+= pair_width) {
//      if (i + pair_width > tsuid.length){
//        throw new IllegalArgumentException(
//            "The TSUID appears to be malformed, improper tag width");
//      }
//      tags.add(Arrays.copyOfRange(tsuid, i, i + pair_width));
//    }
//    return tags;
//  }
//  
//  /**
//   * Returns a map of max UIDs from storage for the given list of UID types 
//   * @param tsdb The TSDB to which we belong
//   * @param kinds A list of qualifiers to fetch
//   * @return A map with the "kind" as the key and the maximum assigned UID as
//   * the value
//   * @since 2.0
//   */
//  public static Deferred<Map<String, Long>> getUsedUIDs(final TSDB tsdb,
//      final byte[][] kinds) {
//    
//    /**
//     * Returns a map with 0 if the max ID row hasn't been initialized yet, 
//     * otherwise the map has actual data
//     */
//    final class GetCB implements Callback<Map<String, Long>, 
//      ArrayList<KeyValue>> {
//
//      @Override
//      public Map<String, Long> call(final ArrayList<KeyValue> row)
//          throws Exception {
//        
//        final Map<String, Long> results = new HashMap<String, Long>(3);
//        if (row == null || row.isEmpty()) {
//          // it could be the case that this is the first time the TSD has run
//          // and the user hasn't put any metrics in, so log and return 0s
//          LOG.info("Could not find the UID assignment row");
//          for (final byte[] kind : kinds) {
//            results.put(new String(kind, CHARSET), 0L);
//          }
//          return results;
//        }
//        
//        for (final KeyValue column : row) {
//          results.put(new String(column.qualifier(), CHARSET), 
//              Bytes.getLong(column.value()));
//        }
//        
//        // if the user is starting with a fresh UID table, we need to account
//        // for missing columns
//        for (final byte[] kind : kinds) {
//          if (results.get(new String(kind, CHARSET)) == null) {
//            results.put(new String(kind, CHARSET), 0L);
//          }
//        }
//        return results;
//      }
//      
//    }
//    
//    final GetRequest get = new GetRequest(tsdb.uidTable(), MAXID_ROW);
//    get.family(ID_FAMILY);
//    get.qualifiers(kinds);
//    return tsdb.getClient().get(get).addCallback(new GetCB());
//  }
//
//  /**
//   * Pre-load UID caches, scanning up to "tsd.core.preload_uid_cache.max_entries"
//   * rows from the UID table.
//   * @param tsdb The TSDB to use 
//   * @param uid_cache_map A map of {@link UniqueId} objects keyed on the kind.
//   * @throws HBaseException Passes any HBaseException from HBase scanner.
//   * @throws RuntimeException Wraps any non HBaseException from HBase scanner.
//   * @2.1
//   */
//  public static void preloadUidCache(final TSDB tsdb,
//      final ByteMap<UniqueId> uid_cache_map) throws HBaseException {
//    int max_results = tsdb.getConfig().getInt(
//        "tsd.core.preload_uid_cache.max_entries");
//    LOG.info("Preloading uid cache with max_results=" + max_results);
//    if (max_results <= 0) {
//      return;
//    }
//    Scanner scanner = null;
//    try {
//      int num_rows = 0;
//      scanner = getSuggestScanner(tsdb.getClient(), tsdb.uidTable(), "", null, 
//          max_results);
//      for (ArrayList<ArrayList<KeyValue>> rows = scanner.nextRows().join();
//          rows != null;
//          rows = scanner.nextRows().join()) {
//        for (final ArrayList<KeyValue> row : rows) {
//          for (KeyValue kv: row) {
//            final String name = fromBytes(kv.key());
//            final byte[] kind = kv.qualifier();
//            final byte[] id = kv.value();
//            LOG.debug("id='{}', name='{}', kind='{}'", Arrays.toString(id),
//                name, fromBytes(kind));
//            UniqueId uid_cache = uid_cache_map.get(kind);
//            if (uid_cache != null) {
//              uid_cache.cacheMapping(name, id);
//            }
//          }
//          num_rows += row.size();
//          row.clear();  // free()
//          if (num_rows >= max_results) {
//            break;
//          }
//        }
//      }
//      for (UniqueId unique_id_table : uid_cache_map.values()) {
//        LOG.info("After preloading, uid cache '{}' has {} ids and {} names.",
//                 unique_id_table.kind(),
//                 unique_id_table.id_cache.size(),
//                 unique_id_table.name_cache.size());
//      }
//    } catch (Exception e) {
//      if (e instanceof HBaseException) {
//        throw (HBaseException)e;
//      } else if (e instanceof RuntimeException) {
//        throw (RuntimeException)e;
//      } else {
//        throw new RuntimeException("Error while preloading IDs", e);
//      }
//    } finally {
//      if (scanner != null) {
//        scanner.close();
//      }
//    }
//  }

  @Override
  public Charset characterSet() {
    return character_set;
  }
  
  class ResolvedFilterImplementation implements ResolvedFilter {
    
    byte[] tag_key;
    List<byte[]> tag_values;
    
    @Override
    public byte[] getTagKey() {
      return tag_key;
    }

    @Override
    public List<byte[]> getTagValues() {
      return tag_values;
    }
    
  }
  
}
