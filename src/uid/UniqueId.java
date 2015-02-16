// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.uid;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.bind.DatatypeConverter;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.eventbus.EventBus;
import com.google.common.base.Strings;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.StringCoder;

import net.opentsdb.stats.Metrics;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.utils.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Const;
import org.hbase.async.Bytes;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static net.opentsdb.core.StringCoder.toBytes;
import static net.opentsdb.stats.Metrics.name;
import static net.opentsdb.stats.Metrics.tag;

/**
 * Represents a table of Unique IDs, manages the lookup and creation of IDs.
 * <p>
 * Don't attempt to use {@code equals()} or {@code hashCode()} on
 * this class.
 */
public class UniqueId {
  private static final Logger LOG = LoggerFactory.getLogger(UniqueId.class);

  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  /** The single column family used by this class. */
  private static final byte[] ID_FAMILY = toBytes("id");
  /** The single column family used by this class. */
  private static final byte[] NAME_FAMILY = toBytes("name");
  /** Row key of the special row used to track the max ID already assigned. */
  private static final byte[] MAXID_ROW = { 0 };
  /** How many time do we try to assign an ID before giving up. */
  private static final short MAX_ATTEMPTS_ASSIGN_ID = 3;
  /** Maximum number of results to return in suggest(). */
  private static final short MAX_SUGGESTIONS = 25;

  /** The TsdbStore to use.  */
  private final TsdbStore tsdb_store;
  /** Table where IDs are stored.  */
  private final byte[] table;
  /** The type of UID represented by this cache */
  private final UniqueIdType type;
  /** Number of bytes on which each ID is encoded. */
  private final short id_width;

  /** Cache for forward mappings (name to ID). */
  private final ConcurrentHashMap<String, byte[]> name_cache =
    new ConcurrentHashMap<String, byte[]>();
  /** Cache for backward mappings (ID to name).
   * The ID in the key is a byte[] converted to a String to be Comparable. */
  private final ConcurrentHashMap<String, String> id_cache =
    new ConcurrentHashMap<String, String>();
  /** Map of pending UID assignments */
  private final HashMap<String, Deferred<byte[]>> pending_assignments =
    new HashMap<String, Deferred<byte[]>>();

  /** Number of times we avoided reading from TsdbStore thanks to the cache. */
  private final Counter cache_hits;
  /** Number of times we had to read from TsdbStore and populate the cache. */
  private final Counter cache_misses;

  /**
   * The event bus to which the id changes done by this instance will be
   * published.
   */
  private final EventBus idEventBus;

  /**
   * Constructor.
   * @param tsdb_store The TsdbStore to use.
   * @param table The name of the table to use.
   * @param type The type of UIDs this instance represents
   * @param metrics
   * @param idEventBus
   */
  public UniqueId(final TsdbStore tsdb_store,
                  final byte[] table,
                  final UniqueIdType type,
                  final Metrics metrics,
                  final EventBus idEventBus) {
    this.tsdb_store = checkNotNull(tsdb_store);
    this.table = checkNotNull(table);
    this.type = checkNotNull(type);
    this.id_width = type.width;
    this.idEventBus = checkNotNull(idEventBus);

    cache_hits = new Counter();
    cache_misses = new Counter();
    registerMetrics(metrics);

  }

  private void registerMetrics(final Metrics metrics) {
    final MetricRegistry registry = metrics.getRegistry();

    Metrics.Tag typeTag = tag("kind", type.toValue());
    registry.register(name("uid.cache-hit", typeTag), cache_hits);
    registry.register(name("uid.cache-miss", typeTag), cache_misses);

    registry.register(name("uid.cache-size", typeTag), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return name_cache.size() + id_cache.size();
      }
    });
  }

  public UniqueIdType type() {
    return type;
  }

  public short width() {
    return id_width;
  }

  /**
   * Causes this instance to discard all its in-memory caches.
   * @since 1.1
   */
  public void dropCaches() {
    name_cache.clear();
    id_cache.clear();
  }

  /**
   * Finds the name associated with a given ID.
   *
   * @param id The ID associated with that name.
   * @see #getId(String)
   * @see #createId(String)
   * @throws NoSuchUniqueId if the given ID is not assigned.
   * @throws IllegalArgumentException if the ID given in argument is encoded
   * on the wrong number of bytes.
   * @since 1.1
   */
  public Deferred<String> getName(final byte[] id) {
    final String name = getNameFromCache(id);
    if (name != null) {
      cache_hits.inc();
      return Deferred.fromResult(name);
    }
    cache_misses.inc();
    class GetNameCB implements Callback<String, Optional<String>> {
      public String call(final Optional<String> name) {
        if (name.isPresent()) {
          addNameToCache(id, name.get());
          addIdToCache(name.get(), id);
          return name.get();
        }

        throw new NoSuchUniqueId(type, id);
      }
    }
    return tsdb_store.getName(id, type).addCallback(new GetNameCB());
  }

  private String getNameFromCache(final byte[] id) {
    return id_cache.get(StringCoder.fromBytes(id));
  }

  private void addNameToCache(final byte[] id, final String name) {
    final String key = StringCoder.fromBytes(id);
    String found = id_cache.get(key);
    if (found == null) {
      found = id_cache.putIfAbsent(key, name);
    }
    if (found != null && !found.equals(name)) {
      throw new IllegalStateException("id=" + Arrays.toString(id) + " => name="
          + name + ", already mapped to " + found);
    }
  }

  public Deferred<byte[]> getId(final String name) {
    final byte[] id = getIdFromCache(name);
    if (id != null) {
      cache_hits.inc();
      return Deferred.fromResult(id);
    }
    cache_misses.inc();
    class GetIdCB implements Callback<byte[], Optional<byte[]>> {
      public byte[] call(final Optional<byte[]> id) {
        if (id.isPresent()) {
          addIdToCache(name, id.get());
          addNameToCache(id.get(), name);
          return id.get();
        }

        throw new NoSuchUniqueName(type.toValue(), name);
      }
    }
    return tsdb_store.getId(name, type).addCallback(new GetIdCB());
  }

  private byte[] getIdFromCache(final String name) {
    return name_cache.get(name);
  }

  private void addIdToCache(final String name, final byte[] id) {
    byte[] found = name_cache.get(name);
    if (found == null) {
      found = name_cache.putIfAbsent(name,
                                    // Must make a defensive copy to be immune
                                    // to any changes the caller may do on the
                                    // array later on.
                                    Arrays.copyOf(id, id.length));
    }
    if (found != null && !Arrays.equals(found, id)) {
      throw new IllegalStateException("name=" + name + " => id="
          + Arrays.toString(id) + ", already mapped to "
          + Arrays.toString(found));
    }
  }

  /** Adds the bidirectional mapping in the cache. */
  private void cacheMapping(final String name, final byte[] id) {
    addIdToCache(name, id);
    addNameToCache(id, name);
  }

  /**
   * Create an id with the specified name.
   * @param name The name of the new id
   * @return A deferred with the byte uid if the id was successfully created
   */
  public Deferred<byte[]> createId(final String name) {
    Deferred<byte[]> assignment;
    synchronized (pending_assignments) {
      assignment = pending_assignments.get(name);
      if (assignment == null) {
        // to prevent UID leaks that can be caused when multiple time
        // series for the same metric or tags arrive, we need to write a
        // deferred to the pending map as quickly as possible. Then we can
        // start the assignment process after we've stashed the deferred
        // and released the lock
        assignment = new Deferred<byte[]>();
        pending_assignments.put(name, assignment);
      } else {
        LOG.info("Already waiting for UID assignment: {}", name);
        return assignment;
      }
    }

    // start the assignment dance after stashing the deferred
    Deferred<byte[]> uid = tsdb_store.allocateUID(name, type);

    uid.addCallback(new Callback<Object, byte[]>() {
      @Override
      public byte[] call(byte[] uid) throws Exception {
        cacheMapping(name, uid);

        LOG.info("Completed pending assignment for: {}", name);
        synchronized (pending_assignments) {
          pending_assignments.remove(name);
        }

        idEventBus.post(new IdCreatedEvent(uid, name, type));

        return uid;
      }
    });

    return uid;
  }

  /**
   * Reassigns the UID to a different name (non-atomic).
   * <p>
   * Whatever was the UID of {@code oldname} will be given to {@code newname}.
   * {@code oldname} will no longer be assigned a UID.
   * <p>
   * Beware that the assignment change is <b>not atommic</b>.  If two threads
   * or processes attempt to rename the same UID differently, the result is
   * unspecified and might even be inconsistent.  This API is only here for
   * administrative purposes, not for normal programmatic interactions.
   * @param oldname The old name to rename.
   * @param newname The new name.
   * @throws NoSuchUniqueName if {@code oldname} wasn't assigned.
   * @throws IllegalArgumentException if {@code newname} was already assigned.
   */
  public Deferred<Object> rename(final String oldname, final String newname) {
    return checkUidExists(newname).addCallback(new Callback<Object, Boolean>() {
      @Override
      public Object call(final Boolean exists) {
        if (exists) {
          throw new IllegalArgumentException("An UID with name " + newname + " " +
                  "for " + type + " already exists");
        }

        return getId(oldname).addCallbackDeferring(new Callback<Deferred<Object>, byte[]>() {
          @Override
          public Deferred<Object> call(final byte[] old_uid) {
            tsdb_store.allocateUID(newname, old_uid, type);

            // Update cache.
            addIdToCache(newname, old_uid);            // add     new name -> ID
            id_cache.put(StringCoder.fromBytes(old_uid), newname);  // update  ID -> new name
            name_cache.remove(oldname);             // remove  old name -> ID

            // Delete the old forward mapping.
            return tsdb_store.deleteUID(toBytes(oldname), type);
          }
        });
      }
    });
  }

  private Deferred<Boolean> checkUidExists(String newname) {
    class NoId implements Callback<Boolean, byte[]> {
      @Override
      public Boolean call(byte[] uid) throws Exception {
        return uid != null;
      }
    }

    class NoSuchId implements Callback<Object, Exception> {
      @Override
      public Object call(Exception e) throws Exception {
        return null;
      }
    }

    return getId(newname).addCallbacks(new NoId(), new NoSuchId());
  }

  /** Returns a human readable string representation of the object. */
  public String toString() {
    return MoreObjects.toStringHelper(this)
            .add("type", type)
            .add("id_width", id_width)
            .toString();
  }

  /**
   * Converts a byte array to a hex encoded, upper case string with padding
   * @param uid The ID to convert
   * @return the UID as a hex string
   * @throws NullPointerException if the ID was null
   * @since 2.0
   */
  public static String uidToString(final byte[] uid) {
    return DatatypeConverter.printHexBinary(uid);
  }

  /**
   * Converts a hex string to a byte array
   * If the {@code uid} is less than {@code uid_length * 2} characters wide, it
   * will be padded with 0s to conform to the spec. E.g. if the tagk width is 3
   * and the given {@code uid} string is "1", the string will be padded to
   * "000001" and then converted to a byte array to reach 3 bytes.
   * All {@code uid}s are padded to 1 byte. If given "1", and {@code uid_length}
   * is 0, the uid will be padded to "01" then converted.
   * @param uid The UID to convert
   * @return The UID as a byte array
   * @throws NullPointerException if the ID was null
   * @throws IllegalArgumentException if the string is not valid hex
   * @since 2.0
   */
  public static byte[] stringToUid(final String uid) {
    return stringToUid(uid, (short) 0);
  }

  /**
   * Converts a UID to an integer value. The array must be the same length as
   * uid_length or an exception will be thrown.
   * @param uid The byte array to convert
   * @param uid_length Length the array SHOULD be according to the UID config
   * @return The UID converted to an integer
   * @throws IllegalArgumentException if the length of the byte array does not
   * match the uid_length value
   * @since 2.1
   */
  public static long uidToLong(final byte[] uid, final short uid_length) {
    if (uid.length != uid_length) {
      throw new IllegalArgumentException("UID was " + uid.length
          + " bytes long but expected to be " + uid_length);
    }

    final byte[] uid_raw = new byte[8];
    System.arraycopy(uid, 0, uid_raw, 8 - uid_length, uid_length);
    return Bytes.getLong(uid_raw);
  }

  /**
   * Converts a UID to an integer value. The array must be the same length as
   * uid_length or an exception will be thrown.
   * @param uid The byte array to convert
   * @param uid_length Length the array SHOULD be according to the UID config
   * @return The UID converted to an integer
   * @throws IllegalArgumentException if the length of the byte array does not
   * match the uid_length value
   * @since 2.1
   */
  public static long uidToLong(final byte[] uid) {
    final byte[] uid_raw = new byte[8];
    System.arraycopy(uid, 0, uid_raw, 8 - uid.length, uid.length);
    return Bytes.getLong(uid_raw);
  }

  /**
   * Converts a Long to a byte array with the proper UID width
   * @param uid The UID to convert
   * @param width The width of the UID in bytes
   * @return The UID as a byte array
   * @throws IllegalStateException if the UID is larger than the width would
   * @throws IllegalArgumentException if width <= 0.
   * allow
   * @since 2.1
   */
  public static byte[] longToUID(final long uid, final short width) {
    checkArgument(width > 0, "width can't be negative");

    final byte[] padded = Bytes.fromLong(uid);

    // Verify that we're going to drop bytes that are 0.
    for (int i = 0; i < padded.length - width; i++) {
      if (padded[i] != 0) {
        final String message = "UID " + Long.toString(uid) +
          " was too large for " + width + " bytes";
        LOG.error("OMG {}", message);
        throw new IllegalStateException(message);
      }
    }

    // Shrink the ID on the requested number of bytes.
    return Arrays.copyOfRange(padded, padded.length - width, padded.length);
  }

  /**
   * Appends the given UID to the given string buffer, followed by "\\E".
   * @param buf The buffer to append
   * @param id The UID to add as a binary regex pattern
   * @since 2.1
   */
  public static void addIdToRegexp(final StringBuilder buf, final byte[] id) {
    boolean backslash = false;
    for (final byte b : id) {
      buf.append((char) (b & 0xFF));
      if (b == 'E' && backslash) {  // If we saw a `\' and now we have a `E'.
        // So we just terminated the quoted section because we just added \E
        // to `buf'.  So let's put a literal \E now and start quoting again.
        buf.append("\\\\E\\Q");
      } else {
        backslash = b == '\\';
      }
    }
    buf.append("\\E");
  }

  /**
   * Converts a hex string to a byte array
   * If the {@code uid} is less than {@code uid_length * 2} characters wide, it
   * will be padded with 0s to conform to the spec. E.g. if the tagk width is 3
   * and the given {@code uid} string is "1", the string will be padded to
   * "000001" and then converted to a byte array to reach 3 bytes.
   * All {@code uid}s are padded to 1 byte. If given "1", and {@code uid_length}
   * is 0, the uid will be padded to "01" then converted.
   * @param uid The UID to convert
   * @param uid_length An optional length, in bytes, that the UID must conform
   * to. Set to 0 if not used.
   * @return The UID as a byte array
   * @throws NullPointerException if the ID was null
   * @throws IllegalArgumentException if the string is not valid hex
   * @since 2.0
   */
  public static byte[] stringToUid(final String uid, final short uid_length) {
    if (uid == null || uid.isEmpty()) {
      throw new IllegalArgumentException("UID was empty");
    }
    String id = uid;
    if (uid_length > 0) {
      while (id.length() < uid_length * 2) {
        id = "0" + id;
      }
    } else {
      if (id.length() % 2 > 0) {
        id = "0" + id;
      }
    }
    return DatatypeConverter.parseHexBinary(id);
  }

  /**
   * Extracts a metric from the tsuid.
   * @param tsuid The tsuid to parse.
   * @return A byte array representing the metric.
   * @throws IllegalArgumentException if the TSUID is malformed
   */
  public static byte[] getMetricFromTSUID(final String tsuid){
    if (Strings.isNullOrEmpty(tsuid)){
      throw new IllegalArgumentException("Missing TSUID");
    }
    if (tsuid.length() <= Const.METRICS_WIDTH * 2) {
      throw new IllegalArgumentException(
              "TSUID is too short, may be missing tags");
    }
    return UniqueId.stringToUid(tsuid.substring(0, Const.METRICS_WIDTH * 2));
  }

  /**
   * Extracts a list of tagks and tagvs as individual values in a list
   * @param tsuid The tsuid to parse
   * @return A list of tagk/tagv UIDs alternating with tagk, tagv, tagk, tagv
   * @throws IllegalArgumentException if the TSUID is malformed
   * @since 2.1
   */
  public static List<byte[]> getTagsFromTSUID(final String tsuid) {
    if (tsuid == null || tsuid.isEmpty()) {
      throw new IllegalArgumentException("Missing TSUID");
    }
    if (tsuid.length() <= Const.METRICS_WIDTH * 2) {
      throw new IllegalArgumentException(
          "TSUID is too short, may be missing tags");
    }

    final List<byte[]> tags = new ArrayList<byte[]>();
    final int pair_width = (Const.TAG_NAME_WIDTH * 2) + (Const.TAG_VALUE_WIDTH * 2);

    // start after the metric then iterate over each tagk/tagv pair
    for (int i = Const.METRICS_WIDTH * 2; i < tsuid.length(); i+= pair_width) {
      if (i + pair_width > tsuid.length()){
        throw new IllegalArgumentException(
            "The TSUID appears to be malformed, improper tag width");
      }
      String tag = tsuid.substring(i, i + (Const.TAG_NAME_WIDTH * 2));
      tags.add(UniqueId.stringToUid(tag));
      tag = tsuid.substring(i + (Const.TAG_NAME_WIDTH * 2), i + pair_width);
      tags.add(UniqueId.stringToUid(tag));
    }
    return tags;
  }

  /**
   * Extracts a list of tagk/tagv pairs from a tsuid
   * @param tsuid The tsuid to parse
   * @return A list of tagk/tagv UID pairs
   * @throws IllegalArgumentException if the TSUID is malformed
   * @since 2.0
   */
  public static List<byte[]> getTagPairsFromTSUID(final byte[] tsuid) {
    if (tsuid == null) {
      throw new IllegalArgumentException("Missing TSUID");
    }
    if (tsuid.length <= Const.METRICS_WIDTH) {
      throw new IllegalArgumentException(
          "TSUID is too short, may be missing tags");
    }

    final List<byte[]> tags = new ArrayList<byte[]>();
    final int pair_width = Const.TAG_NAME_WIDTH + Const.TAG_VALUE_WIDTH;

    // start after the metric then iterate over each tagk/tagv pair
    for (int i = Const.METRICS_WIDTH; i < tsuid.length; i+= pair_width) {
      if (i + pair_width > tsuid.length){
        throw new IllegalArgumentException(
            "The TSUID appears to be malformed, improper tag width");
      }
      tags.add(Arrays.copyOfRange(tsuid, i, i + pair_width));
    }
    return tags;
  }

  /**
   * Pre-load UID caches, scanning up to "tsd.core.preload_uid_cache.max_entries"
   * rows from the UID table.
   * @param uniqueIdInstances A map of {@link net.opentsdb.uid.UniqueId} objects keyed on the kind.
   * @2.1
   */
  public static void preloadUidCache(final Config config,
                                     final TsdbStore store,
                                     final Map<UniqueIdType,UniqueId> uniqueIdInstances) {
    int max_results = config.getInt("tsd.core.preload_uid_cache.max_entries");
    LOG.info("Preloading uid cache with max_results={}", max_results);

    if (max_results <= 0) {
      return;
    }

    try {
      IdQuery idQuery = new IdQuery(null, null, max_results);
      List<IdentifierDecorator> ids = store.executeIdQuery(idQuery).join();

      for (final IdentifierDecorator id : ids) {
        LOG.debug("Preloaded {}", id);
        UniqueId uid_cache = uniqueIdInstances.get(id.getType());
        uid_cache.cacheMapping(id.getName(), id.getId());
      }

      for (UniqueId unique_id_table : uniqueIdInstances.values()) {
        LOG.info("After preloading, uid cache '{}' has {} ids and {} names.",
                unique_id_table.type,
                unique_id_table.id_cache.size(),
                unique_id_table.name_cache.size());
      }
    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }
}
