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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.bind.DatatypeConverter;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.StringCoder;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.UIDMeta;

import net.opentsdb.storage.TsdbStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Const;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.hbase.async.Bytes.ByteMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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
  private volatile int cache_hits;
  /** Number of times we had to read from TsdbStore and populate the cache. */
  private volatile int cache_misses;

  /** Whether or not to generate new UIDMetas */
  private TSDB tsdb;
  
  /**
   * Constructor.
   * @param tsdb_store The TsdbStore to use.
   * @param table The name of the table to use.
   * @param type The type of UIDs this instance represents
   */
  public UniqueId(final TsdbStore tsdb_store, final byte[] table, UniqueIdType type) {
    this.tsdb_store = checkNotNull(tsdb_store);
    this.table = checkNotNull(table);
    this.type = checkNotNull(type);
    this.id_width = type.width;
  }

  /** The number of times we avoided reading from TsdbStore thanks to the cache. */
  public int cacheHits() {
    return cache_hits;
  }

  /** The number of times we had to read from TsdbStore and populate the cache. */
  public int cacheMisses() {
    return cache_misses;
  }

  /** Returns the number of elements stored in the internal cache. */
  public int cacheSize() {
    return name_cache.size() + id_cache.size();
  }

  public UniqueIdType type() {
    return type;
  }

  public short width() {
    return id_width;
  }

  /** @param tsdb Whether or not to track new UIDMeta objects */
  public void setTSDB(final TSDB tsdb) {
    this.tsdb = tsdb;
  }
  
  /** The largest possible ID given the number of bytes the IDs are represented on. */
  public long maxPossibleId() {
    return (1 << id_width * Byte.SIZE) - 1;
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
   * @see #getIdAsync(String)
   * @see #createId(String)
   * @throws NoSuchUniqueId if the given ID is not assigned.
   * @throws HBaseException if there is a problem communicating with HBase.
   * @throws IllegalArgumentException if the ID given in argument is encoded
   * on the wrong number of bytes.
   * @since 1.1
   */
  public Deferred<String> getNameAsync(final byte[] id) {
    final String name = getNameFromCache(id);
    if (name != null) {
      cache_hits++;
      return Deferred.fromResult(name);
    }
    cache_misses++;
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

  public Deferred<byte[]> getIdAsync(final String name) {
    final byte[] id = getIdFromCache(name);
    if (id != null) {
      cache_hits++;
      return Deferred.fromResult(id);
    }
    cache_misses++;
    class GetIdCB implements Callback<byte[], Optional<byte[]>> {
      public byte[] call(final Optional<byte[]> id) {
        if (id.isPresent()) {
          addIdToCache(name, id.get());
          addNameToCache(id.get(), name);
          return id.get();
        }

        throw new NoSuchUniqueName(type.qualifier, name);
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

        if (tsdb != null && tsdb.getConfig().enable_realtime_uid()) {
          final UIDMeta meta = new UIDMeta(type, uid, name);
          tsdb_store.add(meta);
          LOG.info("Wrote UIDMeta for: {}", name);
          tsdb.indexUIDMeta(meta);
        }

        return uid;
      }
    });

    return uid;
  }

  /**
   * Attempts to find suggestions of names given a search term.
   * @param search The search term (possibly empty).
   * @return A list of known valid names that have UIDs that sort of match
   * the search term.  If the search term is empty, returns the first few
   * terms.
   * @throws HBaseException if there was a problem getting suggestions from
   * HBase.
   */
  public Deferred<List<String>> suggest(final String search) throws HBaseException {
    return suggest(search, MAX_SUGGESTIONS);
  }

  /**
   * Attempts to find suggestions of names given a search term.
   * @param search The search term (possibly empty).
   * @return A list of known valid names that have UIDs that sort of match
   * the search term.  If the search term is empty, returns the first few
   * terms.
   * @throws HBaseException if there was a problem getting suggestions from
   * HBase.
   * @since 1.1
   */
  public Deferred<List<String>> suggest(final String search,
                                        final int max_results) {
    checkArgument(max_results > 0, "Count must be greater than 0 but was %s", max_results);
    return new SuggestCB(search, max_results).search();
  }

  /**
   * Helper callback to asynchronously scan TsdbStore for suggestions.
   */
  private final class SuggestCB
    implements Callback<Deferred<List<String>>, ArrayList<ArrayList<KeyValue>>> {
    private final List<String> suggestions = new LinkedList<String>();
    private final Scanner scanner;
    private final int max_results;

    SuggestCB(final String search, final int max_results) {
      this.max_results = max_results;
      this.scanner = getSuggestScanner(tsdb_store, table, search, type, max_results);
    }

    Deferred<List<String>> search() {
      return scanner.nextRows().addCallbackDeferring(this);
    }

    @Override
    public Deferred<List<String>> call(final ArrayList<ArrayList<KeyValue>> rows) {
      if (rows == null) {  // We're done scanning.
        return Deferred.fromResult(suggestions);
      }
      
      for (final ArrayList<KeyValue> row : rows) {
        if (row.size() != 1) {
          LOG.error("WTF shouldn't happen!  Scanner {} returned a row that " +
                  "doesn't have exactly 1 KeyValue: {}", scanner, row);
          if (row.isEmpty()) {
            continue;
          }
        }
        final byte[] key = row.get(0).key();
        final String name = StringCoder.fromBytes(key);
        final byte[] id = row.get(0).value();
        final byte[] cached_id = name_cache.get(name);
        if (cached_id == null) {
          cacheMapping(name, id); 
        } else if (!Arrays.equals(id, cached_id)) {
          throw new IllegalStateException("For type=" + type
            + " name=" + name + ", we have id=" + Arrays.toString(cached_id)
            + " in cache, but just scanned id=" + Arrays.toString(id));
        }
        suggestions.add(name);
        if ((short) suggestions.size() >= max_results) {  // We have enough.
          return Deferred.fromResult(suggestions);
        }
        row.clear();  // free()
      }
      return search();  // Get more suggestions.
    }
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
   * @throws HBaseException if there was a problem with HBase while trying to
   * update the mapping.
   */
  public Deferred<Object> rename(final String oldname, final String newname) {
    return checkUidExists(newname).addCallback(new Callback<Object, Boolean>() {
      @Override
      public Object call(final Boolean exists) {
        if (exists) {
          throw new IllegalArgumentException("An UID with name " + newname + " " +
                  "for " + type + " already exists");
        }

        return getIdAsync(oldname).addCallbackDeferring(new Callback<Deferred<Object>, byte[]>() {
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

    return getIdAsync(newname).addCallbacks(new NoId(), new NoSuchId());
  }

  /** The start row to scan on empty search strings.  `!' = first ASCII char. */
  private static final byte[] START_ROW = new byte[] { '!' };

  /** The end row to scan on empty search strings.  `~' = last ASCII char. */
  private static final byte[] END_ROW = new byte[] { '~' };

  /**
   * Creates a scanner that scans the right range of rows for suggestions.
   * @param tsdb_store The TsdbStore to use.
   * @param tsd_uid_table Table where IDs are stored.
   * @param search The string to start searching at
   * @param type The type of UID to search or null for any types.
   * @param max_results The max number of results to return
   */
  private static Scanner getSuggestScanner(final TsdbStore tsdb_store,
      final byte[] tsd_uid_table, final String search,
      final UniqueIdType type, final int max_results) {
    final byte[] start_row;
    final byte[] end_row;
    if (search.isEmpty()) {
      start_row = START_ROW;
      end_row = END_ROW;
    } else {
      start_row = toBytes(search);
      end_row = Arrays.copyOf(start_row, start_row.length);
      end_row[start_row.length - 1]++;
    }
    final Scanner scanner = tsdb_store.newScanner(tsd_uid_table);
    scanner.setStartKey(start_row);
    scanner.setStopKey(end_row);
    scanner.setFamily(ID_FAMILY);
    if (type != null) {
      scanner.setQualifier(type.qualifier.getBytes(Const.CHARSET_ASCII));
    }
    scanner.setMaxNumRows(max_results <= 4096 ? max_results : 4096);
    return scanner;
  }

  private static byte[] toBytes(final String s) {
    return s.getBytes(CHARSET);
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
   * Returns a map of max UIDs from storage for the given list of UID types 
   * @param tsdb The TSDB to which we belong
   * @param kinds A list of qualifiers to fetch
   * @return A map with the "kind" as the key and the maximum assigned UID as
   * the value
   * @since 2.0
   */
  public static Deferred<Map<String, Long>> getUsedUIDs(final TSDB tsdb,
      final byte[][] kinds) {
    
    /**
     * Returns a map with 0 if the max ID row hasn't been initialized yet, 
     * otherwise the map has actual data
     */
    final class GetCB implements Callback<Map<String, Long>, 
      ArrayList<KeyValue>> {

      @Override
      public Map<String, Long> call(final ArrayList<KeyValue> row)
          throws Exception {
        
        final Map<String, Long> results = new HashMap<String, Long>(3);
        if (row == null || row.isEmpty()) {
          // it could be the case that this is the first time the TSD has run
          // and the user hasn't put any metrics in, so log and return 0s
          LOG.info("Could not find the UID assignment row");
          for (final byte[] kind : kinds) {
            results.put(new String(kind, CHARSET), 0L);
          }
          return results;
        }
        
        for (final KeyValue column : row) {
          results.put(new String(column.qualifier(), CHARSET), 
              Bytes.getLong(column.value()));
        }
        
        // if the user is starting with a fresh UID table, we need to account
        // for missing columns
        for (final byte[] kind : kinds) {
          if (results.get(new String(kind, CHARSET)) == null) {
            results.put(new String(kind, CHARSET), 0L);
          }
        }
        return results;
      }
      
    }
    
    final GetRequest get = new GetRequest(tsdb.uidTable(), MAXID_ROW);
    get.family(ID_FAMILY);
    get.qualifiers(kinds);
    return tsdb.getTsdbStore().get(get).addCallback(new GetCB());
  }

  /**
   * Pre-load UID caches, scanning up to "tsd.core.preload_uid_cache.max_entries"
   * rows from the UID table.
   * @param tsdb The TSDB to use 
   * @param uid_cache_map A map of {@link UniqueId} objects keyed on the kind.
   * @throws HBaseException Passes any HBaseException from HBase scanner.
   * @throws RuntimeException Wraps any non HBaseException from HBase scanner.
   * @2.1
   */
  public static void preloadUidCache(final TSDB tsdb,
      final ByteMap<UniqueId> uid_cache_map) throws HBaseException {
    int max_results = tsdb.getConfig().getInt(
        "tsd.core.preload_uid_cache.max_entries");
    LOG.info("Preloading uid cache with max_results={}", max_results);
    if (max_results <= 0) {
      return;
    }
    Scanner scanner = null;
    try {
      int num_rows = 0;
      scanner = getSuggestScanner(tsdb.getTsdbStore(), tsdb.uidTable(), "", null,
          max_results);
      for (ArrayList<ArrayList<KeyValue>> rows = scanner.nextRows().join();
          rows != null;
          rows = scanner.nextRows().join()) {
        for (final ArrayList<KeyValue> row : rows) {
          for (KeyValue kv: row) {
            final String name = StringCoder.fromBytes(kv.key());
            final byte[] kind = kv.qualifier();
            final byte[] id = kv.value();
            LOG.debug("id='{}', name='{}', kind='{}'", Arrays.toString(id),
                name, StringCoder.fromBytes(kind));
            UniqueId uid_cache = uid_cache_map.get(kind);
            if (uid_cache != null) {
              uid_cache.cacheMapping(name, id);
            }
          }
          num_rows += row.size();
          row.clear();  // free()
          if (num_rows >= max_results) {
            break;
          }
        }
      }
      for (UniqueId unique_id_table : uid_cache_map.values()) {
        LOG.info("After preloading, uid cache '{}' has {} ids and {} names.",
                 unique_id_table.type,
                 unique_id_table.id_cache.size(),
                 unique_id_table.name_cache.size());
      }
    } catch (Exception e) {
      if (e instanceof HBaseException) {
        throw (HBaseException)e;
      } else if (e instanceof RuntimeException) {
        throw (RuntimeException)e;
      } else {
        throw new RuntimeException("Error while preloading IDs", e);
      }
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }
  }
}
