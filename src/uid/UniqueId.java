// This file is part of OpenTSDB.
// Copyright (C) 2010  StumbleUpon, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.uid;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.util.Bytes;

import net.opentsdb.HBaseException;

/**
 * Thread-safe implementation of the {@link UniqueIdInterface}.
 * <p>
 * Don't attempt to use {@code equals()} or {@code hashCode()} on
 * this class.
 * @see UniqueIdInterface
 */
public final class UniqueId implements UniqueIdInterface {

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
  /** How many time do we try to apply a Put before giving up. */
  private static final short MAX_ATTEMPTS_PUT = 6;
  /** Initial delay in ms for exponential backoff to retry failed RPCs. */
  private static final short INITIAL_EXP_BACKOFF_DELAY = 800;

  /** HTable to use. */
  private final HTableInterface table;
  /** The kind of UniqueId, used as the column qualifier. */
  private final byte[] kind;
  /** Number of bytes on which each ID is encoded. */
  private final short idWidth;

  /** Cache for forward mappings (name to ID). */
  private final ConcurrentHashMap<String, byte[]> nameCache =
    new ConcurrentHashMap<String, byte[]>();
  /** Cache for backward mappings (ID to name).
   * The ID in the key is a byte[] converted to a String to be Comparable. */
  private final ConcurrentHashMap<String, String> idCache =
    new ConcurrentHashMap<String, String>();

  /** Number of times we avoided reading from HBase thanks to the cache. */
  private volatile int cacheHits;
  /** Number of times we had to read from HBase and populate the cache. */
  private volatile int cacheMisses;

  /**
   * Constructor.
   * @param table The HTable to use.
   * @param kind The kind of Unique ID this instance will deal with.
   * @param width The number of bytes on which Unique IDs should be encoded.
   * @throws IllegalArgumentException if width is negative or too small/large
   * or if kind is an empty string.
   */
  public UniqueId(final HTableInterface table, final String kind,
                  final int width) {
    this.table = table;
    if (kind.isEmpty()) {
      throw new IllegalArgumentException("Empty string as 'kind' argument!");
    }
    this.kind = toBytes(kind);
    if (width < 1 || width > 8) {
      throw new IllegalArgumentException("Invalid width: " + width);
    }
    this.idWidth = (short) width;
  }

  /** The number of times we avoided reading from HBase thanks to the cache. */
  public int cacheHits() {
    return cacheHits;
  }

  /** The number of times we had to read from HBase and populate the cache. */
  public int cacheMisses() {
    return cacheMisses;
  }

  /** Returns the number of elements stored in the internal cache. */
  public int cacheSize() {
    return nameCache.size() + idCache.size();
  }

  public String kind() {
    return fromBytes(kind);
  }

  public short width() {
    return idWidth;
  }

  public String getName(final byte[] id) throws NoSuchUniqueId, HBaseException {
    if (id.length != idWidth) {
      throw new IllegalArgumentException("Wrong id.length = " + id.length
                                         + " which is != " + idWidth
                                         + " required for '" + kind() + '\'');
    }
    String name = getNameFromCache(id);
    if (name != null) {
      cacheHits++;
    } else {
      cacheMisses++;
      name = getNameFromHBase(id);
      if (name == null) {
        throw new NoSuchUniqueId(kind(), id);
      }
      addNameToCache(id, name);
      addIdToCache(name, id);
    }
    return name;
  }

  private String getNameFromCache(final byte[] id) {
    return idCache.get(fromBytes(id));
  }

  private String getNameFromHBase(final byte[] id) throws HBaseException {
    final byte[] name = hbaseGet(id, NAME_FAMILY);
    return name == null ? null : fromBytes(name);
  }

  private void addNameToCache(final byte[] id, final String name) {
    final String key = fromBytes(id);
    String found = idCache.get(key);
    if (found == null) {
      found = idCache.putIfAbsent(key, name);
    }
    if (found != null && !found.equals(name)) {
      throw new IllegalStateException("id=" + Arrays.toString(id) + " => name="
          + name + ", already mapped to " + found);
    }
  }

  public byte[] getId(final String name) throws NoSuchUniqueName, HBaseException {
    byte[] id = getIdFromCache(name);
    if (id != null) {
      cacheHits++;
    } else {
      cacheMisses++;
      id = getIdFromHBase(name);
      if (id == null) {
        throw new NoSuchUniqueName(kind(), name);
      }
      if (id.length != idWidth) {
        throw new IllegalStateException("Found id.length = " + id.length
                                        + " which is != " + idWidth
                                        + " required for '" + kind() + '\'');
      }
      addIdToCache(name, id);
      addNameToCache(id, name);
    }
    return id;
  }

  private byte[] getIdFromCache(final String name) {
    return nameCache.get(name);
  }

  private byte[] getIdFromHBase(final String name) throws HBaseException {
    return hbaseGet(toBytes(name), ID_FAMILY);
  }

  private void addIdToCache(final String name, final byte[] id) {
    byte[] found = nameCache.get(name);
    if (found == null) {
      found = nameCache.putIfAbsent(name,
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

  public byte[] getOrCreateId(String name) throws HBaseException {
    short attempt = MAX_ATTEMPTS_ASSIGN_ID;
    IOException ioe = null;

    while (attempt-- > 0) {
      try {
        return getId(name);
      } catch (NoSuchUniqueName e) {
        LOG.info("Creating an ID for kind='" + kind()
                 + "' name='" + name + '\'');
      }

      // The dance to assign an ID.
      RowLock lock;
      try {
        lock = getLock();
      } catch (IOException e) {
        // TODO(tsuna): Replace 61000 with
        //   conf.get("hbase.regionserver.lease.period") + 1000
        // once there's an API to retrieve the Configuration object of an HTable.
        // => HBASE-2246
        try {
          Thread.sleep(61000 / MAX_ATTEMPTS_ASSIGN_ID);
        } catch (InterruptedException ie) {
          break;  // We've been asked to stop here, let's bail out.
        }
        ioe = e;
        continue;
      }
      if (lock == null) {  // Should not happen.
        LOG.error("WTF, got a null pointer as a RowLock!");
        ioe = new IOException("WTF null RowLock!");
        continue;
      }
      // We now have hbase.regionserver.lease.period ms to complete the loop.

      try {
        // Verify that the row still doesn't exist (to avoid re-creating it if
        // it got created before we acquired the lock due to a race condition).
        try {
          final byte[] id = getId(name);
          LOG.info("Race condition, found ID for kind='" + kind()
                   + "' name='" + name + '\'');
          return id;
        } catch (NoSuchUniqueName e) {
          // OK, the row still doesn't exist, let's create it now.
        }

        // Assign an ID.
        long id;     // The ID.
        byte row[];  // The same ID, as a byte array.
        try {
          // We want to do this, but can't:
          // id = table.incrementColumnValue(MAXID_ROW, ID_FAMILY, kind, 1);
          // Because incrementColumnValue attempts to lock MAXID_ROW, but we
          // already locked it, and there's no API to specify our own lock.
          // To be fixed by HBASE-2292.
          { // HACK HACK HACK
            {
              final byte[] current_maxid = hbaseGet(MAXID_ROW, ID_FAMILY, lock);
              if (current_maxid != null) {
                if (current_maxid.length == 8) {
                  id = Bytes.toLong(current_maxid) + 1;
                } else {
                  throw new IllegalStateException("invalid current_maxid="
                      + Arrays.toString(current_maxid));
                }
              } else {
                id = 1;
              }
              row = Bytes.toBytes(id);
            }
            final Put update_maxid = new Put(MAXID_ROW, lock);
            update_maxid.add(ID_FAMILY, kind, row);
            hbasePutWithRetry(update_maxid, MAX_ATTEMPTS_PUT,
                              INITIAL_EXP_BACKOFF_DELAY);
          } // end HACK HACK HACK.
          LOG.info("Got ID=" + id
                   + " for kind='" + kind() + "' name='" + name + "'");
          row = Bytes.toBytes(id);
          // row.length should actually be 8.
          if (row.length < idWidth) {
            throw new IllegalStateException("OMG, row.length = " + row.length
                                            + " which is less than " + idWidth
                                            + " for id=" + id
                                            + " row=" + Arrays.toString(row));
          }
          // Verify that we're going to drop bytes that are 0.
          for (int i = 0; i < row.length - idWidth; i++) {
            if (row[i] != 0) {
              final String message = "All Unique IDs for " + kind()
                + " on " + idWidth + " bytes are already assigned!";
              LOG.error("OMG " + message);
              throw new IllegalStateException(message);
            }
          }
          // Shrink the ID on the requested number of bytes.
          row = Arrays.copyOfRange(row, row.length - idWidth, row.length);
        } catch (IOException e) {
          LOG.error("Failed to assign an ID, ICV on row='"
                    + fromBytes(MAXID_ROW) + "' column='" + fromBytes(ID_FAMILY)
                    + ':' + kind() + '\'', e);
          ioe = e;
          continue;
        }
        // The following catch is related to the 'HACK HACK HACK' above.
        catch (HBaseException e) {
          ioe = (IOException) e.getCause();
          LOG.error("Failed to assign an ID, ICV on row='"
                    + fromBytes(MAXID_ROW) + "' column='" + fromBytes(ID_FAMILY)
                    + ':' + kind() + '\'', ioe);
          continue;
        }
        // If we die before the next Put is committed, we just waste an ID.

        // Create the reverse mapping first, so that if we die before creating
        // the forward mapping we don't run the risk of "publishing" a
        // partially assigned ID.  The reverse mapping on its own is harmless
        // but the forward mapping without reverse mapping is bad.
        try {
          final Put reverse_mapping = new Put(row);
          reverse_mapping.add(NAME_FAMILY, kind, toBytes(name));
          hbasePutWithRetry(reverse_mapping, MAX_ATTEMPTS_PUT,
                            INITIAL_EXP_BACKOFF_DELAY);
        } catch (IOException e) {
          LOG.error("Failed to Put reverse mapping!  ID leaked: " + id, e);
          ioe = e;
          continue;
        }

        // Now create the forward mapping.
        try {
          final Put forward_mapping = new Put(toBytes(name));
          forward_mapping.add(ID_FAMILY, kind, row);
          hbasePutWithRetry(forward_mapping, MAX_ATTEMPTS_PUT,
                            INITIAL_EXP_BACKOFF_DELAY);
        } catch (IOException e) {
          LOG.error("Failed to Put forward mapping!  ID leaked: " + id, e);
          ioe = e;
          continue;
        }

        addIdToCache(name, row);
        addNameToCache(row, name);
        return row;
      } finally {
        unlock(lock);
      }
    }
    if (ioe == null) {
      throw new IllegalStateException("Should never happen!");
    }
    throw new HBaseException("Failed to assign an ID for kind='"
                             + kind() + "' name='" + name + "'", ioe);
  }

  /** Gets an exclusive lock for on the table using the MAXID_ROW.
   * The lock expires after hbase.regionserver.lease.period ms
   * (default = 60000)
   */
  private RowLock getLock() throws IOException {
    try {
      return table.lockRow(MAXID_ROW);
    } catch (IOException e) {
      LOG.warn("Failed to lock the '" + fromBytes(MAXID_ROW) + "' row", e);
      throw e;
    }
  }

  /** Releases the lock passed in argument. */
  private void unlock(final RowLock lock) {
    try {
      table.unlockRow(lock);
    } catch (IOException e) {
      LOG.error("Error while releasing the lock on row '"
                + fromBytes(MAXID_ROW) + "'", e);
    }
  }

  /** Returns the cell of the specified row, using family:kind. */
  private byte[] hbaseGet(final byte[] row, final byte[] family) throws HBaseException {
    return hbaseGet(row, family, null);
  }

  /** Returns the cell of the specified row, using family:kind. */
  private byte[] hbaseGet(final byte[] row, final byte[] family,
                          final RowLock lock) throws HBaseException {
    final Get get = lock == null ? new Get(row) : new Get(row, lock);
    get.addColumn(family, kind);
    try {
      return table.get(get).getValue(family, kind);
    } catch (IOException e) {
      // TODO(tsuna): Add some retry logic here.
      throw new HBaseException("HBase get(" + get + ") failed", e);
    }
  }

  /**
   * Attempts to run the Put given in argument, retrying if needed.
   *
   * Puts are synchronized.
   *
   * @param put The Put to execute.
   * @param attemps The maximum number of attempts.
   * @param wait The initial amount of time in ms to sleep for after a
   * failure.  This amount is doubled after each failed attempt.
   * @throws IOException if all the attempts have failed.  This exception
   * will be the exception of the last attempt.
   */
  private void hbasePutWithRetry(final Put put, short attempts, short wait)
    throws IOException {
    while (attempts-- > 0) {
      try {
        synchronized (table) {  // HTable isn't thread-safe for write ops.
          table.put(put);
        }
        return;
      } catch (IOException e) {
        if (attempts > 0) {
          LOG.error("Put failed, attempts left=" + attempts
                    + " (retrying in " + wait + " ms), put=" + put, e);
          try {
            Thread.sleep(wait);
          } catch (InterruptedException ie) {
            throw new IOException("interrupted", ie);
          }
          wait *= 2;
        } else {
          throw e;
        }
      }
    }
    throw new IllegalStateException("This code should never be reached!");
  }

  private static byte[] toBytes(final String s) {
    return s.getBytes(CHARSET);
  }

  private static String fromBytes(final byte[] b) {
    return new String(b, CHARSET);
  }

  /** Returns a human readable string representation of the object. */
  public String toString() {
    return "UniqueId(" + table + ", " + kind() + ", " + idWidth + ")";
  }

}
