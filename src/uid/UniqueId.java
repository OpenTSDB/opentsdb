// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
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

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.RowLock;
import org.hbase.async.RowLockRequest;
import org.hbase.async.Scanner;

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
  /** How many time do we try to apply an edit before giving up. */
  private static final short MAX_ATTEMPTS_PUT = 6;
  /** Initial delay in ms for exponential backoff to retry failed RPCs. */
  private static final short INITIAL_EXP_BACKOFF_DELAY = 800;
  /** Maximum number of results to return in suggest(). */
  private static final short MAX_SUGGESTIONS = 25;

  /** HBase client to use.  */
  private final HBaseClient client;
  /** Table where IDs are stored.  */
  private final byte[] table;
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
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @param kind The kind of Unique ID this instance will deal with.
   * @param width The number of bytes on which Unique IDs should be encoded.
   * @throws IllegalArgumentException if width is negative or too small/large
   * or if kind is an empty string.
   */
  public UniqueId(final HBaseClient client, final byte[] table, final String kind,
                  final int width) {
    this.client = client;
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
    HBaseException hbe = null;

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
      } catch (HBaseException e) {
        try {
          Thread.sleep(61000 / MAX_ATTEMPTS_ASSIGN_ID);
        } catch (InterruptedException ie) {
          break;  // We've been asked to stop here, let's bail out.
        }
        hbe = e;
        continue;
      }
      if (lock == null) {  // Should not happen.
        LOG.error("WTF, got a null pointer as a RowLock!");
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
          // We want to send an ICV with our explicit RowLock, but HBase's RPC
          // interface doesn't expose this interface.  Since an ICV would
          // attempt to lock the row again, and we already locked it, we can't
          // use ICV here, we have to do it manually while we hold the RowLock.
          // To be fixed by HBASE-2292.
          { // HACK HACK HACK
            {
              final byte[] current_maxid = hbaseGet(MAXID_ROW, ID_FAMILY, lock);
              if (current_maxid != null) {
                if (current_maxid.length == 8) {
                  id = Bytes.getLong(current_maxid) + 1;
                } else {
                  throw new IllegalStateException("invalid current_maxid="
                      + Arrays.toString(current_maxid));
                }
              } else {
                id = 1;
              }
              row = Bytes.fromLong(id);
            }
            final PutRequest update_maxid = new PutRequest(
              table, MAXID_ROW, ID_FAMILY, kind, row, lock);
            hbasePutWithRetry(update_maxid, MAX_ATTEMPTS_PUT,
                              INITIAL_EXP_BACKOFF_DELAY);
          } // end HACK HACK HACK.
          LOG.info("Got ID=" + id
                   + " for kind='" + kind() + "' name='" + name + "'");
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
        } catch (HBaseException e) {
          LOG.error("Failed to assign an ID, ICV on row="
                    + Arrays.toString(MAXID_ROW) + " column='" +
                    fromBytes(ID_FAMILY) + ':' + kind() + '\'', e);
          hbe = e;
          continue;
        } catch (IllegalStateException e) {
          throw e;  // To avoid handling this exception in the next `catch'.
        } catch (Exception e) {
          LOG.error("WTF?  Unexpected exception type when assigning an ID,"
                    + " ICV on row=" + Arrays.toString(MAXID_ROW) + " column='"
                    + fromBytes(ID_FAMILY) + ':' + kind() + '\'', e);
          continue;
        }
        // If we die before the next PutRequest succeeds, we just waste an ID.

        // Create the reverse mapping first, so that if we die before creating
        // the forward mapping we don't run the risk of "publishing" a
        // partially assigned ID.  The reverse mapping on its own is harmless
        // but the forward mapping without reverse mapping is bad.
        try {
          final PutRequest reverse_mapping = new PutRequest(
            table, row, NAME_FAMILY, kind, toBytes(name));
          hbasePutWithRetry(reverse_mapping, MAX_ATTEMPTS_PUT,
                            INITIAL_EXP_BACKOFF_DELAY);
        } catch (HBaseException e) {
          LOG.error("Failed to Put reverse mapping!  ID leaked: " + id, e);
          hbe = e;
          continue;
        }

        // Now create the forward mapping.
        try {
          final PutRequest forward_mapping = new PutRequest(
            table, toBytes(name), ID_FAMILY, kind, row);
          hbasePutWithRetry(forward_mapping, MAX_ATTEMPTS_PUT,
                            INITIAL_EXP_BACKOFF_DELAY);
        } catch (HBaseException e) {
          LOG.error("Failed to Put forward mapping!  ID leaked: " + id, e);
          hbe = e;
          continue;
        }

        addIdToCache(name, row);
        addNameToCache(row, name);
        return row;
      } finally {
        unlock(lock);
      }
    }
    if (hbe == null) {
      throw new IllegalStateException("Should never happen!");
    }
    LOG.error("Failed to assign an ID for kind='" + kind()
              + "' name='" + name + "'", hbe);
    throw hbe;
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
  public List<String> suggest(final String search) throws HBaseException {
    // TODO(tsuna): Add caching to try to avoid re-scanning the same thing.
    final Scanner scanner = getSuggestScanner(search);
    final LinkedList<String> suggestions = new LinkedList<String>();
    try {
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          if (row.size() != 1) {
            LOG.error("WTF shouldn't happen!  Scanner " + scanner + " returned"
                      + " a row that doesn't have exactly 1 KeyValue: " + row);
            if (row.isEmpty()) {
              continue;
            }
          }
          final byte[] key = row.get(0).key();
          final String name = fromBytes(key);
          final byte[] id = row.get(0).value();
          final byte[] cached_id = nameCache.get(name);
          if (cached_id == null) {
            addIdToCache(name, id);
            addNameToCache(id, name);
          } else if (!Arrays.equals(id, cached_id)) {
            throw new IllegalStateException("WTF?  For kind=" + kind()
              + " name=" + name + ", we have id=" + Arrays.toString(cached_id)
              + " in cache, but just scanned id=" + Arrays.toString(id));
          }
          suggestions.add(name);
          if ((short) suggestions.size() > MAX_SUGGESTIONS) {
            break;
          }
        }
      }
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
    return suggestions;
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
  public void rename(final String oldname, final String newname) {
    final byte[] row = getId(oldname);
    {
      byte[] id = null;
      try {
        id = getId(newname);
      } catch (NoSuchUniqueName e) {
        // OK, we don't want the new name to be assigned.
      }
      if (id != null) {
        throw new IllegalArgumentException("When trying rename(\"" + oldname
          + "\", \"" + newname + "\") on " + this + ": new name already"
          + " assigned ID=" + Arrays.toString(id));
      }
    }

    final byte[] newnameb = toBytes(newname);

    // Update the reverse mapping first, so that if we die before updating
    // the forward mapping we don't run the risk of "publishing" a
    // partially assigned ID.  The reverse mapping on its own is harmless
    // but the forward mapping without reverse mapping is bad.
    try {
      final PutRequest reverse_mapping = new PutRequest(
        table, row, NAME_FAMILY, kind, newnameb);
      hbasePutWithRetry(reverse_mapping, MAX_ATTEMPTS_PUT,
                        INITIAL_EXP_BACKOFF_DELAY);
    } catch (HBaseException e) {
      LOG.error("When trying rename(\"" + oldname
        + "\", \"" + newname + "\") on " + this + ": Failed to update reverse"
        + " mapping for ID=" + Arrays.toString(row), e);
      throw e;
    }

    // Now create the new forward mapping.
    try {
      final PutRequest forward_mapping = new PutRequest(
        table, newnameb, ID_FAMILY, kind, row);
      hbasePutWithRetry(forward_mapping, MAX_ATTEMPTS_PUT,
                        INITIAL_EXP_BACKOFF_DELAY);
    } catch (HBaseException e) {
      LOG.error("When trying rename(\"" + oldname
        + "\", \"" + newname + "\") on " + this + ": Failed to create the"
        + " new forward mapping with ID=" + Arrays.toString(row), e);
      throw e;
    }

    // Update cache.
    addIdToCache(newname, row);            // add     new name -> ID
    idCache.put(fromBytes(row), newname);  // update  ID -> new name
    nameCache.remove(oldname);             // remove  old name -> ID

    // Delete the old forward mapping.
    try {
      final DeleteRequest old_forward_mapping = new DeleteRequest(
        table, toBytes(oldname), ID_FAMILY, kind);
      client.delete(old_forward_mapping).joinUninterruptibly();
    } catch (HBaseException e) {
      LOG.error("When trying rename(\"" + oldname
        + "\", \"" + newname + "\") on " + this + ": Failed to remove the"
        + " old forward mapping for ID=" + Arrays.toString(row), e);
      throw e;
    } catch (Exception e) {
      final String msg = "Unexpected exception when trying rename(\"" + oldname
        + "\", \"" + newname + "\") on " + this + ": Failed to remove the"
        + " old forward mapping for ID=" + Arrays.toString(row);
      LOG.error("WTF?  " + msg, e);
      throw new RuntimeException(msg, e);
    }
    // Success!
  }

  /** The start row to scan on empty search strings.  `!' = first ASCII char. */
  private static final byte[] START_ROW = new byte[] { '!' };

  /** The end row to scan on empty search strings.  `~' = last ASCII char. */
  private static final byte[] END_ROW = new byte[] { '~' };

  /**
   * Creates a scanner that scans the right range of rows for suggestions.
   */
  private Scanner getSuggestScanner(final String search) {
    final byte[] start_row = search.isEmpty() ? START_ROW : toBytes(search);
    final byte[] end_row = (search.isEmpty() ? END_ROW
                            : Arrays.copyOf(start_row, start_row.length));
    end_row[start_row.length - 1]++;
    final Scanner scanner = client.newScanner(table);
    scanner.setStartKey(start_row);
    scanner.setStopKey(end_row);
    scanner.setFamily(ID_FAMILY);
    scanner.setQualifier(kind);
    scanner.setMaxNumRows(MAX_SUGGESTIONS);
    return scanner;
  }

  /** Gets an exclusive lock for on the table using the MAXID_ROW.
   * The lock expires after hbase.regionserver.lease.period ms
   * (default = 60000)
   * @throws HBaseException if the row lock couldn't be acquired.
   */
  private RowLock getLock() throws HBaseException {
    try {
      return client.lockRow(new RowLockRequest(table, MAXID_ROW)).joinUninterruptibly();
    } catch (HBaseException e) {
      LOG.warn("Failed to lock the `MAXID_ROW' row", e);
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }

  /** Releases the lock passed in argument. */
  private void unlock(final RowLock lock) {
    try {
      client.unlockRow(lock);
    } catch (HBaseException e) {
      LOG.error("Error while releasing the lock on row `MAXID_ROW'", e);
    }
  }

  /** Returns the cell of the specified row, using family:kind. */
  private byte[] hbaseGet(final byte[] row, final byte[] family) throws HBaseException {
    return hbaseGet(row, family, null);
  }

  /** Returns the cell of the specified row key, using family:kind. */
  private byte[] hbaseGet(final byte[] key, final byte[] family,
                          final RowLock lock) throws HBaseException {
    final GetRequest get = new GetRequest(table, key);
    if (lock != null) {
      get.withRowLock(lock);
    }
    get.family(family).qualifier(kind);
    try {
      final ArrayList<KeyValue> row = client.get(get).joinUninterruptibly();
      if (row == null || row.isEmpty()) {
        return null;
      }
      return row.get(0).value();
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }

  /**
   * Attempts to run the PutRequest given in argument, retrying if needed.
   *
   * Puts are synchronized.
   *
   * @param put The PutRequest to execute.
   * @param attemps The maximum number of attempts.
   * @param wait The initial amount of time in ms to sleep for after a
   * failure.  This amount is doubled after each failed attempt.
   * @throws HBaseException if all the attempts have failed.  This exception
   * will be the exception of the last attempt.
   */
  private void hbasePutWithRetry(final PutRequest put, short attempts, short wait)
    throws HBaseException {
    put.setBufferable(false);  // TODO(tsuna): Remove once this code is async.
    while (attempts-- > 0) {
      try {
        client.put(put).joinUninterruptibly();
        return;
      } catch (HBaseException e) {
        if (attempts > 0) {
          LOG.error("Put failed, attempts left=" + attempts
                    + " (retrying in " + wait + " ms), put=" + put, e);
          try {
            Thread.sleep(wait);
          } catch (InterruptedException ie) {
            throw new RuntimeException("interrupted", ie);
          }
          wait *= 2;
        } else {
          throw e;
        }
      } catch (Exception e) {
        LOG.error("WTF?  Unexpected exception type, put=" + put, e);
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
    return "UniqueId(" + fromBytes(table) + ", " + kind() + ", " + idWidth + ")";
  }

}
