// This file is part of OpenTSDB.
// Copyright (C) 2011-2012  The OpenTSDB Authors.
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
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hbase.async.Bytes;
import org.hbase.async.HBaseRpc;
import org.hbase.async.KeyValue;
import org.hbase.async.PleaseThrottleException;

import net.opentsdb.stats.StatsCollector;

/**
 * "Queue" of rows to compact.
 * <p>
 * Whenever we write a data point to HBase, the row key we write to is added
 * to this queue, which is effectively a sorted set.  There is a separate
 * thread that periodically goes through the queue and look for "old rows" to
 * compact.  A row is considered "old" if the timestamp in the row key is
 * older than a certain threshold.
 * <p>
 * The compaction process consists in reading all the cells within a given row
 * and writing them back out as a single big cell.  Once that writes succeeds,
 * we delete all the individual little cells.
 * <p>
 * This process is effective because in HBase the row key is repeated for
 * every single cell.  And because there is no way to efficiently append bytes
 * at the end of a cell, we have to do this instead.
 */
final class CompactionQueue extends ConcurrentSkipListMap<byte[], Boolean> {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionQueue.class);

  /**
   * How many items are currently in the queue.
   * Because {@link ConcurrentSkipListMap#size} has O(N) complexity.
   */
  private final AtomicInteger size = new AtomicInteger();

  private final AtomicLong trivial_compactions = new AtomicLong();
  private final AtomicLong complex_compactions = new AtomicLong();
  private final AtomicLong written_cells = new AtomicLong();
  private final AtomicLong deleted_cells = new AtomicLong();

  /** The {@code TSDB} instance we belong to. */
  private final TSDB tsdb;

  /** On how many bytes do we encode metrics IDs.  */
  private final short metric_width;

  /**
   * Constructor.
   * @param tsdb The TSDB we belong to.
   */
  public CompactionQueue(final TSDB tsdb) {
    super(new Cmp(tsdb));
    this.tsdb = tsdb;
    metric_width = tsdb.metrics.width();
    if (TSDB.enable_compactions) {
      startCompactionThread();
    }
  }

  @Override
  public int size() {
    return size.get();
  }

  public void add(final byte[] row) {
    if (super.put(row, Boolean.TRUE) == null) {
      size.incrementAndGet();  // We added a new entry, count it.
    }
  }

  /**
   * Forces a flush of the all old entries in the compaction queue.
   * @return A deferred that will be called back once everything has been
   * flushed (or something failed, in which case the deferred will carry the
   * exception).  In case of success, the kind of object returned is
   * unspecified.
   */
  public Deferred<ArrayList<Object>> flush() {
    final int size = size();
    if (size > 0) {
      LOG.info("Flushing all old outstanding rows out of " + size + " rows");
    }
    final long now = System.currentTimeMillis();
    return flush(now / 1000 - Const.MAX_TIMESPAN - 1, Integer.MAX_VALUE);
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  void collectStats(final StatsCollector collector) {
    collector.record("compaction.count", trivial_compactions, "type=trivial");
    collector.record("compaction.count", complex_compactions, "type=complex");
    if (!TSDB.enable_compactions) {
      return;
    }
    // The remaining stats only make sense with compactions enabled.
    collector.record("compaction.queue.size", size);
    collector.record("compaction.errors", handle_read_error.errors, "rpc=read");
    collector.record("compaction.errors", handle_write_error.errors, "rpc=put");
    collector.record("compaction.errors", handle_delete_error.errors,
                     "rpc=delete");
    collector.record("compaction.writes", written_cells);
    collector.record("compaction.deletes", deleted_cells);
  }

  /**
   * Flushes all the rows in the compaction queue older than the cutoff time.
   * @param cut_off A UNIX timestamp in seconds (unsigned 32-bit integer).
   * @param maxflushes How many rows to flush off the queue at once.
   * This integer is expected to be strictly positive.
   * @return A deferred that will be called back once everything has been
   * flushed.
   */
  private Deferred<ArrayList<Object>> flush(final long cut_off, int maxflushes) {
    assert maxflushes > 0: "maxflushes must be > 0, but I got " + maxflushes;
    // We can't possibly flush more entries than size().
    maxflushes = Math.min(maxflushes, size());
    if (maxflushes == 0) {  // Because size() might be 0.
      return Deferred.fromResult(new ArrayList<Object>(0));
    }
    final ArrayList<Deferred<Object>> ds =
      new ArrayList<Deferred<Object>>(Math.min(maxflushes,
                                               MAX_CONCURRENT_FLUSHES));
    int nflushes = 0;
    for (final byte[] row : this.keySet()) {
      if (maxflushes == 0) {
        break;
      }
      final long base_time = Bytes.getUnsignedInt(row, metric_width);
      if (base_time > cut_off) {
        break;
      } else if (nflushes == MAX_CONCURRENT_FLUSHES) {
        // We kicked off the compaction of too many rows already, let's wait
        // until they're done before kicking off more.
        break;
      }
      // You'd think that it would be faster to grab an iterator on the map
      // and then call remove() on the iterator to "unlink" the element
      // directly from where the iterator is at, but no, the JDK implements
      // it by calling remove(key) so it has to lookup the key again anyway.
      if (super.remove(row) == null) {  // We didn't remove anything.
        continue;  // So someone else already took care of this entry.
      }
      nflushes++;
      maxflushes--;
      size.decrementAndGet();
      ds.add(tsdb.get(row).addCallbacks(compactcb, handle_read_error));
    }
    final Deferred<ArrayList<Object>> group = Deferred.group(ds);
    if (nflushes == MAX_CONCURRENT_FLUSHES && maxflushes > 0) {
      // We're not done yet.  Once this group of flushes completes, we need
      // to kick off more.
      tsdb.flush();  // Speed up this batch by telling the client to flush.
      final int maxflushez = maxflushes;  // Make it final for closure.
      final class FlushMoreCB implements Callback<Deferred<ArrayList<Object>>,
                                                  ArrayList<Object>> {
        public Deferred<ArrayList<Object>> call(final ArrayList<Object> arg) {
          return flush(cut_off, maxflushez);
        }
        public String toString() {
          return "Continue flushing with cut_off=" + cut_off
            + ", maxflushes=" + maxflushez;
        }
      }
      group.addCallbackDeferring(new FlushMoreCB());
    }
    return group;
  }

  private final CompactCB compactcb = new CompactCB();

  /**
   * Callback to compact a row once it's been read.
   * <p>
   * This is used once the "get" completes, to actually compact the row and
   * write back the compacted version.
   */
  private final class CompactCB implements Callback<Object, ArrayList<KeyValue>> {
    public Object call(final ArrayList<KeyValue> row) {
      return compact(row, null);
    }
    public String toString() {
      return "compact";
    }
  }

  /**
   * Compacts a row into a single {@link KeyValue}.
   * @param row The row containing all the KVs to compact.
   * Must contain at least one element.
   * @return A compacted version of this row.
   */
  KeyValue compact(final ArrayList<KeyValue> row) {
    final KeyValue[] compacted = { null };
    compact(row, compacted);
    return compacted[0];
  }

  /**
   * Compacts a row into a single {@link KeyValue}.
   * <p>
   * If the {@code row} is empty, this function does literally nothing.
   * If {@code compacted} is not {@code null}, then the compacted form of this
   * {@code row} will be stored in {@code compacted[0]}.  Obviously, if the
   * {@code row} contains a single cell, then that cell is the compacted form.
   * Otherwise the compaction process takes places.
   * @param row The row containing all the KVs to compact.  Must be non-null.
   * @param compacted If non-null, the first item in the array will be set to
   * a {@link KeyValue} containing the compacted form of this row.
   * If non-null, we will also not write the compacted form back to HBase
   * unless the timestamp in the row key is old enough.
   * @return A {@link Deferred} if the compaction processed required a write
   * to HBase, otherwise {@code null}.
   */
  private Deferred<Object> compact(final ArrayList<KeyValue> row,
                                   final KeyValue[] compacted) {
    if (row.size() <= 1) {
      if (row.isEmpty()) {  // Maybe the row got deleted in the mean time?
        LOG.debug("Attempted to compact a row that doesn't exist.");
      } else if (compacted != null) {
        // no need to re-compact rows containing a single value.
        compacted[0] = row.get(0);
      }
      return null;
    }

    // We know we have at least 2 cells.  We need to go through all the cells
    // to determine what kind of compaction we're going to do.  If each cell
    // contains a single individual data point, then we can do a trivial
    // compaction.  Otherwise, we have a partially compacted row, and the
    // logic required to compact it is more complex.
    boolean write = true;  // Do we need to write a compacted cell?
    final KeyValue compact;
    {
      boolean trivial = true;  // Are we doing a trivial compaction?
      int qual_len = 0;  // Pre-compute the size of the qualifier we'll need.
      int val_len = 1;   // Reserve an extra byte for meta-data.
      short last_delta = -1;  // Time delta, extracted from the qualifier.
      KeyValue longest = row.get(0);  // KV with the longest qualifier.
      int longest_idx = 0;            // Index of `longest'.
      final int nkvs = row.size();
      for (int i = 0; i < nkvs; i++) {
        final KeyValue kv = row.get(i);
        final byte[] qual = kv.qualifier();
        // If the qualifier length isn't 2, this row might have already
        // been compacted, potentially partially, so we need to merge the
        // partially compacted set of cells, with the rest.
        final int len = qual.length;
        if (len != 2) {
          trivial = false;
          // We only do this here because no qualifier can be < 2 bytes.
          if (len > longest.qualifier().length) {
            longest = kv;
            longest_idx = i;
          }
        } else {
          // In the trivial case, do some sanity checking here.
          // For non-trivial cases, the sanity checking logic is more
          // complicated and is thus pushed down to `complexCompact'.
          final short delta = (short) ((Bytes.getShort(qual) & 0xFFFF)
                                       >>> Const.FLAG_BITS);
          // This data point has a time delta that's less than or equal to
          // the previous one.  This typically means we have 2 data points
          // at the same timestamp but they have different flags.  We're
          // going to abort here because someone needs to fsck the table.
          if (delta <= last_delta) {
            throw new IllegalDataException("Found out of order or duplicate"
              + " data: last_delta=" + last_delta + ", delta=" + delta
              + ", offending KV=" + kv + ", row=" + row + " -- run an fsck.");
          }
          last_delta = delta;
          // We don't need it below for complex compactions, so we update it
          // only here in the `else' branch.
          final byte[] v = kv.value();
          val_len += floatingPointValueToFix(qual[1], v) ? 4 : v.length;
        }
        qual_len += len;
      }

      if (trivial) {
        trivial_compactions.incrementAndGet();
        compact = trivialCompact(row, qual_len, val_len);
      } else {
        complex_compactions.incrementAndGet();
        compact = complexCompact(row, qual_len / 2);
        // Now it's vital that we check whether the compact KV has the same
        // qualifier as one of the qualifiers that were already in the row.
        // Otherwise we might do a `put' in this cell, followed by a delete.
        // We don't want to delete what we just wrote.
        // This can happen if this row was already compacted but someone
        // wrote another individual data point at the same timestamp.
        // Optimization: since we kept track of which KV had the longest
        // qualifier, we can opportunistically check here if it happens to
        // have the same qualifier as the one we just created.
        final byte[] qual = compact.qualifier();
        final byte[] longest_qual = longest.qualifier();
        if (qual.length <= longest_qual.length) {
          KeyValue dup = null;
          int dup_idx = -1;
          if (Bytes.equals(longest_qual, qual)) {
            dup = longest;
            dup_idx = longest_idx;
          } else {
            // Worst case: to be safe we have to loop again and check all
            // the qualifiers and make sure we're not going to overwrite
            // anything.
            // TODO(tsuna): Try to write a unit test that triggers this code
            // path.  I'm not even sure it's possible.  Should we replace
            // this code with an `assert false: "should never be here"'?
            for (int i = 0; i < nkvs; i++) {
              final KeyValue kv = row.get(i);
              if (Bytes.equals(kv.qualifier(), qual)) {
                dup = kv;
                dup_idx = i;
                break;
              }
            }
          }
          if (dup != null) {
            // So we did find an existing KV with the same qualifier.
            // Let's check if, by chance, the value is the same too.
            if (Bytes.equals(dup.value(), compact.value())) {
              // Since the values are the same, we don't need to write
              // anything.  There's already a properly compacted version of
              // this row in TSDB.
              write = false;
            }
            // Now let's make sure we don't delete this qualifier.  This
            // re-allocates the entire array, but should be a rare case.
            row.remove(dup_idx);
          } // else: no dup, we're good.
        } // else: most common case: the compact qualifier is longer than
          // the previously longest qualifier, so we cannot possibly
          // overwrite an existing cell we would then delete.
      }
    }
    if (compacted != null) {  // Caller is interested in the compacted form.
      compacted[0] = compact;
      final long base_time = Bytes.getUnsignedInt(compact.key(), metric_width);
      final long cut_off = System.currentTimeMillis() / 1000
        - Const.MAX_TIMESPAN - 1;
      if (base_time > cut_off) {  // If row is too recent...
        return null;              // ... Don't write back compacted.
      }
    }
    if (!TSDB.enable_compactions) {
      return null;
    }

    final byte[] key = compact.key();
    //LOG.debug("Compacting row " + Arrays.toString(key));
    deleted_cells.addAndGet(row.size());  // We're going to delete this.
    if (write) {
      final byte[] qual = compact.qualifier();
      final byte[] value = compact.value();
      written_cells.incrementAndGet();
      return tsdb.put(key, qual, value)
        .addCallbacks(new DeleteCompactedCB(row), handle_write_error);
    } else {
      // We had nothing to write, because one of the cells is already the
      // correctly compacted version, so we can go ahead and delete the
      // individual cells directly.
      new DeleteCompactedCB(row).call(null);
      return null;
    }
  }

  /**
   * Performs a trivial compaction of a row.
   * <p>
   * This method is to be used only when all the cells in the given row
   * are individual data points (nothing has been compacted yet).  If any of
   * the cells have already been compacted, the caller is expected to call
   * {@link #complexCompact} instead.
   * @param row The row to compact.  Assumed to have 2 elements or more.
   * @param qual_len Exact number of bytes to hold the compacted qualifiers.
   * @param val_len Exact number of bytes to hold the compacted values.
   * @return a {@link KeyValue} containing the result of the merge of all the
   * {@code KeyValue}s given in argument.
   */
  private static KeyValue trivialCompact(final ArrayList<KeyValue> row,
                                         final int qual_len,
                                         final int val_len) {
    // Now let's simply concatenate all the qualifiers and values together.
    final byte[] qualifier = new byte[qual_len];
    final byte[] value = new byte[val_len];
    // Now populate the arrays by copying qualifiers/values over.
    int qual_idx = 0;
    int val_idx = 0;
    for (final KeyValue kv : row) {
      final byte[] q = kv.qualifier();
      // We shouldn't get into this function if this isn't true.
      assert q.length == 2: "Qualifier length must be 2: " + kv;
      final byte[] v = fixFloatingPointValue(q[1], kv.value());
      qualifier[qual_idx++] = q[0];
      qualifier[qual_idx++] = fixQualifierFlags(q[1], v.length);
      System.arraycopy(v, 0, value, val_idx, v.length);
      val_idx += v.length;
    }
    // Right now we leave the last byte all zeros, this last byte will be
    // used in the future to introduce more formats/encodings.

    final KeyValue first = row.get(0);
    return new KeyValue(first.key(), first.family(), qualifier, value);
  }

  /**
   * Fix the flags inside the last byte of a qualifier.
   * <p>
   * OpenTSDB used to not rely on the size recorded in the flags being
   * correct, and so for a long time it was setting the wrong size for
   * floating point values (pretending they were encoded on 8 bytes when
   * in fact they were on 4).  So overwrite these bits here to make sure
   * they're correct now, because once they're compacted it's going to
   * be quite hard to tell if the flags are right or wrong, and we need
   * them to be correct to easily decode the values.
   * @param flags The least significant byte of a qualifier.
   * @param val_len The number of bytes in the value of this qualifier.
   * @return The least significant byte of the qualifier with correct flags.
   */
  private static byte fixQualifierFlags(byte flags, final int val_len) {
    // Explanation:
    //   (1) Take the last byte of the qualifier.
    //   (2) Zero out all the flag bits but one.
    //       The one we keep is the type (floating point vs integer value).
    //   (3) Set the length properly based on the value we have.
    return (byte) ((flags & ~(Const.FLAGS_MASK >>> 1)) | (val_len - 1));
    //              ^^^^^   ^^^^^^^^^^^^^^^^^^^^^^^^^    ^^^^^^^^^^^^^
    //               (1)               (2)                    (3)
  }

  /**
   * Returns whether or not this is a floating value that needs to be fixed.
   * <p>
   * OpenTSDB used to encode all floating point values as `float' (4 bytes)
   * but actually store them on 8 bytes, with 4 leading 0 bytes, and flags
   * correctly stating the value was on 4 bytes.
   * @param flags The least significant byte of a qualifier.
   * @param value The value that may need to be corrected.
   */
  private static boolean floatingPointValueToFix(final byte flags,
                                                 final byte[] value) {
    return (flags & Const.FLAG_FLOAT) != 0   // We need a floating point value.
      && (flags & Const.LENGTH_MASK) == 0x3  // That pretends to be on 4 bytes.
      && value.length == 8;                  // But is actually using 8 bytes.
  }

  /**
   * Returns a corrected value if this is a floating point value to fix.
   * <p>
   * OpenTSDB used to encode all floating point values as `float' (4 bytes)
   * but actually store them on 8 bytes, with 4 leading 0 bytes, and flags
   * correctly stating the value was on 4 bytes.
   * <p>
   * This function detects such values and returns a corrected value, without
   * the 4 leading zeros.  Otherwise it returns the value unchanged.
   * @param flags The least significant byte of a qualifier.
   * @param value The value that may need to be corrected.
   * @throws IllegalDataException if the value is malformed.
   */
  private static byte[] fixFloatingPointValue(final byte flags,
                                              final byte[] value) {
    if (floatingPointValueToFix(flags, value)) {
      // The first 4 bytes should really be zeros.
      if (value[0] == 0 && value[1] == 0 && value[2] == 0 && value[3] == 0) {
        // Just keep the last 4 bytes.
        return new byte[] { value[4], value[5], value[6], value[7] };
      } else {  // Very unlikely.
        throw new IllegalDataException("Corrupted floating point value: "
          + Arrays.toString(value) + " flags=0x" + Integer.toHexString(flags)
          + " -- first 4 bytes are expected to be zeros.");
      }
    }
    return value;
  }

  /**
   * Helper class for complex compaction cases.
   * <p>
   * This is simply a glorified pair of (qualifier, value) that's comparable.
   * Only the qualifier is used to make comparisons.
   * @see #complexCompact
   */
  private static final class Cell implements Comparable<Cell> {
    /** Tombstone used as a helper during the complex compaction.  */
    static final Cell SKIP = new Cell(null, null);

    final byte[] qualifier;
    final byte[] value;

    Cell(final byte[] qualifier, final byte[] value) {
      this.qualifier = qualifier;
      this.value = value;
    }

    public int compareTo(final Cell other) {
      return Bytes.memcmp(qualifier, other.qualifier);
    }

    public boolean equals(final Object o) {
      return o != null && o instanceof Cell && compareTo((Cell) o) == 0;
    }

    public int hashCode() {
      return Arrays.hashCode(qualifier);
    }

    public String toString() {
      return "Cell(" + Arrays.toString(qualifier)
        + ", " + Arrays.toString(value) + ')';
    }
  }

  /**
   * Compacts a partially compacted row.
   * <p>
   * This method is called in the non-trivial re-compaction cases, where a row
   * already contains one or more partially compacted cells.  This can happen
   * for various reasons, such as TSDs dying in the middle of a compaction or
   * races involved with TSDs trying to compact the same row at the same
   * time, or old data being slowly written to a TSD.
   * @param row The row to compact.  Assumed to have 2 elements or more.
   * @param estimated_nvalues Estimate of the number of values to compact.
   * Used to pre-allocate a collection of the right size, so it's better to
   * overshoot a bit to avoid re-allocations.
   * @return a {@link KeyValue} containing the result of the merge of all the
   * {@code KeyValue}s given in argument.
   * @throws IllegalDataException if one of the cells cannot be read because
   * it's corrupted or in a format we don't understand.
   */
  static KeyValue complexCompact(final ArrayList<KeyValue> row,
                                 final int estimated_nvalues) {
    // We know at least one of the cells contains multiple values, and we need
    // to merge all the cells together in a sorted fashion.  We use a simple
    // strategy: split all the cells into individual objects, sort them,
    // merge the result while ignoring duplicates (same qualifier & value).
    final ArrayList<Cell> cells = breakDownValues(row, estimated_nvalues);
    Collections.sort(cells);

    // Now let's done one pass first to compute the length of the compacted
    // value and to find if we have any bad duplicates (same qualifier,
    // different value).
    int nvalues = 0;
    int val_len = 1;  // Reserve an extra byte for meta-data.
    short last_delta = -1;  // Time delta, extracted from the qualifier.
    int ncells = cells.size();
    for (int i = 0; i < ncells; i++) {
      final Cell cell = cells.get(i);
      final short delta = (short) ((Bytes.getShort(cell.qualifier) & 0xFFFF)
                                   >>> Const.FLAG_BITS);
      // Because we sorted `cells' by qualifier, and because the time delta
      // occupies the most significant bits, this should never trigger.
      assert delta >= last_delta: ("WTF? It's supposed to be sorted: " + cells
                                   + " at " + i + " delta=" + delta
                                   + ", last_delta=" + last_delta);
      // The only troublesome case is where we have two (or more) consecutive
      // cells with the same time delta, but different flags or values.
      if (delta == last_delta) {
        // Find the previous cell.  Because we potentially replace the one
        // right before `i' with a tombstone, we might need to look further
        // back a bit more.
        Cell prev = Cell.SKIP;
        // i > 0 because we can't get here during the first iteration.
        // Also the first Cell cannot be Cell.SKIP, so `j' will never
        // underflow.  And even if it does, we'll get an exception.
        for (int j = i - 1; prev == Cell.SKIP; j--) {
          prev = cells.get(j);
        }
        if (cell.qualifier[1] != prev.qualifier[1]
            || !Bytes.equals(cell.value, prev.value)) {
          throw new IllegalDataException("Found out of order or duplicate"
            + " data: cell=" + cell + ", delta=" + delta + ", prev cell="
            + prev + ", last_delta=" + last_delta + ", in row=" + row
            + " -- run an fsck.");
        }
        // else: we're good, this is a true duplicate (same qualifier & value).
        // Just replace it with a tombstone so we'll skip it.  We don't delete
        // it from the array because that would cause a re-allocation.
        cells.set(i, Cell.SKIP);
        continue;
      }
      last_delta = delta;
      nvalues++;
      val_len += cell.value.length;
    }

    final byte[] qualifier = new byte[nvalues * 2];
    final byte[] value = new byte[val_len];
    // Now populate the arrays by copying qualifiers/values over.
    int qual_idx = 0;
    int val_idx = 0;
    for (final Cell cell : cells) {
      if (cell == Cell.SKIP) {
        continue;
      }
      byte[] b = cell.qualifier;
      System.arraycopy(b, 0, qualifier, qual_idx, b.length);
      qual_idx += b.length;
      b = cell.value;
      System.arraycopy(b, 0, value, val_idx, b.length);
      val_idx += b.length;
    }
    // Right now we leave the last byte all zeros, this last byte will be
    // used in the future to introduce more formats/encodings.

    final KeyValue first = row.get(0);
    final KeyValue kv = new KeyValue(first.key(), first.family(),
                                     qualifier, value);
    return kv;
  }

  /**
   * Breaks down all the values in a row into individual {@link Cell}s.
   * @param row The row to compact.  Assumed to have 2 elements or more.
   * @param estimated_nvalues Estimate of the number of values to compact.
   * Used to pre-allocate a collection of the right size, so it's better to
   * overshoot a bit to avoid re-allocations.
   * @throws IllegalDataException if one of the cells cannot be read because
   * it's corrupted or in a format we don't understand.
   */
  private static ArrayList<Cell> breakDownValues(final ArrayList<KeyValue> row,
                                                 final int estimated_nvalues) {
    final ArrayList<Cell> cells = new ArrayList<Cell>(estimated_nvalues);
    for (final KeyValue kv : row) {
      final byte[] qual = kv.qualifier();
      final int len = qual.length;
      final byte[] val = kv.value();
      if (len == 2) {  // Single-value cell.
        // Maybe we need to fix the flags in the qualifier.
        final byte[] actual_val = fixFloatingPointValue(qual[1], val);
        final byte q = fixQualifierFlags(qual[1], actual_val.length);
        final byte[] actual_qual;
        if (q != qual[1]) {  // We need to fix the qualifier.
          actual_qual = new byte[] { qual[0], q };  // So make a copy.
        } else {
          actual_qual = qual;  // Otherwise use the one we already have.
        }
        final Cell cell = new Cell(actual_qual, actual_val);
        cells.add(cell);
        continue;
      }
      // else: we have a multi-value cell.  We need to break it down into
      // individual Cell objects.
      // First check that the last byte is 0, otherwise it might mean that
      // this compacted cell has been written by a future version of OpenTSDB
      // and we don't know how to decode it, so we shouldn't touch it.
      if (val[val.length - 1] != 0) {
        throw new IllegalDataException("Don't know how to read this value:"
          + Arrays.toString(val) + " found in " + kv
          + " -- this compacted value might have been written by a future"
          + " version of OpenTSDB, or could be corrupt.");
      }
      // Now break it down into Cells.
      int val_idx = 0;
      for (int i = 0; i < len; i += 2) {
        final byte[] q = new byte[] { qual[i], qual[i + 1] };
        final int vlen = (q[1] & Const.LENGTH_MASK) + 1;
        final byte[] v = new byte[vlen];
        System.arraycopy(val, val_idx, v, 0, vlen);
        val_idx += vlen;
        final Cell cell = new Cell(q, v);
        cells.add(cell);
      }
      // Check we consumed all the bytes of the value.  Remember the last byte
      // is metadata, so it's normal that we didn't consume it.
      if (val_idx != val.length - 1) {
        throw new IllegalDataException("Corrupted value: couldn't break down"
          + " into individual values (consumed " + val_idx + " bytes, but was"
          + " expecting to consume " + (val.length - 1) + "): " + kv
          + ", cells so far: " + cells);
      }
    }
    return cells;
  }

  /**
   * Callback to delete a row that's been successfully compacted.
   */
  private final class DeleteCompactedCB implements Callback<Object, Object> {

    /** What we're going to delete.  */
    private final byte[] key;
    private final byte[] family;
    private final byte[][] qualifiers;

    public DeleteCompactedCB(final ArrayList<KeyValue> cells) {
      final KeyValue first = cells.get(0);
      key = first.key();
      family = first.family();
      qualifiers = new byte[cells.size()][];
      for (int i = 0; i < qualifiers.length; i++) {
        qualifiers[i] = cells.get(i).qualifier();
      }
    }

    public Object call(final Object arg) {
      return tsdb.delete(key, qualifiers).addErrback(handle_delete_error);
    }

    public String toString() {
      return "delete compacted cells";
    }

  }

  private final HandleErrorCB handle_read_error = new HandleErrorCB("read");
  private final HandleErrorCB handle_write_error = new HandleErrorCB("write");
  private final HandleErrorCB handle_delete_error = new HandleErrorCB("delete");

  /**
   * Callback to handle exceptions during the compaction process.
   */
  private final class HandleErrorCB implements Callback<Object, Exception> {

    private volatile int errors;

    private final String what;

    /**
     * Constructor.
     * @param what String describing what kind of operation (e.g. "read").
     */
    public HandleErrorCB(final String what) {
      this.what = what;
    }

    public Object call(final Exception e) {
      if (e instanceof PleaseThrottleException) {  // HBase isn't keeping up.
        final HBaseRpc rpc = ((PleaseThrottleException) e).getFailedRpc();
        if (rpc instanceof HBaseRpc.HasKey) {
          // We failed to compact this row.  Whether it's because of a failed
          // get, put or delete, we should re-schedule this row for a future
          // compaction.
          add(((HBaseRpc.HasKey) rpc).key());
          return Boolean.TRUE;  // We handled it, so don't return an exception.
        } else {  // Should never get in this clause.
          LOG.error("WTF?  Cannot retry this RPC, and this shouldn't happen: "
                    + rpc);
        }
      }
      // `++' is not atomic but doesn't matter if we miss some increments.
      if (++errors % 100 == 1) {  // Basic rate-limiting to not flood logs.
        LOG.error("Failed to " + what + " a row to re-compact", e);
      }
      return e;
    }

    public String toString() {
      return "handle " + what + " error";
    }
  }

  static final long serialVersionUID = 1307386642;

  /** Starts a compaction thread.  Only one such thread is needed.  */
  private void startCompactionThread() {
    final Thrd thread = new Thrd();
    thread.setDaemon(true);
    thread.start();
  }

  /** How frequently the compaction thread wakes up flush stuff.  */
  // TODO(tsuna): Make configurable?
  private static final int FLUSH_INTERVAL = 10;  // seconds

  /** Minimum number of rows we'll attempt to compact at once.  */
  // TODO(tsuna): Make configurable?
  private static final int MIN_FLUSH_THRESHOLD = 100;  // rows

  /** Maximum number of rows we'll compact concurrently.  */
  // TODO(tsuna): Make configurable?
  private static final int MAX_CONCURRENT_FLUSHES = 10000;  // rows

  /** If this is X then we'll flush X times faster than we really need.  */
  // TODO(tsuna): Make configurable?
  private static final int FLUSH_SPEED = 2;  // multiplicative factor

  /**
   * Background thread to trigger periodic compactions.
   */
  final class Thrd extends Thread {
    public Thrd() {
      super("CompactionThread");
    }

    public void run() {
      long last_flush = 0;
      while (true) {
        try {
          final long now = System.currentTimeMillis();
          final int size = size();
          // Let's suppose MAX_TIMESPAN = 1h.  We have `size' rows to compact,
          // and we better compact them all before in less than 1h, otherwise
          // we're going to "fall behind" when a new hour start (as we'll be
          // creating a ton of new rows then).  So slice MAX_TIMESPAN using
          // FLUSH_INTERVAL to compute what fraction of `size' we need to
          // flush at each iteration.  Note that `size' will usually account
          // for many rows that can't be flushed yet (not old enough) so we're
          // overshooting a bit (flushing more aggressively than necessary).
          // This isn't a problem at all.  The only thing that matters is that
          // the rate at which we flush stuff is proportional to how much work
          // is sitting in the queue.  The multiplicative factor FLUSH_SPEED
          // is added to make flush even faster than we need.  For example, if
          // FLUSH_SPEED is 2, then instead of taking 1h to flush what we have
          // for the previous hour, we'll take only 30m.  This is desirable so
          // that we evict old entries from the queue a bit faster.
          final int maxflushes = Math.max(MIN_FLUSH_THRESHOLD,
            size * FLUSH_INTERVAL * FLUSH_SPEED / Const.MAX_TIMESPAN);
          // Flush if either (1) it's been too long since the last flush
          // or (2) we have too many rows to recompact already.
          // Note that in the case (2) we might not be able to flush anything
          // if the rows aren't old enough.
          if (last_flush - now > Const.MAX_TIMESPAN  // (1)
              || size > maxflushes) {                // (2)
            flush(now / 1000 - Const.MAX_TIMESPAN - 1, maxflushes);
            if (LOG.isDebugEnabled()) {
              final int newsize = size();
              LOG.debug("flush() took " + (System.currentTimeMillis() - now)
                        + "ms, new queue size=" + newsize
                        + " (" + (newsize - size) + ')');
            }
          }
        } catch (Exception e) {
          LOG.error("Uncaught exception in compaction thread", e);
        } catch (OutOfMemoryError e) {
          // Let's free up some memory by throwing away the compaction queue.
          final int sz = size.get();
          CompactionQueue.super.clear();
          size.set(0);
          LOG.error("Discarded the compaction queue, size=" + sz, e);
        } catch (Throwable e) {
          LOG.error("Uncaught *Throwable* in compaction thread", e);
          // Catching this kind of error is totally unexpected and is really
          // bad.  If we do nothing and let this thread die, we'll run out of
          // memory as new entries are added to the queue.  We could always
          // commit suicide, but it's kind of drastic and nothing else in the
          // code does this.  If `enable_compactions' wasn't final, we could
          // always set it to false, but that's not an option.  So in order to
          // try to get a fresh start, let this compaction thread terminate
          // and spin off a new one instead.
          try {
            Thread.sleep(1000);  // Avoid busy looping creating new threads.
          } catch (InterruptedException i) {
            LOG.error("Compaction thread interrupted in error handling", i);
            return;  // Don't flush, we're truly hopeless.
          }
          startCompactionThread();
          return;
        }
        try {
          Thread.sleep(FLUSH_INTERVAL * 1000);
        } catch (InterruptedException e) {
          LOG.error("Compaction thread interrupted, doing one last flush", e);
          flush();
          return;
        }
      }
    }
  }

  /**
   * Helper to sort the byte arrays in the compaction queue.
   * <p>
   * This comparator sorts things by timestamp first, this way we can find
   * all rows of the same age at once.
   */
  private static final class Cmp implements Comparator<byte[]> {

    /** On how many bytes do we encode metrics IDs.  */
    private final short metric_width;

    public Cmp(final TSDB tsdb) {
      metric_width = tsdb.metrics.width();
    }

    public int compare(final byte[] a, final byte[] b) {
      final int c = Bytes.memcmp(a, b, metric_width, Const.TIMESTAMP_BYTES);
      // If the timestamps are equal, sort according to the entire row key.
      return c != 0 ? c : Bytes.memcmp(a, b);
    }
  }

}
