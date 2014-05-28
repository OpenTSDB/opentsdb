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
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import net.opentsdb.meta.Annotation;
import net.opentsdb.uid.UniqueId;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;

import com.stumbleupon.async.Deferred;

/**
 * Represents a read-only sequence of continuous data points.
 * <p>
 * This class stores a continuous sequence of {@link RowSeq}s in memory.
 */
final class Span implements DataPoints {

  /** The {@link TSDB} instance we belong to. */
  private final TSDB tsdb;

  /** All the rows in this span. */
  private ArrayList<RowSeq> rows = new ArrayList<RowSeq>();

  /** A list of annotations for this span. We can't lazily initialize since we
   * have to pass a collection to the compaction queue */
  private ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
  
  /** 
   * Whether or not the rows have been sorted. This should be toggled by the
   * first call to an iterator method
   */
  private boolean sorted;
  
  /**
   * Default constructor.
   * @param tsdb The TSDB to which we belong
   */
  Span(final TSDB tsdb) {
    this.tsdb = tsdb;
  }

  /** @throws IllegalStateException if the span doesn't have any rows */
  private void checkNotEmpty() {
    if (rows.size() == 0) {
      throw new IllegalStateException("empty Span");
    }
  }

  /** 
   * @return the name of the metric associated with the rows in this span
   * @throws IllegalStateException if the span was empty
   * @throws NoSuchUniqueId if the row key UID did not exist
   */
  public String metricName() {
    try {
      return metricNameAsync().joinUninterruptibly();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  public Deferred<String> metricNameAsync() {
    checkNotEmpty();
    return rows.get(0).metricNameAsync();
  }

  /**
   * @return the list of tag pairs for the rows in this span
   * @throws IllegalStateException if the span was empty
   * @throws NoSuchUniqueId if the any of the tagk/v UIDs did not exist
   */
  public Map<String, String> getTags() {
    try {
      return getTagsAsync().joinUninterruptibly();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }

  public Deferred<Map<String, String>> getTagsAsync() {
    checkNotEmpty();
    return rows.get(0).getTagsAsync();
  }
  
  /** @return an empty list since aggregated tags cannot exist on a single span */
  public List<String> getAggregatedTags() {
    return Collections.emptyList();
  }
  
  public Deferred<List<String>> getAggregatedTagsAsync() {
    final List<String> empty = Collections.emptyList();
    return Deferred.fromResult(empty);
  }

  /** @return the number of data points in this span, O(n)
   * Unfortunately we must walk the entire array for every row as there may be a 
   * mix of second and millisecond timestamps */
  public int size() {
    int size = 0;
    for (final RowSeq row : rows) {
      size += row.size();
    }
    return size;
  }

  /** @return 0 since aggregation cannot happen at the span level */
  public int aggregatedSize() {
    return 0;
  }

  public List<String> getTSUIDs() {
    if (rows.size() < 1) {
      return null;
    }
    final byte[] tsuid = UniqueId.getTSUIDFromKey(rows.get(0).key, 
        TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add(UniqueId.uidToString(tsuid));
    return tsuids;
  }
  
  /** @return a list of annotations associated with this span. May be empty */
  public List<Annotation> getAnnotations() {
    return annotations;
  }
  
  /**
   * Adds a compacted row to the span, merging with an existing RowSeq or 
   * creating a new one if necessary. 
   * @param row The compacted row to add to this span.
   * @throws IllegalArgumentException if the argument and this span are for
   * two different time series.
   */
  void addRow(final KeyValue row) {
    long last_ts = 0;
    if (rows.size() != 0) {
      // Verify that we have the same metric id and tags.
      final byte[] key = row.key();
      final RowSeq last = rows.get(rows.size() - 1);
      final short metric_width = tsdb.metrics.width();
      final short tags_offset = (short) (metric_width + Const.TIMESTAMP_BYTES);
      final short tags_bytes = (short) (key.length - tags_offset);
      String error = null;
      if (key.length != last.key.length) {
        error = "row key length mismatch";
      } else if (Bytes.memcmp(key, last.key, 0, metric_width) != 0) {
        error = "metric ID mismatch";
      } else if (Bytes.memcmp(key, last.key, tags_offset, tags_bytes) != 0) {
        error = "tags mismatch";
      }
      if (error != null) {
        throw new IllegalArgumentException(error + ". "
            + "This Span's last row key is " + Arrays.toString(last.key)
            + " whereas the row key being added is " + Arrays.toString(key)
            + " and metric_width=" + metric_width);
      }
      last_ts = last.timestamp(last.size() - 1);  // O(n)
    }

    final RowSeq rowseq = new RowSeq(tsdb);
    rowseq.setRow(row);
    sorted = false;
    if (last_ts >= rowseq.timestamp(0)) {
      // scan to see if we need to merge into an existing row
      for (final RowSeq rs : rows) {
        if (Bytes.memcmp(rs.key, row.key()) == 0) {
          rs.addRow(row);
          return;
        }
      }
    }
    
    rows.add(rowseq);
  }

  /**
   * Package private helper to access the last timestamp in an HBase row.
   * @param metric_width The number of bytes on which metric IDs are stored.
   * @param row A compacted HBase row.
   * @return A strictly positive timestamp in seconds or ms.
   * @throws IllegalArgumentException if {@code row} doesn't contain any cell.
   */
  static long lastTimestampInRow(final short metric_width,
                                 final KeyValue row) {
    final long base_time = Bytes.getUnsignedInt(row.key(), metric_width);
    final byte[] qual = row.qualifier();
    if (qual.length >= 4 && Internal.inMilliseconds(qual[qual.length - 4])) {
      return (base_time * 1000) + ((Bytes.getUnsignedInt(qual, qual.length - 4) & 
          0x0FFFFFC0) >>> (Const.MS_FLAG_BITS));
    }
    final short last_delta = (short)
      (Bytes.getUnsignedShort(qual, qual.length - 2) >>> Const.FLAG_BITS);
    return base_time + last_delta;
  }

  /** @return an iterator to run over the list of data points */
  public SeekableView iterator() {
    checkRowOrder();
    return spanIterator();
  }

  /**
   * Finds the index of the row of the ith data point and the offset in the row.
   * @param i The index of the data point to find.
   * @return two ints packed in a long.  The first int is the index of the row
   * in {@code rows} and the second is offset in that {@link RowSeq} instance.
   */
  private long getIdxOffsetFor(final int i) {
    checkRowOrder();
    int idx = 0;
    int offset = 0;
    for (final RowSeq row : rows) {
      final int sz = row.size();
      if (offset + sz > i) {
        break;
      }
      offset += sz;
      idx++;
    }
    return ((long) idx << 32) | (i - offset);
  }

  /**
   * Returns the timestamp for a data point at index {@code i} if it exists.
   * <b>Note:</b> To get to a timestamp this method must walk the entire byte
   * array, i.e. O(n) so call this sparingly. Use the iterator instead.
   * @param i A 0 based index incremented per the number of data points in the
   * span.
   * @return A Unix epoch timestamp in milliseconds
   * @throws IndexOutOfBoundsException if the index would be out of bounds
   */
  public long timestamp(final int i) {
    checkRowOrder();
    final long idxoffset = getIdxOffsetFor(i);
    final int idx = (int) (idxoffset >>> 32);
    final int offset = (int) (idxoffset & 0x00000000FFFFFFFF);
    return rows.get(idx).timestamp(offset);
  }

  /**
   * Determines whether or not the value at index {@code i} is an integer
   * @param i A 0 based index incremented per the number of data points in the
   * span.
   * @return True if the value is an integer, false if it's a floating point
   * @throws IndexOutOfBoundsException if the index would be out of bounds
   */
  public boolean isInteger(final int i) {
    checkRowOrder();
    final long idxoffset = getIdxOffsetFor(i);
    final int idx = (int) (idxoffset >>> 32);
    final int offset = (int) (idxoffset & 0x00000000FFFFFFFF);
    return rows.get(idx).isInteger(offset);
  }

  /**
   * Returns the value at index {@code i}
   * @param i A 0 based index incremented per the number of data points in the
   * span.
   * @return the value as a long
   * @throws IndexOutOfBoundsException if the index would be out of bounds
   * @throws ClassCastException if the value is a float instead. Call 
   * {@link #isInteger} first
   * @throws IllegalDataException if the data is malformed
   */
  public long longValue(final int i) {
    checkRowOrder();
    final long idxoffset = getIdxOffsetFor(i);
    final int idx = (int) (idxoffset >>> 32);
    final int offset = (int) (idxoffset & 0x00000000FFFFFFFF);
    return rows.get(idx).longValue(offset);
  }

  /**
   * Returns the value at index {@code i}
   * @param i A 0 based index incremented per the number of data points in the
   * span.
   * @return the value as a double
   * @throws IndexOutOfBoundsException if the index would be out of bounds
   * @throws ClassCastException if the value is an integer instead. Call 
   * {@link #isInteger} first
   * @throws IllegalDataException if the data is malformed
   */
  public double doubleValue(final int i) {
    checkRowOrder();
    final long idxoffset = getIdxOffsetFor(i);
    final int idx = (int) (idxoffset >>> 32);
    final int offset = (int) (idxoffset & 0x00000000FFFFFFFF);
    return rows.get(idx).doubleValue(offset);
  }

  /** Returns a human readable string representation of the object. */
  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("Span(")
       .append(rows.size())
       .append(" rows, [");
    for (int i = 0; i < rows.size(); i++) {
      if (i != 0) {
        buf.append(", ");
      }
      buf.append(rows.get(i).toString());
    }
    buf.append("])");
    return buf.toString();
  }

  /**
   * Finds the index of the row in which the given timestamp should be.
   * @param timestamp A strictly positive 32-bit integer.
   * @return A strictly positive index in the {@code rows} array.
   */
  private int seekRow(final long timestamp) {
    checkRowOrder();
    int row_index = 0;
    RowSeq row = null;
    final int nrows = rows.size();
    for (int i = 0; i < nrows; i++) {
      row = rows.get(i);
      final int sz = row.size();
      if (row.timestamp(sz - 1) < timestamp) {
        row_index++;  // The last DP in this row is before 'timestamp'.
      } else {
        break;
      }
    }
    if (row_index == nrows) {  // If this timestamp was too large for the
      --row_index;             // last row, return the last row.
    }
    return row_index;
  }

  /**
   * Checks the sorted flag and sorts the rows if necessary. Should be called
   * by any iteration method.
   * Since 2.0
   */
  private void checkRowOrder() {
    if (!sorted) {
      Collections.sort(rows, new RowSeq.RowSeqComparator());
      sorted = true;
    }
  }
  
  /** Package private iterator method to access it as a Span.Iterator. */
  Span.Iterator spanIterator() {
    if (!sorted) {
      Collections.sort(rows, new RowSeq.RowSeqComparator());
      sorted = true;
    }
    return new Span.Iterator();
  }

  /** Iterator for {@link Span}s. */
  final class Iterator implements SeekableView {

    /** Index of the {@link RowSeq} we're currently at, in {@code rows}. */
    private int row_index;

    /** Iterator on the current row. */
    private RowSeq.Iterator current_row;

    Iterator() {
      current_row = rows.get(0).internalIterator();
    }

    public boolean hasNext() {
      return (current_row.hasNext()             // more points in this row
              || row_index < rows.size() - 1);  // or more rows
    }

    public DataPoint next() {
      if (current_row.hasNext()) {
        return current_row.next();
      } else if (row_index < rows.size() - 1) {
        row_index++;
        current_row = rows.get(row_index).internalIterator();
        return current_row.next();
      }
      throw new NoSuchElementException("no more elements");
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

    public void seek(final long timestamp) {
      int row_index = seekRow(timestamp);
      if (row_index != this.row_index) {
        this.row_index = row_index;
        current_row = rows.get(row_index).internalIterator();
      }
      current_row.seek(timestamp);
    }

    public String toString() {
      return "Span.Iterator(row_index=" + row_index
        + ", current_row=" + current_row + ", span=" + Span.this + ')';
    }

  }

  /** Package private iterator method to access it as a DownsamplingIterator. */
  Span.DownsamplingIterator downsampler(final long interval,
                                        final Aggregator downsampler) {
    return new Span.DownsamplingIterator(interval, downsampler);
  }

  /**
   * Iterator that downsamples the data using an {@link Aggregator}.
   * <p>
   * This implementation relies on the fact that the {@link RowSeq}s in this
   * {@link Span} have {@code O(1)} access to individual data points, in order
   * to be efficient.
   */
  final class DownsamplingIterator
    implements SeekableView, DataPoint,
               Aggregator.Longs, Aggregator.Doubles {

    /** Extra bit we set on the timestamp of floating point values. */
    private static final long FLAG_FLOAT = 0x8000000000000000L;

    /** Mask to use in order to get rid of the flag above. */
    private static final long TIME_MASK  = 0x7FFFFFFFFFFFFFFFL;

    /** The "sampling" interval, in milliseconds. */
    private final long interval;

    /** Function to use to for downsampling. */
    private final Aggregator downsampler;

    /** Index of the {@link RowSeq} we're currently at, in {@code rows}. */
    private int row_index;

    /** The row we're currently at. */
    private RowSeq.Iterator current_row;

    /**
     * Current timestamp (unsigned 32 bits).
     * The most significant bit is used to store FLAG_FLOAT.
     */
    private long time;

    /** Current value (either an actual long or a double encoded in a long). */
    private long value;

    /**
     * Ctor.
     * @param interval The interval in seconds wanted between each data point.
     * @param downsampler The downsampling function to use.
     * @param iterator The iterator to access the underlying data.
     */
    DownsamplingIterator(final long interval,
                         final Aggregator downsampler) {
      this.interval = interval;
      this.downsampler = downsampler;
      this.current_row = rows.get(0).internalIterator();
    }

    // ------------------ //
    // Iterator interface //
    // ------------------ //

    public boolean hasNext() {
      return (current_row.hasNext()             // more points in this row
              || row_index < rows.size() - 1);  // or more rows
    }

    private boolean moveToNext() {
      if (!current_row.hasNext()) {
        // Yes, move on to the next one.
        if (row_index < rows.size() - 1) {  // Do we have more rows?
          current_row = rows.get(++row_index).internalIterator();
          current_row.next();  // Position the iterator on the first element.
          return true;
        } else {  // No more rows, can't go further.
          return false;
        }
      }
      current_row.next();
      return true;
    }

    public DataPoint next() {
      if (!hasNext()) {
        throw new NoSuchElementException("no more data points in " + this);
      }

      // Look ahead to see if all the data points that fall within the next
      // interval turn out to be integers.  While we do this, compute the
      // average timestamp of all the datapoints in that interval.
      long newtime = 0;
      final int saved_row_index = row_index;
      final long saved_state = current_row.saveState();
      // Since we know hasNext() returned true, we have at least 1 point.
      moveToNext();
      time = current_row.timestamp() + interval;  // end of interval
      //LOG.info("End of interval: " + time + " Interval: " + interval);
      boolean integer = true;
      int npoints = 0;
      do {
        npoints++;
        newtime += current_row.timestamp();
        //LOG.debug("Downsampling @ time " + current_row.timestamp());
        integer &= current_row.isInteger();
      } while (moveToNext() && current_row.timestamp() < time);
      newtime /= npoints;

      // Now that we're done looking ahead, let's go back where we were.
      if (row_index != saved_row_index) {
        row_index = saved_row_index;
        current_row = rows.get(row_index).internalIterator();
      }
      current_row.restoreState(saved_state);

      // Compute `value'.  This will rely on `time' containing the end time of
      // this interval...
      if (integer) {
        value = downsampler.runLong(this);
      } else {
        value = Double.doubleToRawLongBits(downsampler.runDouble(this));
      }
      // ... so update the time only here.
      time = newtime;
      //LOG.info("Downsampled avg time " + time);
      if (!integer) {
        time |= FLAG_FLOAT;
      }
      return this;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

    // ---------------------- //
    // SeekableView interface //
    // ---------------------- //

    public void seek(final long timestamp) {
      int row_index = seekRow(timestamp);
      if (row_index != this.row_index) {
        //LOG.debug("seek from row #" + this.row_index + " to " + row_index);
        this.row_index = row_index;
        current_row = rows.get(row_index).internalIterator();
      }
      current_row.seek(timestamp);
    }

    // ------------------- //
    // DataPoint interface //
    // ------------------- //

    public long timestamp() {
      return time & TIME_MASK;
    }

    public boolean isInteger() {
      return (time & FLAG_FLOAT) == 0;
    }

    public long longValue() {
      if (isInteger()) {
        return value;
      }
      throw new ClassCastException("this value is not a long in " + this);
    }

    public double doubleValue() {
      if (!isInteger()) {
        return Double.longBitsToDouble(value);
      }
      throw new ClassCastException("this value is not a float in " + this);
    }

    public double toDouble() {
      return isInteger() ? longValue() : doubleValue();
    }

    // -------------------------- //
    // Aggregator.Longs interface //
    // -------------------------- //

    public boolean hasNextValue() {
      if (!current_row.hasNext()) {
        if (row_index < rows.size() - 1) {
          //LOG.info("hasNextValue: next row? " + (rows.get(row_index + 1).timestamp(0) < time));
          return rows.get(row_index + 1).timestamp(0) < time;
        } else {
          //LOG.info("hasNextValue: false, this is the end");
          return false;
        }
      }
      //LOG.info("hasNextValue: next point? " + (current_row.peekNextTimestamp() < time));
      return current_row.peekNextTimestamp() < time;
    }

    public long nextLongValue() {
      if (hasNextValue()) {
        moveToNext();
        return current_row.longValue();
      }
      throw new NoSuchElementException("no more longs in interval of " + this);
    }

    // ---------------------------- //
    // Aggregator.Doubles interface //
    // ---------------------------- //

    public double nextDoubleValue() {
      if (hasNextValue()) {
        moveToNext();
        // Use `toDouble' instead of `doubleValue' because we can get here if
        // there's a mix of integer values and floating point values in the
        // current downsampled interval.
        return current_row.toDouble();
      }
      throw new NoSuchElementException("no more floats in interval of " + this);
    }

    public String toString() {
      final StringBuilder buf = new StringBuilder();
      buf.append("Span.DownsamplingIterator(interval=").append(interval)
         .append(", downsampler=").append(downsampler)
         .append(", row_index=").append(row_index)
         .append(", current_row=").append(current_row.toStringSummary())
         .append("), current time=").append(timestamp())
         .append(", current value=");
     if (isInteger()) {
       buf.append("long:").append(longValue());
     } else {
       buf.append("double:").append(doubleValue());
     }
     buf.append(", rows=").append(rows).append(')');
     return buf.toString();
    }

  }

}
