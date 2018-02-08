// This file is part of OpenTSDB.
// Copyright (C) 2016-2017  The OpenTSDB Authors.
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

import com.stumbleupon.async.Deferred;
import net.opentsdb.meta.Annotation;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId;
import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;

import java.util.*;

/**
 * Represents a read-only sequence of continuous histogram data points.
 * <p>
 * This class stores a continuous sequence of {@link HistogramRowSeq}s in memory.
 * 
 * @since 2.4
 */
public class HistogramSpan implements HistogramDataPoints {

  /**
   * The {@link TSDB} instance we belong to.
   */
  protected final TSDB tsdb;

  /**
   * All the rows in this span.
   */
  protected List<iHistogramRowSeq> rows = new ArrayList<iHistogramRowSeq>();

  /**
   * A list of annotations for this span. We can't lazily initialize since we
   * have to pass a collection to the compaction queue
   */
  private List<Annotation> annotations = new ArrayList<Annotation>(0);

  /**
   * Whether or not the rows have been sorted. This should be toggled by the
   * first call to an iterator method
   */
  private boolean sorted;

  /**
   * Stores a warning about a prematurely terminated query
   */
  private String warning;

  /**
   * Default constructor.
   *
   * @param tsdb The TSDB to which we belong
   */
  protected HistogramSpan(final TSDB tsdb) {
    this.tsdb = tsdb;
  }

  public String getWarning() {
    return warning;
  }

  /**
   * Store a warning about the query
   *
   * @param warning The warning to store
   */
  public void setWarning(final String warning) {
    this.warning = warning;
  }

  /**
   * @throws IllegalStateException if the span doesn't have any rows
   */
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
      return metricNameAsync().join();
    } catch (InterruptedException iex) {
      throw new RuntimeException("Interrupted the metric name call", iex);
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

  public byte[] metricUID() {
    return rows.get(0).metricUID();
  }

  /**
   * @return the list of tag pairs for the rows in this span
   * @throws IllegalStateException if the span was empty
   * @throws NoSuchUniqueId if the any of the tagk/v UIDs did not exist
   */
  public Map<String, String> getTags() {
    try {
      return getTagsAsync().join();
    } catch (InterruptedException iex) {
      throw new RuntimeException("Interrupted the tags call", iex);
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

  @Override
  public Bytes.ByteMap<byte[]> getTagUids() {
    checkNotEmpty();
    return rows.get(0).getTagUids();
  }

  /**
   * @return an empty list since aggregated tags cannot exist on a single span
   */
  public List<String> getAggregatedTags() {
    return Collections.emptyList();
  }

  public Deferred<List<String>> getAggregatedTagsAsync() {
    final List<String> empty = Collections.emptyList();
    return Deferred.fromResult(empty);
  }

  @Override
  public List<byte[]> getAggregatedTagUids() {
    return Collections.emptyList();
  }

  /**
   * @return the number of data points in this span, O(n) Unfortunately we must
   *         walk the entire array for every row as there may be a mix of second
   *         and millisecond timestamps
   */
  public int size() {
    int size = 0;
    for (final iHistogramRowSeq row : rows) {
      size += row.size();
    }
    return size;
  }

  /**
   * @return 0 since aggregation cannot happen at the span level
   */
  public int aggregatedSize() {
    return 0;
  }

  public List<String> getTSUIDs() {
    if (rows.size() < 1) {
      return null;
    }
    final byte[] tsuid = UniqueId.getTSUIDFromKey(rows.get(0).key(), 
        TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add(UniqueId.uidToString(tsuid));
    return tsuids;
  }

  /**
   * @return a list of annotations associated with this span. May be empty
   */
  public List<Annotation> getAnnotations() {
    return annotations;
  }

  /**
   * Adds a row of histogram data points to the span, merging with an existing
   * RowSeq or creating a new one if necessary.
   * <p>
   * Take the following rows as an example. Salt A has a row T0 at the base time T0,
   * a row T2 at the base time T2...; Salt B has a row T1 at the base time T1, 
   * a row T3 at the base time T3...
   * 
   * Since the salt is generated basing the following hash code:
   * <pre>
   * {@code 
   *  int modulo = (new String(Arrays.copyOfRange(rowKey,  Const.SALT_WIDTH(),
   *  rowKey.length))).hashCode() % Const.SALT_BUCKETS();
   *  byte[] salt = Internal.getSalt(modulo);
   * }
   * </pre>
   * This hash scheme guarantees that all the data points with the same base time will 
   * has the same salt. 
   * <pre>
   * |---A---|       |---B---|
   * |  T0   |       |   T1  |
   * |-------|       |-------|
   * |  T2   |       |   T3  |
   * |-------|       |-------|
   * |  T4   |       |   T5  |
   * |-------|       |-------|
   * </pre>
   * 
   * This method expects the caller adds the rows from the same salt in the 
   * timestamp order. When the caller adds the rows from salt A, the result 
   * rows will be as below:
   * <pre>
   * |-------|       
   * |  T0   |      
   * |-------|      
   * |  T2   |      
   * |-------|       
   * |  T4   |       
   * |-------|
   * </pre>
   * 
   * After the caller adds the rows from salt B, the result rows will be as below:
   * <pre>
   * |-------|       
   * |  T0   |      
   * |-------|      
   * |  T2   |      
   * |-------|       
   * |  T4   |       
   * |-------|
   * |  T1   |
   * |-------|
   * |  T3   |
   * |-------|
   * |  T5   |
   * |-------|
   * </pre>
   * When the caller iterates the data points in the Span, the Span will firstly 
   * sort the rows. Then the final result rows will be as below:
   * <pre>
   * |-------|       
   * |  T0   |      
   * |-------|      
   * |  T1   |      
   * |-------|       
   * |  T2   |       
   * |-------|
   * |  T3   |
   * |-------|
   * |  T4   |
   * |-------|
   * |  T5   |
   * |-------|
   * </pre>
   * <p>
   * @param key The row key of the row that want to add in the span
   * @param data_points histogram data points in the row
   * @throws IllegalArgumentException if the argument and this span are for 
   * two different time series.
   */
  protected void addRow(final byte[] key, 
                        final List<HistogramDataPoint> data_points) {
    if (null == key || null == data_points) {
      throw new NullPointerException("row key and histogram data points "
          + "can't be null");
    }
    
    long last_ts = 0;
    if (rows.size() != 0) {
      // Verify that we have the same metric id and tags.
      final iHistogramRowSeq last = rows.get(rows.size() - 1);
      final short metric_width = tsdb.metrics.width();
      final short tags_offset = (short) (Const.SALT_WIDTH() + metric_width + 
          Const.TIMESTAMP_BYTES);
      final short tags_bytes = (short) (key.length - tags_offset);
      String error = null;
      if (key.length != last.key().length) {
        error = "row key length mismatch";
      } else if (Bytes.memcmp(key, last.key(), Const.SALT_WIDTH(), metric_width) != 0) {
        error = "metric ID mismatch";
      } else if (Bytes.memcmp(key, last.key(), tags_offset, tags_bytes) != 0) {
        error = "tags mismatch";
      }
      if (error != null) {
        throw new IllegalArgumentException(error + ". " 
            + "This Span's last row key is " + Arrays.toString(last.key())
            + " whereas the row key being added is " + Arrays.toString(key) 
            + " and metric_width=" + metric_width);
      }
      last_ts = last.timestamp(last.size() - 1); // O(n)
    }

    final iHistogramRowSeq rowseq = createRowSequence(tsdb);
    rowseq.setRow(key, data_points);
    sorted = false;
    if (last_ts >= rowseq.timestamp(0)) {
      // scan to see if we need to merge into an existing row
      for (final iHistogramRowSeq rs : rows) {
        if ((rs.key().length == key.length)
            && (Bytes.memcmp(rs.key(), key, Const.SALT_WIDTH(), 
                (rs.key().length - Const.SALT_WIDTH())) == 0)) {
          rs.addRow(data_points);
          return;
        }
      }
    }

    rows.add(rowseq);
  }

  /**
   * Package private helper to access the last timestamp in an HBase row.
   *
   * @param metric_width The number of bytes on which metric IDs are stored.
   * @param row A compacted HBase row.
   * @return A strictly positive timestamp in seconds or ms.
   * @throws IllegalArgumentException if {@code row} doesn't contain any cell.
   */
  static long lastTimestampInRow(final short metric_width, final KeyValue row) {
    final long base_time = Internal.baseTime(row.key());
    final byte[] qual = row.qualifier();
    return Internal.getTimeStampFromNonDP(base_time, qual);
  }

  /**
   * @return an iterator to run over the list of data points
   */
  public HistogramSeekableView iterator() {
    checkRowOrder();
    return spanIterator();
  }

  /**
   * Finds the index of the row of the ith data point and the offset in the row.
   *
   * @param i The index of the data point to find.
   * @return two ints packed in a long. The first int is the index of the row in
   *         {@code rows} and the second is offset in that {@link RowSeq}
   *         instance.
   */
  private long getIdxOffsetFor(final int i) {
    checkRowOrder();
    int idx = 0;
    int offset = 0;
    for (final iHistogramRowSeq row : rows) {
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
   *
   * @param i A 0 based index incremented per the number of data points in the
   *          span.
   * @return A Unix epoch timestamp in milliseconds
   * @throws IndexOutOfBoundsException
   *           if the index would be out of bounds
   */
  public long timestamp(final int i) {
    checkRowOrder();
    final long idxoffset = getIdxOffsetFor(i);
    final int idx = (int) (idxoffset >>> 32);
    final int offset = (int) (idxoffset & 0x00000000FFFFFFFF);
    return rows.get(idx).timestamp(offset);
  }

  /**
   * Returns a human readable string representation of the object.
   */
  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("HistogramSpan(").append(rows.size()).append(" rows, [");
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
   *
   * @param timestamp A strictly positive 32-bit integer.
   * @return A strictly positive index in the {@code rows} array.
   */
  private int seekRow(final long timestamp) {
    checkRowOrder();
    int row_index = 0;
    iHistogramRowSeq row = null;
    final int nrows = rows.size();
    for (int i = 0; i < nrows; i++) {
      row = rows.get(i);
      final int sz = row.size();
      if (sz < 1) {
        row_index++;
      } else if (row.timestamp(sz - 1) < timestamp) {
        row_index++; // The last DP in this row is before 'timestamp'.
      } else {
        break;
      }
    }
    if (row_index == nrows) { // If this timestamp was too large for the
      --row_index; // last row, return the last row.
    }
    return row_index;
  }

  /**
   * Checks the sorted flag and sorts the rows if necessary. Should be called by
   * any iteration method. Since 2.0
   */
  private void checkRowOrder() {
    if (!sorted) {
      Collections.sort(rows, new HistogramRowSeq.HistogramRowSeqComparator());
      sorted = true;
    }
  }

  /**
   * Package private iterator method to access it as a Span.Iterator.
   */
  HistogramSpan.Iterator spanIterator() {
    checkRowOrder();
    return new HistogramSpan.Iterator();
  }

  /**
   * Iterator for {@link HistogramSpan}s.
   */
  final class Iterator implements HistogramSeekableView {

    /**
     * Index of the {@link HistogramRowSeq} we're currently at, in {@code rows}.
     */
    private int row_index;

    /**
     * Iterator on the current row.
     */
    private iHistogramRowSeq.Iterator current_row;

    Iterator() {
      current_row = rows.get(0).internalIterator();
    }

    // ------------------ //
    // Iterator interface //
    // ------------------ //

    @Override
    public boolean hasNext() {
      if (current_row.hasNext()) {
        return true;
      }
      // handle situations where a row in the middle may be empty due to some
      // kind of logic kicking out data points
      while (row_index < rows.size() - 1) {
        row_index++;
        current_row = rows.get(row_index).internalIterator();
        if (current_row.hasNext()) {
          return true;
        }
      }
      return false;
    }

    @Override
    public HistogramDataPoint next() {
      if (current_row.hasNext()) {
        return current_row.next();
      }
      // handle situations where a row in the middle may be empty due to some
      // kind of logic kicking out data points
      while (row_index < rows.size() - 1) {
        row_index++;
        current_row = rows.get(row_index).internalIterator();
        if (current_row.hasNext()) {
          return current_row.next();
        }
      }
      throw new NoSuchElementException("no more elements");
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    // ---------------------- //
    // SeekableView interface //
    // ---------------------- //

    @Override
    public void seek(final long timestamp) {
      int row_index = seekRow(timestamp);
      if (row_index != this.row_index) {
        this.row_index = row_index;
        current_row = rows.get(row_index).internalIterator();
      }
      current_row.seek(timestamp);
    }

    @Override
    public String toString() {
      return "HistogramSpan.Iterator(row_index=" + row_index 
          + ", current_row=" + current_row + ", span="
          + HistogramSpan.this + ')';
    }

  }

  /**
   * 
   * @param start_time The time in milliseconds at which the data begins.
   * @param end_time The time in milliseconds at which the data ends.
   * @param downsampler The downsampling specification to use
   * @param is_rollup Whether or not the query is handling rollup data
   * @param query_start Start of the actual query
   * @param query_end End of the actual query
   * @return A new downsampler.
   * @since 2.3
   */
  HistogramDownsampler downsampler(final long start_time, 
                                   final long end_time,
                                   final DownsamplingSpecification downsampler, 
                                   final boolean is_rollup, 
                                   final long query_start,
                                   final long query_end) {
    // ignore the fill policy 
    return new HistogramDownsampler(spanIterator(), downsampler, query_start, 
        query_end);
  }

  /**
   * Return the query index that maps this datapoints to the original subquery
   * 
   * @return index of the query in the TSQuery class
   */
  public int getQueryIndex() {
    throw new UnsupportedOperationException("Span.java: getQueryIndex not "
        + "supported");
  }

  /**
   * RowSeq abstract factory API implementation
   * 
   * @param tsdb The TSDB to which we belong
   * @return RowSeq object which stores read-only sequence of continuous HBase
   *         rows
   */
  protected iHistogramRowSeq createRowSequence(TSDB tsdb) {
    return new HistogramRowSeq(tsdb);
  }
}
