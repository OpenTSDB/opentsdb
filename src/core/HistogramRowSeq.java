// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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

import net.opentsdb.core.HistogramDataPoint.HistogramBucket;
import net.opentsdb.meta.Annotation;
import org.hbase.async.Bytes;

import java.util.*;

/**
 * Represents a read-only sequence of continuous HBase rows.
 * <p>
 * This class stores in memory the data of one or more continuous HBase rows for
 * a given time series. To consolidate memory, the data points are stored in two
 * byte arrays: one for the time offsets/flags and another for the values.
 * Access is granted via pointers.
 */
public class HistogramRowSeq implements iHistogramRowSeq {

  /** The {@link TSDB} instance we belong to. */
  private final TSDB tsdb;

  /** First row key. */
  protected byte[] key;

  protected List<HistogramDataPoint> rowSeq;

  public HistogramRowSeq(final TSDB tsdb) {
    this.tsdb = tsdb;
  }

  @Override
  public void setRow(final byte[] key, final List<HistogramDataPoint> row) {
    if (this.key != null) {
      throw new IllegalStateException("setRow was already called on " + this);
    }

    this.key = key;
    this.rowSeq = row;
  }

  @Override
  public void addRow(final List<HistogramDataPoint> row) {
    if (null == this.key) {
      throw new IllegalStateException("setRow was never called on " + this);
    }

    int index_local = 0;
    int index_remote = 0;
    List<HistogramDataPoint> combinedRows = new ArrayList<HistogramDataPoint>(this.rowSeq.size() + row.size());
    while (index_local < this.rowSeq.size() && index_remote < row.size()) {
      HistogramDataPoint hdp_local = this.rowSeq.get(index_local);
      HistogramDataPoint hdp_remote = row.get(index_remote);

      long sort = hdp_remote.timestamp() - hdp_local.timestamp();
      if (0 == sort) {
        // duplicate histogram data point with the same timestamp, pick the local one
        combinedRows.add(hdp_local);
        ++index_local;
        ++index_remote;
      } else if (sort > 0) {
        // the remote one has a bigger timestamp, pick the local one
        combinedRows.add(hdp_local);
        ++index_local;
      } else {
        // the local one has a bigger timestamp, pick the remote one
        combinedRows.add(hdp_remote);
        ++index_remote;
      }
    } // end while

    if (index_local < this.rowSeq.size()) {
      // add the left elements of local to the combined list
      combinedRows.addAll(this.rowSeq.subList(index_local, this.rowSeq.size()));
    } else if (index_remote < row.size()) {
      // add the left elements of remote to the combined list
      combinedRows.addAll(row.subList(index_remote, row.size()));
    }

    this.rowSeq = combinedRows;
  }

  @Override
  public byte[] key() {
    return key;
  }

  @Override
  public long baseTime() {
    return Internal.baseTime(key);
  }

  @Override
  public Iterator internalIterator() {
    return new Iterator();
  }

  @Override
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

  @Override
  public Deferred<String> metricNameAsync() {
    if (key == null) {
      throw new IllegalStateException("the row key is null!");
    }
    return RowKey.metricNameAsync(tsdb, key);
  }

  @Override
  public byte[] metricUID() {
    return Arrays.copyOfRange(key, Const.SALT_WIDTH(), Const.SALT_WIDTH() + TSDB.metrics_width());
  }

  @Override
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

  @Override
  public Deferred<Map<String, String>> getTagsAsync() {
    return Tags.getTagsAsync(tsdb, key);
  }

  @Override
  public Bytes.ByteMap<byte[]> getTagUids() {
    return Tags.getTagUids(key);
  }

  @Override
  public List<String> getAggregatedTags() {
    return Collections.emptyList();
  }

  @Override
  public Deferred<List<String>> getAggregatedTagsAsync() {
    final List<String> empty = Collections.emptyList();
    return Deferred.fromResult(empty);
  }

  @Override
  public List<byte[]> getAggregatedTagUids() {
    return Collections.emptyList();
  }

  @Override
  public List<String> getTSUIDs() {
    return Collections.emptyList();
  }

  @Override
  public List<Annotation> getAnnotations() {
    return Collections.emptyList();
  }

  @Override
  public String getWarning() {
    return null;
  }

  @Override
  public int size() {
    return rowSeq.size();
  }

  @Override
  public int aggregatedSize() {
    return 0;
  }

  @Override
  public HistogramSeekableView iterator() {
    return internalIterator();
  }

  @Override
  public long timestamp(int i) {
    checkIndex(i);

    return rowSeq.get(i).timestamp();
  }

  @Override
  public int getQueryIndex() {
    return 0;
  }

  /**
   * @throws IndexOutOfBoundsException
   *           if {@code i} is out of bounds.
   */
  private void checkIndex(final int i) {
    if (i >= size()) {
      throw new IndexOutOfBoundsException("index " + i + " >= " + size() + " for this=" + this);
    }
    if (i < 0) {
      throw new IndexOutOfBoundsException("negative index " + i + " for this=" + this);
    }
  }
  
  /** Returns a human readable string representation of the object. */
  @Override
  public String toString() {
    final String metric = metricName();
    final int sz = size();
    
    // The argument passed to StringBuilder is an estimate = number of data points * 125
    final StringBuilder buf = new StringBuilder(sz * 125);
    final long base_time = baseTime();
    buf.append("RowSeq(")
       .append(key == null ? "<null>" : Arrays.toString(key))
       .append(" (metric=")
       .append(metric)
       .append("), base_time=")
       .append(base_time)
       .append(" (")
       .append(base_time > 0 ? new Date(base_time * 1000) : "no date")
       .append(")");
    
    for (short i = 0; i < sz; ++i) {
      buf.append('+').append(rowSeq.get(i).timestamp());
      buf.append(":histogram(").append(Arrays.toString(rowSeq.get(i).getRawData()));
      buf.append(')');
      if (i != sz -1) {
        buf.append(", ");
      }
    } // end for
    
    buf.append(")");
    return buf.toString();
  }

  /**
   * Used to compare two RowSeq objects when sorting a {@link Span}. Compares on
   * the {@code RowSeq#baseTime()}
   * 
   * @since 2.0
   */
  public static final class HistogramRowSeqComparator implements Comparator<iHistogramRowSeq> {
    public int compare(final iHistogramRowSeq a, final iHistogramRowSeq b) {
      if (null == a || null == b) {
        if (a == b) {
          return 0;
        } else {
          return (null == a) ? -1 : 1;
        }
      }
      if (a.baseTime() == b.baseTime()) {
        return 0;
      }
      return a.baseTime() < b.baseTime() ? -1 : 1;
    }
  }

  final class Iterator implements iHistogramRowSeq.Iterator {
    private int next_index;

    Iterator() {
    }
    
    @Override
    public long timestamp() {
      return getCurrent().timestamp();
    }

    @Override
    public byte[] getRawData() {
      return getCurrent().getRawData();
    }

    @Override
    public void resetFromRawData(byte[] raw_data) {
      getCurrent().resetFromRawData(raw_data);
    }

    @Override
    public double percentile(double p) {
      return getCurrent().percentile(p);
    }

    @Override
    public List<Double> percentile(List<Double> p) {
      return getCurrent().percentile(p);
    }

    @Override
    public void aggregate(HistogramDataPoint histo, HistogramAggregation func) {
      getCurrent().aggregate(histo, func);
    }

    @Override
    public boolean hasNext() {
      return next_index < rowSeq.size();
    }

    @Override
    public HistogramDataPoint next() {
      if (!hasNext()) {
        throw new NoSuchElementException("no more elements");
      }
      
      ++next_index;
      return this;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void seek(long timestamp) {
      if ((timestamp & Const.MILLISECOND_MASK) != 0) { // negative or not 48 bits
        throw new IllegalArgumentException("invalid timestamp: " + timestamp);
      }

      // TODO: this can be optimized to O(nlogn)
      next_index = 0;
      while (next_index < rowSeq.size() && rowSeq.get(next_index).timestamp() < timestamp) {
        ++next_index;
      }
    }
    
    @Override
    public HistogramDataPoint clone() {
      return getCurrent().clone();
    }
    
    @Override
    public HistogramDataPoint cloneAndSetTimestamp(final long timestamp) {
      return getCurrent().cloneAndSetTimestamp(timestamp);
    }

    private HistogramDataPoint getCurrent() {
      assert next_index > 0 : "not initialized: " + this;
      return rowSeq.get(next_index - 1);
    }

    @Override
    public Map<HistogramBucket, Long> getHistogramBucketsIfHas() {
      return getCurrent().getHistogramBucketsIfHas();
    }
  }
}
