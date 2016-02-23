// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.query.expression;

import java.util.HashSet;
import java.util.Set;

import org.hbase.async.Bytes.ByteMap;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.MutableDataPoint;
import net.opentsdb.utils.ByteSet;

/**
 * Contains the information for a series that has been processed through an 
 * expression iterator. Each time a metric data point series set is added we 
 * add the metric and compute the tag sets. 
 * <p>
 * As the iterator progresses, it will call into the {@link #reset} methods.
 * @since 2.3
 */
public class ExpressionDataPoint implements DataPoint {

  /** A list of metric UIDs wrapped up into this expression result */
  private final ByteSet metric_uids;
  
  /** The list of tag key/value pairs common to all series in this expression */
  private final ByteMap<byte[]> tags;
  
  /** The list of aggregated tag keys common to all series in this expression */
  private final ByteSet aggregated_tags;
  
  /** The list of TSUIDs from all series in this expression */
  private final Set<String> tsuids;
  
  /** The size of the aggregated results.
   * TODO - this is simply the size of the first series added. We need a way
   * to compute this properly.
   */
  private long size;
  
  /** The total number of raw data points in all series */
  private long raw_size;
  
  /** The data point overwritten each time through the iterator */
  private final MutableDataPoint dp;
  
  /** An index in the original {@link TimeSyncedIterator} iterator array */
  private int index;
  
  /**
   * Default ctor that simply sets up new objects for all internal fields.
   * TODO - lazily initialize the field to avoid unused objects
   */
  public ExpressionDataPoint() {
    metric_uids = new ByteSet();
    tags = new ByteMap<byte[]>();
    aggregated_tags = new ByteSet();
    tsuids = new HashSet<String>();
    dp = new MutableDataPoint();
  }
  
  /**
   * Ctor that sets up the meta data maps and initializes an empty dp
   * @param dps The data point to pull meta from
   */
  @SuppressWarnings("unchecked")
  public ExpressionDataPoint(final DataPoints dps) {
    metric_uids = new ByteSet();
    metric_uids.add(dps.metricUID());
    tags = dps.getTagUids() != null ? 
        (ByteMap<byte[]>) dps.getTagUids().clone() : new ByteMap<byte[]>();
    aggregated_tags = new ByteSet();
    if (dps.getAggregatedTagUids() != null) {
      for (final byte[] tagk : dps.getAggregatedTagUids()) {
        aggregated_tags.add(tagk);
      }
    }
    tsuids = new HashSet<String>(dps.getTSUIDs());
    // TODO - restore when these are faster
    //size = dps.size();
    //raw_size = dps.aggregatedSize();
    dp = new MutableDataPoint();
    dp.reset(Long.MAX_VALUE, Double.NaN);
  }
  
  /**
   * Ctor that clones the meta data of the existing dps and sets up an empty value
   * @param dps The data point to pull meta from
   */
  @SuppressWarnings("unchecked")
  public ExpressionDataPoint(final ExpressionDataPoint dps) {
    metric_uids = new ByteSet();
    metric_uids.addAll(dps.metric_uids);
    tags = (ByteMap<byte[]>) dps.tags.clone();
    aggregated_tags = new ByteSet();
    aggregated_tags.addAll(dps.aggregated_tags);
    tsuids = new HashSet<String>(dps.tsuids);
    size = dps.size;
    raw_size = dps.raw_size;
    dp = new MutableDataPoint();
    dp.reset(Long.MAX_VALUE, Double.NaN);
  }
  
  /**
   * Add another metric series to this collection, computing the tag and
   * agg intersections and incrementing the size.
   * @param dps The series to add
   */
  public void add(final DataPoints dps) {
    metric_uids.add(dps.metricUID());
    
    // TODO - tags intersection
    
    for (final byte[] tagk : dps.getAggregatedTagUids()) {
      aggregated_tags.add(tagk);
    }
    
    tsuids.addAll(dps.getTSUIDs());
    // TODO - this ain't right. We need to number of dps emitted from HERE. For
    // now we'll just take the first dps size. If it's downsampled then this
    // will be accurate.
    //size += dps.size();
    // TODO - restore when this is faster
    //raw_size += dps.aggregatedSize();
  }
  
  /**
   * Add another metric series to this collection, computing the tag and
   * agg intersections and incrementing the size.
   * @param dps The series to add
   */
  public void add(final ExpressionDataPoint dps) {
    metric_uids.addAll(dps.metric_uids);
    
    // TODO - tags intersection
    
    aggregated_tags.addAll(dps.aggregated_tags);
    
    tsuids.addAll(dps.tsuids);
    // TODO - this ain't right. We need to number of dps emitted from HERE. For
    // now we'll just take the first dps size. If it's downsampled then this
    // will be accurate.
    //size += dps.size();
    raw_size += dps.raw_size;
  }
  
  /** @return the metric UIDs */
  public ByteSet metricUIDs() {
    return metric_uids;
  }
  
  /** @return the list of common tag pairs in the series */
  public ByteMap<byte[]> tags() {
    return tags;
  }
  
  /** @return the list of aggregated tags */
  public ByteSet aggregatedTags() {
    return aggregated_tags;
  }
  
  /** @return the list of TSUIDs aggregated into this series */
  public Set<String> tsuids() {
    return tsuids;
  }
  
  /** @return the aggregated number of data points in this series */
  public long size() {
    return size;
  }
  
  /** @return the number of raw data points in this series */
  public long rawSize() {
    return raw_size;
  }
  
  /**
   * Stores a Double data point
   * @param timestamp The timestamp
   * @param value The value
   */
  public void reset(final long timestamp, final double value) {
    dp.reset(timestamp, value);
  }
  
  /**
   * Stores a data point pulled from the given data point interface
   * @param dp the data point to read from
   */
  public void reset(final DataPoint dp) {
    this.dp.reset(dp);
  }

  // DataPoint implementations
  
  @Override
  public long timestamp() {
    return dp.timestamp();
  }

  @Override
  public boolean isInteger() {
    return dp.isInteger();
  }

  @Override
  public long longValue() {
    return dp.longValue();
  }

  @Override
  public double doubleValue() {
    return dp.doubleValue();
  }

  @Override
  public double toDouble() {
    return dp.toDouble();
  }
  
  /** @param index The index in the {@link TimeSyncedIterator} array */
  public void setIndex(final int index) {
    this.index = index;
  }
  
  /** @return the index in the {@link TimeSyncedIterator} array */
  public int getIndex() {
    return index;
  }
}
