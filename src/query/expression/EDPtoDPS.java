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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.hbase.async.Bytes.ByteMap;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.MutableDataPoint;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.meta.Annotation;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.ByteSet;

/**
 * An ugly temporary class for converting from an expression datapoint to a 
 * standard data point for serialization in the default query format.
 */
public class EDPtoDPS implements DataPoints {
  /** The TSDB used for UID to name lookups */
  private final TSDB tsdb;
  
  /** The index of this data point in the iterator */
  private final int index;
  
  /** The iterator that contains the results for this data point */
  private final ExpressionIterator iterator;
  
  /** The list of data points from the iterator from which we read */
  private final ExpressionDataPoint[] edps;
  
  /**
   * Default ctor
   * @param tsdb The TSDB used for UID to name lookups
   * @param index The index of this data point in the iterator
   * @param iterator The iterator that contains the results for this data point
   */
  public EDPtoDPS(final TSDB tsdb, final int index, 
      final ExpressionIterator iterator) {
    this.tsdb = tsdb;
    this.index = index;
    this.iterator = iterator;
    edps = iterator.values();
  }
  
  @Override
  public String metricName() {
    try {
      return metricNameAsync().joinUninterruptibly();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }

  @Override
  public Deferred<String> metricNameAsync() {
    if (edps[index].metricUIDs() == null) {
      throw new IllegalStateException("Iterator UID was null for index " + 
          index + " and iterator " + iterator);
    }
    final byte[] uid = edps[index].metricUIDs().iterator().next();
    return tsdb.getUidName(UniqueIdType.METRIC, uid);
  }

  @Override
  public byte[] metricUID() {
    if (edps[index].metricUIDs() == null) {
      throw new IllegalStateException("Iterator UID was null for index " + 
          index + " and iterator " + iterator);
    }
    return edps[index].metricUIDs().iterator().next();
  }

  @Override
  public Map<String, String> getTags() {
    try {
      return getTagsAsync().joinUninterruptibly();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }

  @Override
  public Deferred<Map<String, String>> getTagsAsync() {
    return Tags.getTagsAsync(tsdb, edps[index].tags());
  }

  @Override
  public ByteMap<byte[]> getTagUids() {
    return edps[index].tags();
  }

  @Override
  public List<String> getAggregatedTags() {
    try {
      return getAggregatedTagsAsync().joinUninterruptibly();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }

  @Override
  public Deferred<List<String>> getAggregatedTagsAsync() {
    final ByteSet tagks = edps[index].aggregatedTags();
    final List<String> aggregated_tags = new ArrayList<String>(tagks.size());
    
    final List<Deferred<String>> names = 
        new ArrayList<Deferred<String>>(tagks.size());
    for (final byte[] tagk : tagks) {
      names.add(tsdb.getUidName(UniqueIdType.TAGK, tagk));
    }
    
    /** Adds the names to the aggregated_tags list */
    final class ResolveCB implements Callback<List<String>, ArrayList<String>> {
      @Override
      public List<String> call(final ArrayList<String> names) throws Exception {
        for (final String name : names) {
          aggregated_tags.add(name);
        }
        return aggregated_tags;
      }
    }
    
    return Deferred.group(names).addCallback(new ResolveCB());
  }

  @Override
  public List<byte[]> getAggregatedTagUids() {
    final List<byte[]> agg_tags = new ArrayList<byte[]>(
        edps[index].aggregatedTags());
    return agg_tags;
  }

  @Override
  public List<String> getTSUIDs() {
    // TODO Fix it up
    return Collections.emptyList();
  }

  @Override
  public List<Annotation> getAnnotations() {
    // TODO Fix it up
    return Collections.emptyList();
  }

  @Override
  public int size() {
    // TODO Estimate
    return -1;
  }

  @Override
  public int aggregatedSize() {
    // TODO Estimate
    return -1;
  }

  @Override
  public SeekableView iterator() {
    return new Iterator();
  }

  @Override
  public long timestamp(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isInteger(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long longValue(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double doubleValue(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getQueryIndex() {
    // TODO Fix it up
    return 0;
  }
  
  /**
   * Simple class that fills the local data point while iterating through the 
   * expression data points at the proper index.
   */
  private class Iterator implements SeekableView {
    /** A data pont to mutate as we iterate */
    final MutableDataPoint dp = new MutableDataPoint();
    
    @Override
    public boolean hasNext() {
      return iterator.hasNext(index);
    }

    @Override
    public DataPoint next() {
      iterator.next(index);
      dp.reset(edps[index].timestamp(), edps[index].toDouble());
      return dp;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void seek(long timestamp) {
      throw new UnsupportedOperationException();
    }
    
  }
}
