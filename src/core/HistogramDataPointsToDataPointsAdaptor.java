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

import java.util.List;
import java.util.Map;

import org.hbase.async.Bytes.ByteMap;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.meta.Annotation;

/**
 * Converts histogram DataPoints to DataPoints for using the existing metric
 * serialization code when querying percentiles.
 * 
 * @since 2.4
 */
public class HistogramDataPointsToDataPointsAdaptor implements DataPoints {
  final private HistogramDataPoints hist_data_points;
  final private float percentile;
  
  public HistogramDataPointsToDataPointsAdaptor(final HistogramDataPoints hdps, 
                                                final float percentile) {
    this.hist_data_points = hdps;
    this.percentile = percentile;
  }

  @Override
  public String metricName() {
    return this.hist_data_points.metricName() + "_pct_" 
        + Float.toString(this.percentile);
  }

  @Override
  public Deferred<String> metricNameAsync() {
    return this.hist_data_points.metricNameAsync()
          .addCallback(new Callback<String, String>() {
      public String call(final String name) {
        return name + "_pct_" + Float.toString(percentile);
      }
    });
  }

  @Override
  public byte[] metricUID() {
    return this.hist_data_points.metricUID();
  }

  @Override
  public Map<String, String> getTags() {
    return this.hist_data_points.getTags();
  }

  @Override
  public Deferred<Map<String, String>> getTagsAsync() {
    return this.hist_data_points.getTagsAsync();
  }

  @Override
  public ByteMap<byte[]> getTagUids() {
    return this.hist_data_points.getTagUids();
  }

  @Override
  public List<String> getAggregatedTags() {
    return this.hist_data_points.getAggregatedTags();
  }

  @Override
  public Deferred<List<String>> getAggregatedTagsAsync() {
    return this.hist_data_points.getAggregatedTagsAsync();
  }

  @Override
  public List<byte[]> getAggregatedTagUids() {
    return this.hist_data_points.getAggregatedTagUids();
  }

  @Override
  public List<String> getTSUIDs() {
    return this.hist_data_points.getTSUIDs();
  }

  @Override
  public List<Annotation> getAnnotations() {
    return this.hist_data_points.getAnnotations();
  }
  
  @Override
  public int size() {
    return this.hist_data_points.size();
  }

  @Override
  public int aggregatedSize() {
    return this.hist_data_points.aggregatedSize();
  }

  @Override
  public SeekableView iterator() {
    return internalIterator();
  }

  private HistogramDataPoint getHistogramDataPoint(int i) {
    if (i < 0) {
      throw new IndexOutOfBoundsException("negative index: " + i);
    }
    final int saved_i = i;
    final HistogramSeekableView it = this.hist_data_points.iterator();
    HistogramDataPoint dp = null;
    while (it.hasNext() && i >= 0) {
      dp = it.next();
      i--;
    }
    if (i != -1 || dp == null) {
      throw new IndexOutOfBoundsException("index " + saved_i
          + " too large (it's >= " + size() + ") for " + this);
    }
    return dp;
  }
  
  @Override
  public long timestamp(int i) {
    return getHistogramDataPoint(i).timestamp();
  }

  @Override
  public boolean isInteger(int i) {
    return false;
  }

  @Override
  public long longValue(int i) {
    throw new ClassCastException("value #" + i + " is not a long in " + this);
  }

  @Override
  public double doubleValue(int i) {
    return getHistogramDataPoint(i).percentile(this.percentile);
  }

  @Override
  public int getQueryIndex() {
    return this.hist_data_points.getQueryIndex();
  }

  @Override
  public boolean isPercentile() {
    return true;
  }

  @Override
  public float getPercentile() {
    return this.percentile;
  }

  private Iterator internalIterator() {
    return new Iterator();
  }
  
  ////////////////////////////////////////////////////////////////////////////
  // internal iterator
  ////////////////////////////////////////////////////////////////////////////
  final class Iterator implements SeekableView, DataPoint {
    final private HistogramSeekableView source;
    private double value;
    private long timestamp;
    
    public Iterator() {
      this.source = hist_data_points.iterator();
    }
    
    @Override
    public boolean hasNext() {
      return this.source.hasNext();
    }

    @Override
    public DataPoint next() {
      HistogramDataPoint hdp = this.source.next();
      this.value = hdp.percentile(percentile);
      this.timestamp = hdp.timestamp();
      return this;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove is not supported here");
    }
    
    @Override
    public void seek(long timestamp) {
      this.source.seek(timestamp);
    }
    
    @Override
    public long timestamp() {
      return this.timestamp;
    }

    @Override
    public boolean isInteger() {
      return false;
    }

    @Override
    public long longValue() {
      throw new ClassCastException("value #" + " is not a long in " + this); 
    }
    
    @Override
    public double doubleValue() {
      return this.value;
    }

    @Override
    public double toDouble() {
      return this.value;
    }

    @Override
    public long valueCount() {
      return 0;
    }
  }
}
