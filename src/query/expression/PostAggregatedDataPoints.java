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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.hbase.async.Bytes.ByteMap;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.SeekableView;
import net.opentsdb.meta.Annotation;

import com.stumbleupon.async.Deferred;

/**
 * A class to store an array of data points processed through expressions along
 * with the original meta data of the result set (metric, tags, etc).
 * @since 2.3
 */
public class PostAggregatedDataPoints implements DataPoints {

  /** The original results from storage, used for fetching meta data */
  private final DataPoints base_data_points;
  
  /** The results of the expression calculation */
  private final DataPoint[] points;

  /** An optional alias for the results */
  private String alias = null;

  /**
   * Default ctor
   * @param base_data_points The original results from storage for fetching meta
   * @param points The results of the expression calculation
   */
  public PostAggregatedDataPoints(final DataPoints base_data_points, 
      final DataPoint[] points) {
    if (base_data_points == null) {
      throw new IllegalArgumentException("base_data_points cannot be null");
    }
    if (points == null) {
      throw new IllegalArgumentException("points cannot be null");
    }
    this.base_data_points = base_data_points;
    this.points = points;
  }

  @Override
  public String metricName() {
    if (alias != null) {
      return alias;
    } else {
      return base_data_points.metricName();
    }
  }

  @Override
  public Deferred<String> metricNameAsync() {
    if (alias != null) {
      return Deferred.fromResult(alias);
    }
    return base_data_points.metricNameAsync();
  }

  @Override
  public Map<String, String> getTags() {
    if (alias != null) {
      return Collections.<String, String>emptyMap();
    } else {
      return base_data_points.getTags();
    }
  }

  @Override
  public Deferred<Map<String, String>> getTagsAsync() {
    if (alias != null) {
      return Deferred.fromResult(Collections.<String, String>emptyMap());
    }
    return base_data_points.getTagsAsync();
  }

  @Override
  public List<String> getAggregatedTags() {
    if (alias != null) {
      return Collections.<String>emptyList();
    }
    return base_data_points.getAggregatedTags();
  }

  @Override
  public Deferred<List<String>> getAggregatedTagsAsync() {
    if (alias != null) {
      return Deferred.fromResult(Collections.<String>emptyList());
    }
    return base_data_points.getAggregatedTagsAsync();
  }

  @Override
  public List<String> getTSUIDs() {
    return base_data_points.getTSUIDs();
  }

  @Override
  public List<Annotation> getAnnotations() {
    return base_data_points.getAnnotations();
  }
  
  @Override
  public ByteMap<byte[]> getTagUids() {
    return base_data_points.getTagUids();
  }

  @Override
  public int getQueryIndex() {
    return base_data_points.getQueryIndex();
  }

  @Override
  public int size() {
    return points.length;
  }

  @Override
  public int aggregatedSize() {
    return points.length;
  }

  @Override
  public SeekableView iterator() {
    return new SeekableViewImpl(points);
  }

  @Override
  public long timestamp(int i) {
    return points[i].timestamp();
  }

  @Override
  public boolean isInteger(int i) {
    return points[i].isInteger();
  }

  @Override
  public long longValue(int i) {
    return points[i].longValue();
  }

  @Override
  public double doubleValue(int i) {
    return points[i].doubleValue();
  }
  
  /**
   * An iterator working over the data points resulting from the expression
   * calculation.
   */
  static class SeekableViewImpl implements SeekableView {

    private int pos = 0;
    private final DataPoint[] dps;
    
    SeekableViewImpl(final DataPoint[] dps) {
      this.dps = dps;
    }

    @Override
    public boolean hasNext() {
      return pos < dps.length;
    }

    @Override
    public DataPoint next() {
      if (hasNext()) {
        return dps[pos++];
      } else {
        throw new NoSuchElementException("no more elements");
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void seek(long timestamp) {
      for (int i = pos; i < dps.length; i++) {
        if (dps[i].timestamp() >= timestamp) {
          break;
        } else {
          pos++;
        }
      }
    }
  }

  /** @param alias The alias to set for the time series. Used in place of
   * the metric and nulls out all tags. */
  public void setAlias(String alias) {
    this.alias = alias;
  }
}
