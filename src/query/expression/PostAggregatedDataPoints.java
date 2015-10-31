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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.hbase.async.Bytes.ByteMap;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.SeekableView;
import net.opentsdb.meta.Annotation;

import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;

public class PostAggregatedDataPoints implements DataPoints {

  private final DataPoints baseDataPoints;
  private final DataPoint[] points;

  private String alias = null;

  public PostAggregatedDataPoints(DataPoints baseDataPoints, DataPoint[] points) {
    this.baseDataPoints = baseDataPoints;
    this.points = points;
  }

  @Override
  public String metricName() {
    if (alias != null) return alias;
    else return baseDataPoints.metricName();
  }

  @Override
  public Deferred<String> metricNameAsync() {
    if (alias != null) return Deferred.fromResult(alias);
    return baseDataPoints.metricNameAsync();
  }

  @Override
  public Map<String, String> getTags() {
    if (alias != null) return Maps.newHashMap();
    else return baseDataPoints.getTags();
  }

  @Override
  public Deferred<Map<String, String>> getTagsAsync() {
    Map<String, String> def = new HashMap<String, String>();
    if (alias != null) return Deferred.fromResult(def);
    return baseDataPoints.getTagsAsync();
  }

  @Override
  public List<String> getAggregatedTags() {
    return baseDataPoints.getAggregatedTags();
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  @Override
  public Deferred<List<String>> getAggregatedTagsAsync() {
    return baseDataPoints.getAggregatedTagsAsync();
  }

  @Override
  public List<String> getTSUIDs() {
    return baseDataPoints.getTSUIDs();
  }

  @Override
  public List<Annotation> getAnnotations() {
    return baseDataPoints.getAnnotations();
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

  static class SeekableViewImpl implements SeekableView {

    private int pos=0;
    private final DataPoint[] dps;

    public SeekableViewImpl(DataPoint[] dps) {
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
        throw new NoSuchElementException("tsdb uses exceptions to determine end of iterators");
      }
    }

    @Override
    public void remove() {
      throw new RuntimeException("Not supported exception");
    }

    @Override
    public void seek(long timestamp) {
      for (int i=pos; i<dps.length; i++) {
        if (dps[i].timestamp() >= timestamp) {
          break;
        } else {
          pos++;
        }
      }
    }
  }

  @Override
  public ByteMap<byte[]> getTagUids() {
    return baseDataPoints.getTagUids();
  }

  @Override
  public int getQueryIndex() {
    return baseDataPoints.getQueryIndex();
  }

}
