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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.hbase.async.Bytes;

public class LongHistogramDataPointForTest implements Histogram {
  private final int id;
  private long data;
  
  public LongHistogramDataPointForTest(final int id) {
    this.id = id;
  }
  
  LongHistogramDataPointForTest(final int id, final long value) {
    this.id = id;
    this.data = value;
  }
  
  protected LongHistogramDataPointForTest(final LongHistogramDataPointForTest rhs) {
    this.id = rhs.id;
    this.data = rhs.data;
  }
  
  protected LongHistogramDataPointForTest(final LongHistogramDataPointForTest rhs, 
      final long timestamp) {
    this.id = rhs.id;
    this.data = rhs.data;
  }
  
  public void setRawData(final byte[] data) {
    this.data = Bytes.getLong(data);
  }
  
  @Override
  public double percentile(double p) {
    return data * p;
  }

  @Override
  public List<Double> percentiles(List<Double> p) {
    List<Double> rs = new ArrayList<Double>();
    for (Double d : p) {
      rs.add(d.doubleValue() * data);
    }
    return rs;
  }

  @Override
  public void aggregate(Histogram histo, HistogramAggregation func) {
    if (!(histo instanceof LongHistogramDataPointForTest)) {
      throw new IllegalArgumentException("The object must be an instance of the " 
          + "LongHistogramDataPointForTest");
    }
    
    long agg = this.data + Bytes.getLong(histo.histogram(false));
    this.data = agg;
  }
  
  @Override
  public Histogram clone() {
    return new LongHistogramDataPointForTest(this);
  }
  
  @Override
  public int getId() {
    return id;
  }
  
  @Override
  public byte[] histogram(boolean include_id) {
    if (include_id) {
      final byte[] result = new byte[9];
      result[0] = (byte) id;
      System.arraycopy(Bytes.fromLong(data), 0, result, 1, 8);
      return result;
    }
    return Bytes.fromLong(data);
  }

  @Override
  public void fromHistogram(byte[] raw, boolean includes_id) {
    if (includes_id) {
      data = Bytes.getLong(raw, 1);
    } else {
      data = Bytes.getLong(raw);
    }
  }

  @Override
  public Map getHistogram() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void aggregate(List<Histogram> histos, HistogramAggregation func) {
    // TODO Auto-generated method stub
    
  }
  
}
