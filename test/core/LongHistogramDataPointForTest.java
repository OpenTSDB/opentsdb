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

public class LongHistogramDataPointForTest implements HistogramDataPoint {
  private long timestamp;
  private long data;

  LongHistogramDataPointForTest(final long timestamp, final byte[] raw_data) {
    this.timestamp = timestamp;
    this.data = Bytes.getLong(raw_data);
  }
  
  protected LongHistogramDataPointForTest(final LongHistogramDataPointForTest rhs) {
    this.timestamp = rhs.timestamp;
    this.data = rhs.data;
  }
  
  protected LongHistogramDataPointForTest(final LongHistogramDataPointForTest rhs, 
      final long timestamp) {
    this.data = rhs.data;
    this.timestamp = timestamp;
  }
  
  @Override
  public long timestamp() {
    return this.timestamp;
  }

  public void setTimeStamp(final long timestamp) {
    this.timestamp = timestamp;
  }
  
  @Override
  public byte[] getRawData() {
    return Bytes.fromLong(this.data);
  }

  public void setRawData(final byte[] data) {
    this.data = Bytes.getLong(data);
  }
  
  @Override
  public void resetFromRawData(byte[] raw_data) {
    // TODO Auto-generated method stub

  }

  @Override
  public double percentile(double p) {
    return data * p;
  }

  @Override
  public List<Double> percentile(List<Double> p) {
    List<Double> rs = new ArrayList<Double>();
    for (Double d : p) {
      rs.add(d.doubleValue() * data);
    }
    return rs;
  }

  @Override
  public void aggregate(HistogramDataPoint histo, HistogramAggregation func) {
    if (!(histo instanceof LongHistogramDataPointForTest)) {
      throw new IllegalArgumentException("The object must be an instance of the " 
          + "LongHistogramDataPointForTest");
    }
    
    long agg = this.data + Bytes.getLong(histo.getRawData());
    this.data = agg;
  }
  
  @Override
  public HistogramDataPoint clone() {
    return new LongHistogramDataPointForTest(this);
  }
  
  @Override
  public HistogramDataPoint cloneAndSetTimestamp(final long timestamp) {
    return new LongHistogramDataPointForTest(this, timestamp);
  }
  
  @Override
  public Map<HistogramBucket, Long> getHistogramBucketsIfHas() {
    throw new UnsupportedOperationException(
        "LongHistogramDataPointForTest doesn't support getHistogramBuckets operation");
  }
}
