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
import java.util.TreeMap;

/**
 * An adapter of TSDB's {@code HistogramDataPoint} interface with Yamas's
 * {@code Histogram} interface.
 * 
 * @since 2.4
 */
public class SimpleHistogramDataPointAdapter implements HistogramDataPoint {

  private final Histogram histogram;

  private final long timestamp;

  public SimpleHistogramDataPointAdapter(final Histogram histogram, 
                                         final long timestamp) {
    this.histogram = histogram;
    this.timestamp = timestamp;
  }
  
  protected SimpleHistogramDataPointAdapter(
      final SimpleHistogramDataPointAdapter rhs) {
    histogram = rhs.histogram.clone();
    timestamp = rhs.timestamp;
  }

  protected SimpleHistogramDataPointAdapter(
      final SimpleHistogramDataPointAdapter rhs, 
      final long timestamp) {
    histogram = rhs.histogram.clone();
    this.timestamp = timestamp;
  }

  @Override
  public long timestamp() {
    return timestamp;
  }

  @Override
  public byte[] getRawData() {
    return histogram.histogram();
  }

  @Override
  public void resetFromRawData(final byte[] raw_data) {
    histogram.fromHistogram(raw_data);
  }

  @Override
  public double percentile(final double p) {
    return histogram.percentile(p);
  }

  @Override
  public List<Double> percentile(final List<Double> p) {
    return histogram.percentiles(p);
  }

  @Override
  public void aggregate(final HistogramDataPoint histo, 
                        final HistogramAggregation func) {
    if (!(histo instanceof SimpleHistogramDataPointAdapter)) {
      throw new IllegalArgumentException("The object must be an instance of the " 
          + "YamasHistogramDataPointAdapter");
    }
    histogram.aggregate(((SimpleHistogramDataPointAdapter) histo).histogram, 
        mapAggregation(func));
  }

  @Override
  public HistogramDataPoint clone() {
    return new SimpleHistogramDataPointAdapter(this);
  }
  
  @Override
  public HistogramDataPoint cloneAndSetTimestamp(final long timestamp) {
    return new SimpleHistogramDataPointAdapter(this, timestamp);
  }

  private HistogramAggregation mapAggregation(final HistogramAggregation func) {
    switch (func) {
    case SUM:
      return HistogramAggregation.SUM;
    }

    throw new UnsupportedOperationException("Failed to map the aggregator.");
  }
  
  @Override
  public Map<HistogramBucket, Long> getHistogramBucketsIfHas() {
    if (histogram instanceof SimpleHistogram) {
      SimpleHistogram yms1_histogram = (SimpleHistogram) (histogram);
      Map<HistogramBucket, Long> buckets = yms1_histogram.getHistogram();
      if (null != buckets) {
        Map<HistogramBucket, Long> result = new TreeMap<HistogramBucket, Long>();
        for (Map.Entry<HistogramBucket, Long> entry : buckets.entrySet()) {
          HistogramBucket bucket = new HistogramBucket(
              HistogramBucket.BucketType.REGULAR,
              entry.getKey().getLowerBound(), entry.getKey().getUpperBound());
          result.put(bucket, entry.getValue());
        }

        HistogramBucket underflow_bucket = new HistogramBucket(
            HistogramBucket.BucketType.UNDERFLOW, 0.0f, 0.0f);
        result.put(underflow_bucket, yms1_histogram.getUnderflow());

        HistogramBucket overflow_bucket = new HistogramBucket(
            HistogramBucket.BucketType.OVERFLOW, 0.0f, 0.0f);
        result.put(overflow_bucket, yms1_histogram.getOverflow());
        return result;
      }
    } else {
      throw new UnsupportedOperationException("The founding histogram object "
          + "is not one of class Yamas1Histogram");
    }
    return null;
  }
}
