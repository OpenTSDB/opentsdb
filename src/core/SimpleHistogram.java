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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.hbase.async.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import net.opentsdb.core.HistogramDataPoint.HistogramBucket;
import net.opentsdb.core.HistogramDataPoint.HistogramBucket.BucketType;

/**
 * A simple bucketed histogram with a fixed number of buckets, an underflow 
 * counter and an overflow counter. 
 * 
 * @since 3.0
 */
public class SimpleHistogram implements Histogram {
  private static Logger LOG = LoggerFactory.getLogger(SimpleHistogram.class);
  
  private final int id;
  
  @JsonProperty("buckets")
  TreeMap<HistogramBucket, Long> buckets = new TreeMap<HistogramBucket, Long>();

  @JsonProperty("underflow")
  Long underflow = 0L;

  @JsonProperty("overflow")
  Long overflow = 0L;

  public SimpleHistogram(final int id) {
    this.id = id;
  }
  
  public void addBucket(Float min, Float max, Long count) {
    if (min == null || max == null) {
      return;
    }
    if (count == null) {
      count = 0L; //Prevent Null Exception
    }

    buckets.put(new HistogramBucket(BucketType.REGULAR, min, max), count);
  }
  
  public byte[] histogram(final boolean include_id) {
    ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    Output output = new Output(outBuffer);

    int bucketCount = buckets.size();
    try {
      if (include_id) {
        output.writeByte(id);
      }
      output.writeShort(bucketCount);

      for (Map.Entry<HistogramBucket, Long> bucket : buckets.entrySet()) {
        output.writeFloat(bucket.getKey().getLowerBound());
        output.writeFloat(bucket.getKey().getUpperBound());
        output.writeLong(bucket.getValue(), true);
      }

      output.writeLong(this.getUnderflow(), true);
      output.writeLong(this.getOverflow(), true);
    } finally {
      output.close();
    }
    return outBuffer.toByteArray();
  }
  
  public void fromHistogram(byte[] raw, final boolean include_id) {
    if (raw.length < 6) {
      throw new IllegalArgumentException("Byte array shorter than 6 bytes "
          + "detected: " + Bytes.pretty(raw));
    }
    Input input = null;
    try {
      input = new Input(new ByteArrayInputStream(raw));
      if (include_id) {
        input.readByte(); // pull out the id.
      }
      int bucketCount = input.readShort();

      for (int i = 0; i < bucketCount; i++) {
        buckets.put(new HistogramBucket(BucketType.REGULAR, input.readFloat(), 
            input.readFloat()), input.readLong(true));
      }

      this.setUnderflow(input.readLong(true));
      this.setOverflow(input.readLong(true));
    } finally {
      if (input != null) {
        input.close();
      }
    }
  }
  
  private int calcBucketSum () {
    int sum = 0;
    for (Map.Entry<HistogramBucket, Long> entry : buckets.entrySet()) {
      sum += entry.getValue().intValue();
    }
    
    return sum;
  }

  public double percentile(double perc) {
    if (perc < 1.0 || perc > 100.0) {
      return -1.0;
    }

    int bucketSum = calcBucketSum();

    long runningCount = 0;
    double prevBucketArea = 0.0;
    double percValue = 0.0;

    for (Map.Entry<HistogramBucket, Long> entry : buckets.entrySet()) {
      runningCount += entry.getValue().intValue();
      Double currBucketArea = runningCount * 100.0 / bucketSum;

      if (currBucketArea >= perc) {
        //Find closest ranks
        //Float currBucketStart = entry.getKey().getLowerBound();
        //Float nextBucketStart = entry.getKey().getUpperBound();

        //percValue = currBucketStart + ((nextBucketStart - currBucketStart) * 
        // (perc - prevBucketArea) / (currBucketArea - prevBucketArea));
        percValue = (entry.getKey().getLowerBound() + 
            entry.getKey().getUpperBound()) / 2;
        break;
      } else {
        prevBucketArea = runningCount * 100.0 / bucketSum;
      }
    }

    return percValue;
  }

  @Override
  public List<Double> percentiles(List<Double> percs) {
    List<Double> percValues = new ArrayList<Double>();

    for (Double perc : percs) {
      percValues.add(percentile(perc));
    }

    return percValues;
  }

  @JsonIgnore
  @Override
  public Map<HistogramBucket, Long> getHistogram() {
    return Collections.unmodifiableMap(buckets);
  }

  @Override
  public SimpleHistogram clone() {
    SimpleHistogram cloneObj = new SimpleHistogram(id);

    for (Map.Entry<HistogramBucket, Long> bucket : buckets.entrySet()) {
      cloneObj.addBucket(bucket.getKey().getLowerBound(), 
          bucket.getKey().getUpperBound(), bucket.getValue());
    }
    cloneObj.setUnderflow(underflow);
    cloneObj.setOverflow(overflow);

    return cloneObj;
  }

  @Override
  public int getId() {
    return id;
  }
  
  public Long getBucketCount(Float min, Float max) {
    HistogramBucket qryBucket = new HistogramBucket(BucketType.REGULAR, min, max);
    if (buckets.containsKey(qryBucket)) {
      Long bucketCount = buckets.get(qryBucket);
      if (bucketCount == null) {
          return 0L;
      } else {
          return bucketCount;
      }
    } else {
      return 0L;
    }
  }

  public void write(Kryo kryo, Output output) {
    int bucketCount = buckets.size();
    output.writeShort(bucketCount);

    for (Map.Entry<HistogramBucket, Long> bucket : buckets.entrySet()) {
      bucket.getKey().write(kryo, output);
      output.writeLong(bucket.getValue(), true);
    }

    output.writeLong(this.getUnderflow(), true);
    output.writeLong(this.getOverflow(), true);
  }
  
  public void read(Kryo kryo, Input input) {
    int bucketCount = input.readShort();
    if (bucketCount < 1) {
      LOG.debug("Byte array passed has less than 1 histogram entry");
      return;
    }

    for (int i = 0; i < bucketCount; i++) {
      HistogramBucket bucket = new HistogramBucket();
        bucket.read(kryo, input);
        buckets.put(bucket, input.readLong(true));
    }

    this.setUnderflow(input.readLong(true));
    this.setOverflow(input.readLong(true));
  }
  
  public void aggregate(Histogram histo, HistogramAggregation func) {
    if (func == HistogramAggregation.SUM) {
      SimpleHistogram y1Histo = (SimpleHistogram) histo;
      for (Map.Entry<HistogramBucket, Long> bucket : 
        (Set<Map.Entry<HistogramBucket, Long>>) histo.getHistogram().entrySet()) {
        Long newCount = this.getBucketCount(bucket.getKey().getLowerBound(),
            bucket.getKey().getUpperBound()) + bucket.getValue();
        this.addBucket(bucket.getKey().getLowerBound(), 
            bucket.getKey().getUpperBound(), newCount);
      }
      this.setOverflow(y1Histo.getOverflow() + this.getOverflow());
      this.setUnderflow(y1Histo.getUnderflow() + this.getUnderflow());
    } else {
      LOG.debug("Unsupported histogram aggregation used");
    }
  }

  public void aggregate(List<Histogram> histos, HistogramAggregation func) {
    if (func == HistogramAggregation.SUM) {
      for (Histogram histo : histos) {
        this.aggregate(histo, HistogramAggregation.SUM);
      }
    } else {
      LOG.debug("Unsupported histogram aggregation used");
    }
  }

  public Long getUnderflow() {
    return underflow;
  }

  public void setUnderflow(Long underflow) {
    this.underflow = underflow;
  }

  public Long getOverflow() {
    return overflow;
  }

  public void setOverflow(Long overflow) {
    this.overflow = overflow;
  }
  
  /**
   * Generates an array of bucket lower bounds from {@code start} to {@code end}
   * with a fixed error interval (i.e. the span of the buckets). Up to 100 
   * buckets can be created using this method. Additionally, a range of 
   * measurements can be provided to "focus" on with a smaller bucket span while
   * the remaining buckets have a wider span. 
   * @param start The starting bucket measurement (lower bound).
   * @param end The ending bucket measurement (upper bound).
   * @param focusRangeStart The focus range start bound. Can be the same as
   * {@code start}.
   * @param focusRangeEnd The focus rang end bound. Can be the same as 
   * {@code end}.
   * @param errorPct The acceptable error with respect to bucket width. E.g.
   * 0.05 for 5%.
   * @return An array of bucket lower bounds.
   */
  public static double[] initializeHistogram (final float start, 
                                              final float end, 
                                              final float focusRangeStart, 
                                              final float focusRangeEnd, 
                                              final float errorPct) {
    if (Float.compare(start, end) >= 0) {
        throw new IllegalArgumentException("Histogram start (" + start + ") must be "
            + "less than Histogram end (" + end +")");
    } else if (Float.compare(focusRangeStart, focusRangeEnd) >= 0) {
        throw new IllegalArgumentException("Histogram focus range start (" 
            + focusRangeStart + ") must be less than Histogram focus end (" 
            + focusRangeEnd + ")");
    } else if (Float.compare(start, focusRangeStart) > 0 || 
        Float.compare(focusRangeStart, end) >= 0 || 
        Float.compare(start, focusRangeEnd) >= 0 || 
        Float.compare(focusRangeEnd, end) > 0) {
        throw new IllegalArgumentException("Focus range start (" + focusRangeStart 
            + ") and Focus range end (" + focusRangeEnd + ") must be greater "
                + "than Histogram start (" + start + ") and less than "
                    + "Histogram end (" + end + ")");
    } else if (Float.compare(errorPct, 0.0f) <= 0) {
        throw new IllegalArgumentException("Error rate (" + errorPct + ") must be "
            + "greater than zero");
    }
    int MAX_BUCKETS = 100;
    float stepSize = (1 + errorPct)/(1 - errorPct);
    int bucketcount = Double.valueOf(Math.ceil(
        Math.log(focusRangeEnd/focusRangeStart) / Math.log(stepSize)))
          .intValue() + 1;

    if (Float.compare(start, focusRangeStart) < 0) {
      bucketcount++;
    }

    if (Float.compare(focusRangeEnd, end) < 0) {
      bucketcount++;
    }

    if (bucketcount > MAX_BUCKETS) {
      throw new IllegalArgumentException("A max of " + MAX_BUCKETS 
          + " buckets are supported. " + bucketcount + " were requested");
    }

    double[] retval = new double[bucketcount];
    int j = 0;
    if (Float.compare(start, focusRangeStart) < 0) {
      retval[j] = start;
      j++;
    }

    for (float i = focusRangeStart; i < focusRangeEnd; i*=stepSize, j++) {
      retval[j] = i;
    }

    if (Float.compare(focusRangeEnd, end) < 0) {
      retval[j++] = focusRangeEnd;
    }
    retval[j] = end;

    return retval;
  }
}
