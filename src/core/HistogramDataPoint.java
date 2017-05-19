// This file is part of OpenTSDB.
// Copyright (C) 2011-2012  The OpenTSDB Authors.
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
import java.util.Set;

/**
 * Represents a single histogram data point.
 *
 */
public interface HistogramDataPoint extends Cloneable {
    byte PREFIX = 0x6;

    /**
     * Returns the timestamp (in milliseconds) associated with this data point.
     * @return A strictly positive, 32 bit integer.
     */
    long timestamp();

    /**
     * Get the encoded value of this histogram.
     * NOTE: implementation should store the serialize information
     * in the byte array so latter it can decide how to deserialize it back
     * @return The encoded value os this histogram data point
     */
    byte[] getRawData();

    /**
     * Decode the raw data and reset the current histogram data point to the
     * decoded value
     * @param raw_data The encoded value of the histogram data point
     */
    void resetFromRawData(final byte[] raw_data);

    /**
     * Calculate percentile of this histogram data point
     * @param p the distribution threshold
     * @return The percentile value
     */
    double percentile(final double p);

    /**
     * Calculate percentile values of this histogram data point
     * @param p the distribution threshold list
     * @return A list of the percentile values
     */
    List<Double> percentile(final List<Double> p);

    void aggregate(HistogramDataPoint histo, HistogramAggregation func);
    
    /**
     * Create and return a copy of this object
     * 
     * @return A deep copy object {@link HistogramDataPoint}
     */
    HistogramDataPoint clone();
    
    
    HistogramDataPoint cloneAndSetTimestamp(final long timestamp);
    
    
    /////////////////////////////////////////////////////////////////////////////////////////////
    // A nested class to present the bucket information
    ////////////////////////////////////////////////////////////////////////////////////////////
    public class HistogramBucket implements Comparable<HistogramBucket> {
      public enum BucketType {
        UNDERFLOW, REGULAR, OVERFLOW
      }

      private final BucketType type;
      private final float lower_bound;
      private final float upper_bound;

      public HistogramBucket(final BucketType type, final float lower_bound, 
          final float uper_bound) {
        this.type = type;
        this.lower_bound = lower_bound;
        this.upper_bound = uper_bound;
      }
      
      public BucketType bucketType() {
        return this.type;
      }
      
      public float getLowerBound() {
        return this.lower_bound;
      }
      
      public float getUpperBound() {
        return this.upper_bound;
      }
      
      
      @Override
      public boolean equals(Object that) {
        if (this == that) {
          return true;
        }
        
        if (that == null || getClass() != that.getClass()) {
          return false;
        }
        
        HistogramBucket bk = (HistogramBucket)that;
        if (bucketType() != bk.bucketType()) {
          return false;
        }
        
        if ((BucketType.UNDERFLOW == bucketType() && BucketType.UNDERFLOW  == bk.bucketType()) || 
            (BucketType.OVERFLOW == bucketType() && BucketType.OVERFLOW == bk.bucketType())) {
          return true;
        }
        
        if (Float.compare(getLowerBound(), bk.getLowerBound()) != 0) {
          return false;
        }
        
        return (Float.compare(getUpperBound(), bk.getUpperBound()) == 0);
      }

      @Override
      public int compareTo(HistogramBucket that) {
        if (this.equals(that)) {
          return 0;
        } else if (BucketType.UNDERFLOW == type) {
          return -1;
        } else if (BucketType.REGULAR == type) {
          int lower_bound_compare = Float.compare(getLowerBound(), that.getLowerBound());
          if (lower_bound_compare != 0) {
            return lower_bound_compare;
          } else {
            return Float.compare(getUpperBound(), that.getUpperBound());
          }
        } else if (BucketType.OVERFLOW == type) {
          return +1;
        }
        
        return 0;
      }
    }
    
    /**
     * Get buckets from this histogram data point 
     * @return
     */
    Map<HistogramBucket, Long> getHistogramBucketsIfHas();
    
     /**
    void aggregate(List<Histogram> histos, HistoAggregation func);
     */
}
