// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.query.processor.bucketquantile;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.pools.PooledObject;

/**
 * Computes percentiles on {@link NumericArrayType} arrays.
 * Notes:
 * - If all series are null or don't have data, we'll return series without data.
 * - If all values for a time are 0, we return NaN for all quantiles.
 * - NaNs in values are skipped unless the count breaches the threshold when
 *   configured.
 * - Negative values in the source are treated as NaNs.
 * 
 * Also note that we compute ALL percentiles in one pass so we avoid re-doing
 * the iteration.
 * 
 * TODO - may be some stuff around counters. For now if we have all of the 
 * buckets then it doesn't matter if they are monotonically increasing counts
 * as we compute the quantiles the same. BUT if it's expected that buckets 
 * without changes are _not_ reported and the query system needs to derive the
 * missing data, then we need to look at some history.
 * 
 * @since 3.0
 */
public class BucketQuantileNumericArrayProcessor extends BucketQuantileProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(
      BucketQuantileNumericArrayProcessor.class);
  
  /** The timestamp we'll return for the array. */
  protected TimeStamp timestamp;
  
  /**
   * Default ctor.
   * @param index The index in the total time series list.
   * @param node The non-null node.
   * @param sources The non-null source.
   */
  BucketQuantileNumericArrayProcessor(final int index,
                                        final BucketQuantile node,
                                        final TimeSeries[] sources,
                                        final TimeSeriesId base_id) {
    super(index, node, sources, base_id);
    limit = -1;
  }
  
  @Override
  void run() {
    PooledObject accumulator_pooled = null;
    PooledObject[] results_pooled = null;
    PooledObject indices_pooled = null;
    PooledObject thresholds_pooled = null;
    TypedTimeSeriesIterator[] iterators = null;
    
    try {
      final List<Double> qtiles = ((BucketQuantileConfig) node.config()).getQuantiles();
      quantiles = new double[qtiles.size()][];
      if (node.longArrayPool() != null) {
        accumulator_pooled = node.longArrayPool().claim(sources.length);
        results_pooled = new PooledObject[sources.length];
      }
      if (node.intArrayPool() != null) {
        indices_pooled = node.intArrayPool().claim(sources.length);
      }
      if (node.doubleArrayPool() != null) {
        thresholds_pooled = node.doubleArrayPool().claim(quantiles.length);
        pooled_objects = new PooledObject[qtiles.size()];
      }
      final double negative_threshold = ((BucketQuantileConfig) node.config()).getNanThreshold();
      final boolean cumulative = ((BucketQuantileConfig) node.config()).getCumulativeBuckets();
      final long[] accumulator = accumulator_pooled == null ? new long[sources.length] :
          (long[]) accumulator_pooled.object();
      final long[][] results = new long[sources.length][];
      final int[] indices = indices_pooled == null ? new int[sources.length] :
          (int[]) indices_pooled.object();
      final double[] p_thresholds = thresholds_pooled == null ? new double[quantiles.length] :
        (double[]) thresholds_pooled.object();
      iterators = new TypedTimeSeriesIterator[sources.length];
      
      for (int i = 0; i < sources.length; i++) {
        if (sources[i] == null) {
          indices[i] = -1;
          if (LOG.isTraceEnabled()) {
            LOG.trace("No source at " + i);
          }
          continue;
        }
        
        final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op = 
            sources[i].iterator(NumericArrayType.TYPE);
        if (!op.isPresent()) {
          indices[i] = -1;
          if (LOG.isTraceEnabled()) {
            LOG.trace("No array iterator for source at " + i + ": " + sources[i].id());
          }
          continue;
        }
        
        final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = 
            op.get();
        iterators[i] = iterator;
        if (!iterator.hasNext()) {
          indices[i] = -1;
          if (LOG.isTraceEnabled()) {
            LOG.trace("No data for the iterator for source at " + i + ": " 
                + sources[i].id());
          }
          continue;
        }
        
        final TimeSeriesValue<NumericArrayType> value = 
            (TimeSeriesValue<NumericArrayType>) iterator.next();
        if (value.value() == null) {
          indices[i] = -1;
          if (LOG.isTraceEnabled()) {
            LOG.trace("Null value from source at " + i + ": " + sources[i].id());
          }
          continue;
        }
        
        if (timestamp == null) {
          timestamp = value.timestamp().getCopy();
          if (base_id == null) {
            base_id = sources[i].id();
          }
        }
        indices[i] = value.value().offset();
        if (limit < 0) {
          // Better be the same for all.
          limit = value.value().end() - value.value().offset();
        } else if (value.value().end() - value.value().offset() != limit) {
          throw new IllegalStateException("The size of the array from the source "
              + "at " + i + ": " + sources[i].id() + " was less than the "
                  + "limit of [" + limit + "] => " + (value.value().end() - 
                      value.value().offset()));
        }
        
        if (value.value().isInteger()) {
          results[i] = value.value().longArray();
        } else {
          // ewwwww, eww! No!! This shouldn't happen because histograms
          // are counts!
          if (LOG.isTraceEnabled()) {
            LOG.trace("Converting from double [" + Arrays.toString(value.value().doubleArray()) + " to long at " + i + ": " + sources[i].id());
          }
          if (node.longArrayPool() != null) {
            results_pooled[i] = node.longArrayPool().claim(value.value().end() - value.value().offset());
          }
          final long[] new_value = results_pooled == null || results_pooled[i] == null ?
              new long[value.value().end() - value.value().offset()] :
                (long[]) results_pooled[i].object();
          final double[] double_value = value.value().doubleArray();
          for (int x = value.value().offset(); x < value.value().end(); x++) {
            if (Double.isNaN(double_value[x])) {
              new_value[x] = -1;
            } else {
              new_value[x] = (long) double_value[x];
            }
          }
          results[i] = new_value;
        }
      }
      
      if (limit < 0) {
        LOG.warn("No time series or data were found at index " + index);
        limit = 0;
        return;
      }
      
      // setup the ptiles
      for (int i = 0; i < quantiles.length; i++) {
        // TODO - pools
        if (node.doubleArrayPool() != null) {
          pooled_objects[i] = node.doubleArrayPool().claim(limit);
        }
        quantiles[i] = pooled_objects[i] == null ? new double[limit] :
          (double[]) pooled_objects[i].object();
      }
      
      // now that we have all of the counts in order we can start the iteration.
      for (int i = 0; i < limit; i++) {
        // load the accumulator
        // TODO - overflow
        long sum = -1;
        int negatives = 0;
        for (int x = 0; x < sources.length; x++) {
          if (indices[x] < 0) {
            // null
            accumulator[x] = -1;
            negatives++;
          } else {
            accumulator[x] = results[x][indices[x]++];
            if (accumulator[x] < 0) {
              negatives++;
            } else if (sum < 0 || cumulative) {
              sum = accumulator[x];
            } else {
              sum += accumulator[x];
            }
          }
        }
        
        // check for too much bad data.
        if (negatives > 0 && negative_threshold > 0) {
          if (((double) negatives / (double) quantiles.length) * 100 >= 
              negative_threshold) {
            for (int y = 0; y < quantiles.length; y++) {
              quantiles[y][i] = Double.NaN;
            }
            // skip processing
            continue;
          }
        } else if (sum < 0) {
          for (int y = 0; y < quantiles.length; y++) {
            quantiles[y][i] = Double.NaN;
          }
          continue;
        }
        
        // quantiles are sorted so we keep counting until we exceed the quantiles, 
        // then advance.
        for (int y = 0; y < qtiles.size(); y++) {
          p_thresholds[y] = ((double) sum) * qtiles.get(y);
        }
        
        int threshold_idx = 0;
        double so_far = -1;
        int last_non_zero_bucket = -1;
        OuterLoop:
        for (int z = 0; z < sources.length; z++) {
          if (accumulator[z] <= 0) {
            // nan or null.
            continue;
          }
          
          if (cumulative) {
            so_far = accumulator[z];
          } else {
            so_far += accumulator[z];
          }
          last_non_zero_bucket = z;
          while (so_far >= p_thresholds[threshold_idx]) {
            quantiles[threshold_idx][i] = 
                ((BucketQuantile) node).buckets()[z].report;
            p_thresholds[threshold_idx] = -Double.MIN_VALUE;
            threshold_idx++;
            if (threshold_idx >= quantiles.length) {
              // done, no more work to do.
              break OuterLoop;
            }
          }
        }
        
        if (so_far <= 0) {
          for (int z = 0; z < qtiles.size(); z++) {
            quantiles[z][i] = Double.NaN;
          }
        } else {
          for (int z = 0; z < qtiles.size(); z++) {
            if (p_thresholds[z] != -Double.MIN_VALUE) {
              if (last_non_zero_bucket >= 0) {
                quantiles[z][i] = 
                    ((BucketQuantile) node).buckets()[last_non_zero_bucket].report;
              } else {
                quantiles[z][i] = Double.NaN;
              }
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Unexpected exception", e);
    } finally {
      if (accumulator_pooled != null) {
        accumulator_pooled.release();
      }
      if (results_pooled != null) {
        for (int i = 0; i < results_pooled.length; i++) {
          if (results_pooled[i] != null) {
            results_pooled[i].release();
          }
        }
      }
      if (indices_pooled != null) {
        indices_pooled.release();
      }
      if (thresholds_pooled != null) {
        thresholds_pooled.release();
      }
      if (iterators != null) {
        for (int i = 0; i < iterators.length; i++) {
          if (iterators[i] != null) {
            try {
              iterators[i].close();
            } catch (IOException e) {
              // ignore for the most part.
              e.printStackTrace();
            }
          }
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (pooled_objects != null) {
      for (int i = 0; i < pooled_objects.length; i++) {
        if (pooled_objects[i] != null) {
          pooled_objects[i].release();
        }
      }
    }
  }

  @Override
  TimeSeries getSeries(final int percentile_index) {
    return new BucketQuantileNumericArrayIterator(percentile_index, this);
  }
}
