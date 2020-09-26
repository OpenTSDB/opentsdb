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
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pools.PooledObject;

/**
 * Computes quantiles on {@link NumericType} values.
 * Notes:
 * - If all series are null or don't have data, we'll return series without data.
 * - If all values for a time are 0, we skip the value for all quantiles.
 * - NaNs in values are skipped unless the count breaches the threshold when
 *   configured.
 * - Negative values in the source are treated as NaNs.
 * 
 * Also note that we compute ALL quantiles in one pass so we avoid re-doing
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
public class BucketQuantileNumericProcessor extends BucketQuantileProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(
      BucketQuantileNumericProcessor.class);
  
  /** The quantiles we'll populate and return to the caller. */
  protected long[] timestamps;
  protected PooledObject timestamps_pooled;
  
  /**
   * Default ctor.
   * @param index The index in the total time series list.
   * @param node The non-null node.
   * @param sources The non-null source.
   */
  BucketQuantileNumericProcessor(final int index,
                                 final BucketQuantile node,
                                 final TimeSeries[] sources,
                                 final TimeSeriesId base_id) {
    super(index, node, sources, base_id);
  }
  
  @Override
  void run() {
    PooledObject thresholds_pooled = null;
    TypedTimeSeriesIterator[] iterators = null;
    
    try {
      final List<Double> qtiles = ((BucketQuantileConfig) node.config()).getQuantiles();
      quantiles = new double[qtiles.size()][];
      if (node.longArrayPool() != null) {
        timestamps_pooled = node.longArrayPool().claim(DEFAULT_ARRAY_SIZE);
      }
      if (node.doubleArrayPool() != null) {
        thresholds_pooled = node.doubleArrayPool().claim(quantiles.length);
        pooled_objects = new PooledObject[qtiles.size()];
      }
      final double negative_threshold = ((BucketQuantileConfig) node.config()).getNanThreshold();
      final boolean cumulative = ((BucketQuantileConfig) node.config()).getCumulativeBuckets();
      final double[] p_thresholds = thresholds_pooled == null ? new double[quantiles.length] :
        (double[]) thresholds_pooled.object();
      iterators = new TypedTimeSeriesIterator[sources.length];
      timestamps = timestamps_pooled == null ? new long[DEFAULT_ARRAY_SIZE] : 
        (long[]) timestamps_pooled.object();
      
      boolean have_series = false;
      for (int i = 0; i < sources.length; i++) {
        if (sources[i] == null) {
          LOG.trace("No source at " + i);
          continue;
        }
        
        final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op = 
            sources[i].iterator(NumericType.TYPE);
        if (!op.isPresent()) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("No array iterator for source at " + i + ": " + sources[i].id());
          }
          continue;
        }
        
        final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = 
            op.get();
        if (!iterator.hasNext()) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("No data for the iterator for source at " + i + ": " 
                + sources[i].id());
          }
          continue;
        }
        iterators[i] = iterator;
        have_series = true;
        if (base_id == null) {
          base_id = sources[i].id();
        }
      }
      
      if (!have_series) {
        return;
      }
      
      // setup the ptiles
      for (int i = 0; i < quantiles.length; i++) {
        // TODO - pools
        if (node.doubleArrayPool() != null) {
          pooled_objects[i] = node.doubleArrayPool().claim();
        }
        quantiles[i] = pooled_objects[i] == null ? new double[DEFAULT_ARRAY_SIZE] :
          (double[]) pooled_objects[i].object();
      }
      
      final TimeSeriesValue<NumericType>[] values = 
          new TimeSeriesValue[sources.length];
      // load up the initial values.
      final TimeStamp current_ts = new MillisecondTimeStamp(Long.MAX_VALUE);
      for (int i = 0; i < iterators.length; i++) {
        while (iterators[i] != null && iterators[i].hasNext()) {
          values[i] = (TimeSeriesValue<NumericType>) iterators[i].next();
          if (values[i].value() == null) {
            continue;
          }
          
          if (values[i].timestamp().compare(Op.LT, current_ts)) {
            current_ts.update(values[i].timestamp());
          }
          break;
        }
      }
      
      // here's the big loop where we iterate, sync up the timestamps and
      // populate the quantiles arrays.
      final long[] includes = new long[sources.length];
      BigLoop:
      while (true) {
        long sum = -1;
        int negatives = 0;
        
        // first we get the sums, negatives and sync.
        for (int i = 0; i < sources.length; i++) {
          if (values[i] == null) {
            includes[i] = -1;
            negatives++;
            continue;
          }
          
          if (values[i].timestamp().compare(Op.EQ, current_ts)) {
            final NumericType value = values[i].value();
            if (value.isInteger()) {
              if (value.longValue() < 0) {
                negatives++;
                includes[i] = -1;
              } else if (sum < 0 || cumulative) {
                sum = value.longValue();
                includes[i] = value.longValue();
              } else {
                sum += value.longValue();
                includes[i] = value.longValue();
              }
            } else {
              if (!Double.isFinite(value.doubleValue()) || value.doubleValue() < 0) {
                negatives++;
                includes[i] = -1;
              } else if (sum < 0 || cumulative) {
                if (LOG.isTraceEnabled()) {
                  LOG.trace("Casting a double to a long.");
                }
                sum = (long) value.doubleValue();
                includes[i] = (long) value.doubleValue();
              } else {
                if (LOG.isTraceEnabled()) {
                  LOG.trace("Casting a double to a long.");
                }
                sum += (long) value.doubleValue();
                includes[i] = (long) value.doubleValue();
              }
            }
          } else {
            includes[i] = -1;
            negatives++; // effectively nan.
          }
        }
        
        // don't bother emitting NaNs if everything is NaN'd out
        if (negatives > 0 && negative_threshold > 0) {
          if (((double) negatives / (double) quantiles.length) * 100 >= 
              negative_threshold) {
            // skip processing this NaN.
            if (advance(iterators, values, includes, current_ts)) {
              continue;
            } else {
              break BigLoop;
            }
          }
        } else if (sum < 0) {
          // skip processing this NaN.
          if (advance(iterators, values, includes, current_ts)) {
            continue;
          } else {
            break BigLoop;
          }
        }
        
        // so far so good, we _may_ have a value. Let's check and see if we
        // can compute ptiles, but only after we make sure we have space.
        if (limit >= timestamps.length) {
          final int new_size = timestamps.length * 2;
          // GROW !
          final PooledObject temp_ts_pooled = node.longArrayPool() != null ? 
              node.longArrayPool().claim(new_size) : null;
          final long[] temp_timestamps = temp_ts_pooled == null ?
              new long[new_size] :
                (long[]) temp_ts_pooled.object();
          System.arraycopy(timestamps, 0, temp_timestamps, 0, limit);
          timestamps = temp_timestamps;
          if (timestamps_pooled != null) {
            timestamps_pooled.release();
          }
          timestamps_pooled = temp_ts_pooled;
          
          for (int i = 0; i < quantiles.length; i++) {
            final PooledObject pooled = node.doubleArrayPool() != null ?
                node.doubleArrayPool().claim(new_size) : null;
            final double[] temp = pooled == null ?
                new double[new_size] : (double[]) pooled.object();
            System.arraycopy(quantiles[i], 0, temp, 0, limit);
            quantiles[i] = temp;
            if (pooled_objects != null) {
              if (pooled_objects[i] != null) {
                pooled_objects[i].release();
                pooled_objects[i] = pooled;
              }
            }
          }
        }
        
        // compute thresholds.
        for (int i = 0; i < qtiles.size(); i++) {
          p_thresholds[i] = ((double) sum) * qtiles.get(i);
        }
        
        int threshold_idx = 0;
        double so_far = -1;
        int last_non_zero_bucket = -1;
        OuterLoop:
        for (int i = 0; i < sources.length; i++) {
          if (includes[i] <= 0) {
            continue;
          }
          
          if (cumulative) {
            so_far = includes[i];
          } else {
            so_far += includes[i];
          }
          last_non_zero_bucket = i;
          while (so_far >= p_thresholds[threshold_idx]) {
            quantiles[threshold_idx][limit] = 
                ((BucketQuantile) node).buckets()[i].report;
            p_thresholds[threshold_idx] = -Double.MIN_VALUE;
            threshold_idx++;
            if (threshold_idx >= quantiles.length) {
              // done, no more work to do.
              break OuterLoop;
            }
          }
        }
        
        // double check our values and set overflows if needed.
        if (so_far <= 0) {
          // all invalid so toss it.
          if (advance(iterators, values, includes, current_ts)) {
            continue;
          } else {
            break BigLoop;
          }
        }
        
        // handle stragglers where we didn't meet the threshold.
        for (int z = 0; z < qtiles.size(); z++) {
          if (p_thresholds[z] != -Double.MIN_VALUE) {
            if (last_non_zero_bucket >= 0) {
              quantiles[z][limit] = 
                  ((BucketQuantile) node).buckets()[last_non_zero_bucket].report;
            } else {
              quantiles[z][limit] = Double.NaN;
            }
          }
        }
        
        // we had a valid value.
        timestamps[limit] = current_ts.epoch();
        limit++;
        if (advance(iterators, values, includes, current_ts)) {
          continue;
        } else {
          break BigLoop;
        }
      }
    } catch (Exception e) {
      LOG.error("Unexpected exception", e);
    } finally {
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
    if (timestamps_pooled != null) {
      timestamps_pooled.release();
    }
  }

  @Override
  TimeSeries getSeries(final int quantile_index) {
    return new BucketQuantileNumericIterator(quantile_index, this);
  }
  
  boolean advance(final TypedTimeSeriesIterator[] iterators, 
                  final TimeSeriesValue<NumericType>[] values,
                  final long[] includes,
                  final TimeStamp current_ts) {
    long current_epoch = current_ts.epoch();
    current_ts.updateMsEpoch(Long.MAX_VALUE);
    boolean has_data = false;
    for (int i = 0; i < iterators.length; i++) {
      if (includes[i] >= 0 || 
          (values[i] != null && values[i].timestamp().epoch() == current_epoch)) {
        // we know we have to increment it.
        if (!iterators[i].hasNext()) {
          // done with this series
          values[i] = null;
        }
        
        while (iterators[i].hasNext()) {
          values[i] = (TimeSeriesValue<NumericType>) iterators[i].next();
          if (values[i].value() != null) {
            if (values[i].timestamp().compare(Op.LT, current_ts)) {
              current_ts.update(values[i].timestamp());
            }
            has_data = true;
            break;
          }
          
          if (!iterators[i].hasNext()) {
            values[i] = null;
            break;
          }
        }
      } else if (values[i] != null) {
        if (values[i].timestamp().compare(Op.LT, current_ts)) {
          current_ts.update(values[i].timestamp());
        }
        has_data = true;
      }
    }
    
    return has_data;
  }
}
