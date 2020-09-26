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

import java.io.Closeable;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.pools.PooledObject;

/**
 * Base class for processors that will compute the quantiles.
 * 
 * @since 3.0
 */
public abstract class BucketQuantileProcessor implements Closeable {

  protected static final int DEFAULT_ARRAY_SIZE = 1024;
  
  /** The node we belong to. */
  protected final BucketQuantile node;
  
  /** The sorted sources. */
  protected final TimeSeries[] sources;
  
  /** The real index in the overall result set. */
  protected final int index;
  
  /** The quantiles to be populated. */
  protected double[][] quantiles;
  protected PooledObject[] pooled_objects;
  
  /** The base ID we'll use. */
  protected TimeSeriesId base_id;
  
  /** Final result length for each quantile. */
  protected int limit;
  
  /**
   * Default ctor.
   * @param index The real index of this processor in the result set.
   * @param node The node we belong to.
   * @param sources The array of source metrics.
   */
  BucketQuantileProcessor(final int index,
                          final BucketQuantile node,
                          final TimeSeries[] sources,
                          final TimeSeriesId base_id) {
    this.index = index;
    this.node = node;
    this.sources = sources;
    this.base_id = base_id;
  }
  
  /**
   * Called when we need to pull out a time series from the overall result set.
   */
  abstract void run();
  
  /**
   * Gets the time series with the percentile appended and iterates over the
   * quantiles already calculated by {@link #run()}.
   * 
   * @param quantile_index The index of the quantile in the list of quantiles
   * requested by the user.
   * @return The non-null time series to work with.
   */
  abstract TimeSeries getSeries(final int quantile_index);
}
