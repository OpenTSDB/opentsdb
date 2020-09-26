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

import com.google.common.collect.Maps;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;

/**
 * Base implementation for the iterators.
 * 
 * @since 3.0
 */
public abstract class BucketQuantileIterator implements TimeSeries {
  
  protected final int quantile_index;
  protected final BucketQuantileProcessor processor;
  protected TimeSeriesId id;
  
  BucketQuantileIterator(final int quantile_index, 
                         final BucketQuantileProcessor processor) {
    this.quantile_index = quantile_index;
    this.processor = processor;
  }
  
  @Override
  public TimeSeriesId id() {
    if (id != null) {
      return id;
    }
    // TODO - byte id
    BaseTimeSeriesStringId.Builder builder = BaseTimeSeriesStringId.newBuilder()
        .setMetric(((BucketQuantileConfig) processor.node.config()).getAs());
    if (processor.base_id != null) {
      builder.setTags(Maps.newHashMap(((TimeSeriesStringId) processor.base_id).tags()));
    }
        // TODO - ?
        //.setAggregatedTags(((TimeSeriesStringId) base_id).aggregatedTags())
        //.setDisjointTags(((TimeSeriesStringId) base_id).disjointTags())
    builder.addTags(BucketQuantileFactory.QUANTILE_TAG, 
        "".format("%.3f", 
            ((BucketQuantileConfig) processor.node.config()).getQuantiles().get(quantile_index) * 100));
    id = builder.build();
    return id;
  }
}
