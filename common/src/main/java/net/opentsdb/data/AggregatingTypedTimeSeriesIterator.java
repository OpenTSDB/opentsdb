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
package net.opentsdb.data;

/**
 * An interface that extends the iterator with a {@link #next(Aggregator)} method
 * that will populate the given aggregator with the values at the proper 
 * index. This can only be used for downsampled, normalized data.
 * 
 * @since 3.0
 */
public interface AggregatingTypedTimeSeriesIterator<T extends TimeSeriesDataType> 
    extends TypedTimeSeriesIterator<T> {

  /**
   * Runs through the data and adds the results to the aggregator at the proper
   * index.
   * @param aggregator A non-null aggregator to populate.
   */
  public void next(final Aggregator aggregator);
  
}
