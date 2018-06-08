// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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
package net.opentsdb.query;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;

/**
 * A factory that returns iterators for a {@link QueryNode} to mutate time series
 * flowing through them.
 * 
 * @since 3.0
 */
public interface QueryIteratorFactory {
  
  /** @return A non-null collection of types supported by this factory. */
  public Collection<TypeToken<?>> types();
  
  /**
   * Returns an iterator using a non-keyed collection of time series sources.
   * @param node A non-null query node the iterator belongs to.
   * @param result The result this source is a part of.
   * @param sources A non-null and non-empty list of time series sources to
   * read from. 
   * @return A non-null iterator over a specific data type.
   */
  public Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> newIterator(
      final QueryNode node,
      final QueryResult result,
      final Collection<TimeSeries> sources);

  /**
   * Returns an iterator using a keyed collection of time series sources.
   * @param node A non-null query node the iterator belongs to.
   * @param result The result this source is a part of.
   * @param sources A non-null and non-empty list of time series sources to
   * read from. 
   * @return A non-null iterator over a specific data type.
   */
  public Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> newIterator(
      final QueryNode node,
      final QueryResult result,
      final Map<String, TimeSeries> sources);
}
