// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.query.processor;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryResult;

/**
 * A factory for generating {@link QueryNode}s that perform some kind of
 * data manipulation, hence the need to instantiate new iterators to work
 * over various types of data.
 * 
 * @since 3.0
 */
public interface ProcessorFactory extends QueryNodeFactory {
  
  /**
   * @return The types of data this factory can instantiate iterators for.
   */
  public Collection<TypeToken<?>> types();
  
  /**
   * Registers a specific iterator factory for a given type.
   * @param type A non-null type.
   * @param factory A non-null factory for the type.
   */
  public void registerIteratorFactory(final TypeToken<?> type, 
                                      final QueryIteratorFactory factory);
  
  /**
   * Returns an instantiated iterator of the given type if supported
   * @param type A non-null type.
   * @param node The parent node.
   * @param result The result this source is a part of.
   * @param sources A collection of sources to incorporate in the iterator.
   * @return A non-null iterator if successful or null if an iterator is
   * not present for the type.
   */
  public Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> newIterator(
      final TypeToken<?> type,
      final QueryNode node,
      final QueryResult result,
      final Collection<TimeSeries> sources);
  
  /**
   * Returns an instantiated iterator of the given type if supported
   * @param type A non-null type.
   * @param node The parent node.
   * @param result The result this source is a part of.
   * @param sources A map of sources to incorporate in the iterator.
   * @return A non-null iterator if successful or null if an iterator is
   * not present for the type.
   */
  public Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> newIterator(
      final TypeToken<?> type,
      final QueryNode node,
      final QueryResult result,
      final Map<String, TimeSeries> sources);
}
