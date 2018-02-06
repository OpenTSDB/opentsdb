// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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

import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;

/**
 * The factory used to generate a {@link QueryNode} for a new query execution.
 * 
 * @since 3.0
 */
public interface QueryNodeFactory extends TSDBPlugin {

  /**
   * Instantiates a new node using the given context and config.
   * @param context A non-null query pipeline context.
   * @param config A query node config. May be null if the node does not
   * require a configuration.
   * @return
   */
  public QueryNode newNode(final QueryPipelineContext context, 
                           final QueryNodeConfig config);
  
  /**
   * The descriptive ID of the factory used when parsing queries.
   * @return A non-null unique ID of the factory.
   */
  public String id();
 
  public Collection<TypeToken<?>> types();
  
  public void registerIteratorFactory(final TypeToken<?> type, 
                                      final QueryIteratorFactory factory);
  
  public Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> newIterator(
      final TypeToken<?> type,
      final QueryNode node,
      final Collection<TimeSeries> sources);
  
  public Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> newIterator(
      final TypeToken<?> type,
      final QueryNode node,
      final Map<String, TimeSeries> sources);
}
