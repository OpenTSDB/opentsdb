// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;

/**
 * The factory used to generate a {@link QueryNode} for a new query execution.
 * 
 * @since 3.0
 */
public interface QueryNodeFactory {

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