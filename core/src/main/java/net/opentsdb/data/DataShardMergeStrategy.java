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
package net.opentsdb.data;

import java.util.List;

import com.google.common.reflect.TypeToken;

import io.opentracing.Span;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.query.context.QueryContext;

/**
 * A strategy to merge one or more data shards of the same 
 * {@link TimeSeriesDataType}.
 * <b>Invariant:</b>
 * All of the shards must be of the same type. If any of the shards are of the
 * wrong type for this implementation, 
 * {@link #merge(TimeSeriesId, List, QueryContext, Span)} should throw an 
 * {@link IllegalArgumentException} exception.
 * <p>
 * Note that implementations are not expected to check the {@link TimeSeriesId} 
 * of the individual shards during merge. Instead the ID of the resulting shard
 * is passed in to the {@link #merge(TimeSeriesId, List, QueryContext, Span)} 
 * method. 
 * 
 * @param <T> The type of data that this merger will work on.
 * 
 * @since 3.0
 */
public interface DataShardMergeStrategy<T extends TimeSeriesDataType> {

  /** @return The type of data this merger works on. */
  public TypeToken<T> type();
  
  /**
   * Merges the list of data shards into a single result set.
   * @param shards A non-null list of shards of the same {@link TimeSeriesDataType}.
   * @param context A non-null query context.
   * @param tracer_span An optional tracer span.
   * @return A non-null shard with the merged results.
   * @throws IllegalArgumentException if the ID was null, shards were null, one 
   * or more of the shards had the wrong time or one or more shards were null
   * in the list.
   */
  public TimeSeriesIterator<T> merge(final TimeSeriesId id, 
                            final List<TimeSeriesIterator<?>> shards, 
                            final QueryContext context, 
                            final Span tracer_span);
}
