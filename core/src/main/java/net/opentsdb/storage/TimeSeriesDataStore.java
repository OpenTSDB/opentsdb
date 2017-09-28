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
package net.opentsdb.storage;

import com.stumbleupon.async.Deferred;

import io.opentracing.Span;
import net.opentsdb.core.TsdbPlugin;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.QueryExecution;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.stats.TsdbTrace;

/**
 * The class for reading or writing time series data to a local data store. 
 * <p>
 * This class is generally meant to implement a time series storage schema on
 * either:
 * <ul>
 * <li>A local system such as using flat files or an LSM implementation on the
 * local disk using something like RocksDB or LevelDB.<li>
 * <li>A remote distributed store such as HBase, Bigtable or Cassandra.<li>
 * </ul>
 *
 * TODO - more complete calls and documentation
 * 
 * @since 3.0
 */
public abstract class TimeSeriesDataStore extends TsdbPlugin {
  
  /**
   * Writes the given value to the data store.
   * @param id A non-null ID for the value.
   * @param value A non-null value to write.
   * @param trace An optional tracer.
   * @param upstream_span An optional span for tracing.
   * @return A deferred resolving to null on success or an exception if the 
   * value was unable to be written.
   */
  public abstract Deferred<Object> write(final TimeSeriesId id,
                                         final TimeSeriesValue<?> value, 
                                         final TsdbTrace trace, 
                                         final Span upstream_span);
  
  /**
   * Executes the given query against the data store.
   * @param context A non-null query context.
   * @param query A non-null query to execute.
   * @param upstream_span An optional tracer span.
   * @return A query execution with a deferred containing the results or an
   * exception.
   */
  public abstract QueryExecution<IteratorGroups> runTimeSeriesQuery(
      final QueryContext context,
      final TimeSeriesQuery query,
      final Span upstream_span);
  
}
