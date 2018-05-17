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
package net.opentsdb.query.serdes;

import java.util.Iterator;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.pbuf.TimeSeriesDataPB;
import net.opentsdb.data.pbuf.TimeSeriesPB;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.serdes.SerdesOptions;

/**
 * An interface for serial/deserializers with Protobuf messages. Each
 * implementation must handle a type.
 * <p>
 * Note that the serdes modules are called from multiple threads and they
 * most not keep track of any state.
 * 
 * @since 3.0
 */
public interface PBufIteratorSerdes {

  /** @return The type of data handled by this class. */
  public TypeToken<? extends TimeSeriesDataType> type();
  
  /**
   * The method that serializes the data. A non-null time series builder
   * is given and the serialization method must create zero or more 
   * segments to be stored in a {@link TimeSeriesData} proto that is
   * added to the ts_builder. If there isn't any data for the iterator,
   * just populate the builder with an empty data object for the given
   * type.
   * 
   * @param ts_builder A non-null time series Protobuf builder to add to.
   * @param context A query context to pull info from.
   * @param options A non-null serdes options to pull timestamps and other
   * configs from.
   * @param result The original query result.
   * @param iterator A non-null iterator.
   * 
   * @throws SerdesException If something goes wrong during serialization.
   */
  public void serialize(final TimeSeriesPB.TimeSeries.Builder ts_builder, 
                        final QueryContext context, 
                        final SerdesOptions options, 
                        final QueryResult result,
                        final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator);
  
  /**
   * Returns an iterator over the data serialized in the TimeSeriesData
   * protobuf object.
   * 
   * @param series A non-null result to parse.
   * @return A non-null iterator. If the data set doesn't have any data
   * just return an iterator with it's {@link Iterator#hasNext()} returning
   * false.
   * 
   * @throws SerdesException If something goes wrong during deserialization.
   */
  public Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> 
    deserialize(final TimeSeriesDataPB.TimeSeriesData series);
}
