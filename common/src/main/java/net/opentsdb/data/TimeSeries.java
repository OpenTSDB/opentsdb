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
package net.opentsdb.data;

import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;

import com.google.common.reflect.TypeToken;

/**
 * A collection of data for a specific time series, i.e. {@link TimeSeriesId}.
 * The collection may contain multiple data types, e.g. a series of numeric
 * values and a series of strings or annotations.
 * <p>
 * To pull data out of this collection, call {@link #iterator(TypeToken)} or
 * {@link #iterators()}. These will return Java iterators that return
 * {@link TimeSeriesValue} objects on each call to {@link Iterator#next()}. Each
 * call to one of these iterator methods will return a new view of the data 
 * starting at the beginning of the set.
 * <p>
 * All data types will contain data within the same time range for this time
 * series.
 * 
 * @since 3.0
 */
public interface TimeSeries {

  /**
   * A reference to the time series ID associated with this value.
   * @return A non-null {@link TimeSeriesId}.
   */
  public TimeSeriesId id();
  
  /**
   * Returns a new iterator for the type of time series data denoted by {code type}
   * as an {@link Optional}.
   * @param type A non-null {@link TimeSeriesDataType} type of data.
   * @return A new iterator for the type of data or absent if no such type is
   * present for the time series.
   */
  public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> 
    iterator(final TypeToken<?> type);
  
  /**
   * Returns a collection of iterators for all types of data belonging to the 
   * time series. The collection returned may not be null but may be empty.
   * @return A non-null collection of zero or more iterators.
   */
  public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators();
  
  /**
   * Returns a collection of data types contained in the time series. The 
   * collection returned may not be null but may be empty.
   * @return A non-null collection of zero or more data types.
   */
  public Collection<TypeToken<?>> types();
  
  /**
   * Method called when the consuming sink is finished processing this time series
   * and allows downstream methods to release resources.
   * NOTE: Calling close will invalidate all iterators returned by this series.
   */
  public void close();
}
