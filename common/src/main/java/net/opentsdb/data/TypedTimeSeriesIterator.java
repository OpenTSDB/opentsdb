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
package net.opentsdb.data;

import com.google.common.reflect.TypeToken;
import java.util.Iterator;

/**
 * An iterator for {@link TimeSeriesValue}s that lets us determine the
 * type of the underlying data without having to resolve the types.
 * 
 * @since 3.0
 */
public interface TypedTimeSeriesIterator extends 
    Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> {

  /**
   * @return The non-null type of data returned in the iterator.
   */
  public TypeToken<? extends TimeSeriesDataType> getType();

}
