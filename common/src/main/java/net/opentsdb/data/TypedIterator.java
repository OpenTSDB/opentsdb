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
import java.util.function.Consumer;

/**
 * An iterator for {@link TimeSeriesValue}s that lets us determine the
 * type of the underlying data without having to resolve the types.
 * 
 * @param <T> The type of data iterated over.
 */
public class TypedIterator<T> implements Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> {

  /** The base iterator. */
  protected Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator;
  
  /** The data type. */
  protected TypeToken<?> type;

  /**
   * Default ctor.
   * @param iterator The non-null source iterator.
   * @param type The non-null data type.
   */
  public TypedIterator(
    final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator,
    final TypeToken<? extends TimeSeriesDataType> type) {
    this.iterator = iterator;
    this.type = type;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    return iterator.next();
  }

  @Override
  public void remove() {
    iterator.remove();
  }

  @Override
  public void forEachRemaining(
    Consumer<? super TimeSeriesValue<? extends TimeSeriesDataType>> action) {
    iterator.forEachRemaining(action);
  }

  public TypeToken<?> getType() {
    return type;
  }

  public Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> getIterator() {
    return iterator;
  }
}
