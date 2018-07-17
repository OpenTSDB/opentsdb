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

import java.util.Collection;
import java.util.Iterator;

/**
 * An interface used when writing values that wraps multiple data in a
 * single batch. 
 * 
 * @since 3.0
 */
public interface TimeSeriesDatumIterable extends Iterable<TimeSeriesDatum> {

  /**
   * A wrapper around a collection to create an iterable.
   * @param datum The non-null datum to read from.
   * @return The iterable collection.
   * @throws IllegalArgumentException if the datum was null.
   */
  public static TimeSeriesDatumIterable fromCollection(
      final Collection<TimeSeriesDatum> datum) {
    if (datum == null) {
      throw new IllegalArgumentException("Datum collection cannot be null.");
    }
    
    return new TimeSeriesDatumIterable() {
      @Override
      public Iterator<TimeSeriesDatum> iterator() {
        return datum.iterator();
      }
    };
  }
}
