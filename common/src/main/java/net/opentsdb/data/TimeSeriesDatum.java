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

/**
 * A single value consisting of an ID and a typed value.
 * 
 * @since 3.0
 */
public interface TimeSeriesDatum {

  /**
   * The non-null ID for the value.
   * @return A non-null ID.
   */
  public TimeSeriesDatumId id();
  
  /**
   * A non-null value incorporating the timestamp and a value. Note that
   * the value of this value may be null and a store has to decide how
   * to handle that.
   * @return The non-null value.
   */
  public TimeSeriesValue<? extends TimeSeriesDataType> value();
  
  /**
   * A wrapper around an existing ID and value. This is useful for 
   * testing and one-off uses but for high performance writes, the 
   * interface should be implemented on existing data.
   * 
   * @param id The non-null ID to wrap.
   * @param value The non-null value to wrap.
   * @return A wrapped datum object.
   * @throws IllegalArgumentException if the ID or value were null.
   */
  public static TimeSeriesDatum wrap(
      final TimeSeriesDatumId id, 
      final TimeSeriesValue<? extends TimeSeriesDataType> value) {
    if (id == null) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    if (value == null) {
      throw new IllegalArgumentException("Value cannot be null.");
    }
    
    return new TimeSeriesDatum() {

      @Override
      public TimeSeriesDatumId id() {
        return id;
      }

      @Override
      public TimeSeriesValue<? extends TimeSeriesDataType> value() {
        return value;
      }
      
    };
  }
}
