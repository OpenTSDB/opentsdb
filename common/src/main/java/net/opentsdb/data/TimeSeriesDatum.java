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
  
}
