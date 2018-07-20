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

import com.google.common.reflect.TypeToken;
import com.google.protobuf.Any;

import net.opentsdb.data.TimeSeriesDataType;

/**
 * An interface for serial/deserializers with Protobuf messages. Each
 * implementation must handle a type.
 * <p>
 * Note that the serdes modules are called from multiple threads and they
 * most not keep track of any state.
 * 
 * @since 3.0
 */
public interface PBufDatumSerdes {

  /** @return The type of data handled by this class. */
  public TypeToken<? extends TimeSeriesDataType> type();
  
  /**
   * Converts the TSDB datum value into a Protobuf value.
   * @param value A non-null value.
   * @return An Any to store in the main PBuf builder. 
   */
  public Any serialize(final TimeSeriesDataType value);
  
  /**
   * Converts the PBuf encoded value back to a TSDB data type.
   * @param data The non-null data to parse.
   * @return The non-null TSDB value.
   */
  public TimeSeriesDataType deserialize(final Any data);
}
