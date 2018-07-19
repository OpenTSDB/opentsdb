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
package net.opentsdb.storage;

import java.io.InputStream;

import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;

/**
 * An interface used to implement a converter that reads time series data
 * from an input source and generates an iterable instance of datum to
 * write to storage.
 * 
 * @since 3.0
 */
public interface WritingTimeSeriesDataConverter {

  /**
   * Convert the string to a single value.
   * @param source A non-null and non-empty string.
   * @return A non-null datum.
   */
  public TimeSeriesDatum convert(final String source);
  
  /**
   * Convert the byte array to an iterable set of datum sharing the same 
   * timestamp and tags.
   * @param source A non-null and non-empty byte array.
   * @return A non-null datum.
   */
  
  public TimeSeriesDatum convert(final byte[] source);
  
  /**
   * Convert the input stream to a single value.
   * @param source A non-null stream ready for reading. Note that on
   * return, the source may need to be reset if reading has to happen a
   * second time.
   * @return A non-null datum.
   */
  public TimeSeriesDatum convert(final InputStream source);
  
  /**
   * Convert the string to a single value.
   * @param source A non-null and non-empty string.
   * @return A non-null iterable instance.
   */
  public TimeSeriesSharedTagsAndTimeData convertShared(final String source);
  
  /**
   * Convert the byte array to an iterable set of datum sharing the same 
   * timestamp and tags.
   * @param source A non-null and non-empty byte array.
   * @return A non-null iterable instance.
   */
  
  public TimeSeriesSharedTagsAndTimeData convertShared(final byte[] source);
  
  /**
   * Convert the input stream to an iterable set of datum sharing the same 
   * timestamp and tags.
   * @param source A non-null stream ready for reading. Note that on
   * return, the source may need to be reset if reading has to happen a
   * second time.
   * @return A non-null iterable instance.
   */
  public TimeSeriesSharedTagsAndTimeData convertShared(final InputStream source);
}
