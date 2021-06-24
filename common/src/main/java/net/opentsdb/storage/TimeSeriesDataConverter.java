// This file is part of OpenTSDB.
// Copyright (C) 2018-2021  The OpenTSDB Authors.
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
import java.io.OutputStream;

import net.opentsdb.data.LowLevelTimeSeriesData;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;

/**
 * An interface used to implement a converter that reads time series data
 * from an input source and generates an iterable instance of datum to
 * write to storage or some other data consumer.
 * 
 * @since 3.0
 */
public interface TimeSeriesDataConverter {

  /**
   * Convert the string to a single value.
   * @param source A non-null and non-empty string.
   * @return A non-null datum.
   */
  public TimeSeriesDatum convert(final String source);
  
  /**
   * Convert the byte array to a single value.
   * @param source A non-null and non-empty byte array.
   * @return A non-null datum.
   */
  public TimeSeriesDatum convert(final byte[] source);

  /**
   * Convert the byte array to a single value.
   * @param source A non-null and non-empty byte array.
   * @param offset The starting offset into the array to read from.
   * @param length The length of data to read from the source.
   * @return A non-null datum.
   */
  public TimeSeriesDatum convert(final byte[] source,
                                 final int offset,
                                 final int length);
  
  /**
   * Convert the input stream to a single value.
   * @param source A non-null stream ready for reading. Note that on
   * return, the source may need to be reset if reading has to happen a
   * second time.
   * @return A non-null datum.
   */
  public TimeSeriesDatum convert(final InputStream source);

  /**
   * Returns the length of the datum if it was to be serialized.
   * @param datumn The non-null datum to serialize.
   * @return The length of the data in bytes.
   */
  public int serializationSize(final TimeSeriesDatum datumn);

  /**
   * Writes the given datum to the byte array starting at the given offset.
   * Call {@link #serialize(TimeSeriesDatum, byte[], int)} first to size the
   * buffer properly.
   * @param datum The non-null datum to serialize.
   * @param buffer The non-null buffer to write to.
   * @param offset The offset into the buffer at which to start writing.
   */
  public void serialize(final TimeSeriesDatum datum,
                        final byte[] buffer,
                        final int offset);

  /**
   * Writes the given datum to the input stream. Call
   * {@link #serialize(TimeSeriesDatum, byte[], int)} if the stream needs to be
   * sized prior to writing.
   * @param datum The non-null datum to serialize.
   * @param stream The non-null and open stream to write to.
   */
  public void serialize(final TimeSeriesDatum datum,
                        final OutputStream stream);
  
  /**
   * Convert the string to an instance of time series data that share a common
   * tag set and timestamp.
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
   * Convert the byte array to an iterable set of datum sharing the same
   * timestamp and tags.
   * @param source A non-null and non-empty byte array.
   * @param offset The offset into the array at which to start writing.
   * @param length The amount of data in bytes to read from the source.
   * @return A non-null iterable instance.
   */
  public TimeSeriesSharedTagsAndTimeData convertShared(final byte[] source,
                                                       final int offset,
                                                       final int length);

  /**
   * Convert the input stream to an iterable set of datum sharing the same 
   * timestamp and tags.
   * @param source A non-null stream ready for reading. Note that on
   * return, the source may need to be reset if reading has to happen a
   * second time.
   * @return A non-null iterable instance.
   */
  public TimeSeriesSharedTagsAndTimeData convertShared(final InputStream source);

  /**
   * Returns the length of the data if it was to be serialized.
   * @param data The non-null data to serialize.
   * @return The length of the data in bytes.
   */
  public int serializationSize(final TimeSeriesSharedTagsAndTimeData data);

  /**
   * Writes the given datu to the byte array starting at the given offset.
   * Call {@link #serialize(TimeSeriesSharedTagsAndTimeData, byte[], int)}
   * first to size the buffer properly.
   * @param data The non-null data to serialize.
   * @param buffer The non-null buffer to write to.
   * @param offset The offset into the buffer at which to start writing.
   */
  public void serialize(final TimeSeriesSharedTagsAndTimeData data,
                        final byte[] buffer,
                        final int offset);

  /**
   * Writes the given datu to the byte array starting at the given offset.
   * Call {@link #serialize(TimeSeriesSharedTagsAndTimeData, byte[], int)}
   * if the stream needs to be sized.
   * @param data The non-null data to serialize.
   * @param stream The non-null and open stream to write to.
   */
  public void serialize(final TimeSeriesSharedTagsAndTimeData data,
                        final OutputStream stream);

  /**
   * Convert the byte array to an iterable low level instance.
   * @param source A non-null and non-empty byte array.
   * @return A non-null iterable instance.
   */
  public LowLevelTimeSeriesData convertLowLevelData(final byte[] source);

  /**
   * Convert the byte array to an iterable low level instance.
   * @param source A non-null and non-empty byte array.
   * @param offset An offset into the source array at which to start reading.
   * @param length The amount of data to read from the source.
   * @return A non-null iterable instance.
   */
  public LowLevelTimeSeriesData convertLowLevelData(final byte[] source,
                                                    final int offset,
                                                    final int length);

  /**
   * Convert the input stream to an iterable low level interface.
   * @param source A non-null stream ready for reading. Note that on
   * return, the source may need to be reset if reading has to happen a
   * second time.
   * @return A non-null iterable instance.
   */
  public LowLevelTimeSeriesData convertLowLevelData(final InputStream source);

  /**
   * Returns the length of the data if it was to be serialized.
   * @param data The non-null data to serialize.
   * @return The length of the data in bytes.
   */
  public int serializationSize(final LowLevelTimeSeriesData data);

  /**
   * Writes the given datu to the byte array starting at the given offset.
   * Call {@link #serialize(LowLevelTimeSeriesData, byte[], int)}
   * first to size the buffer properly.
   * @param data The non-null data to serialize.
   * @param buffer The non-null buffer to write to.
   * @param offset The offset into the buffer at which to start writing.
   */
  public void serialize(final LowLevelTimeSeriesData data,
                        final byte[] buffer,
                        final int offset);

  /**
   * Writes the given datu to the byte array starting at the given offset.
   * Call {@link #serialize(LowLevelTimeSeriesData, byte[], int)}
   * first if the stream needs sizing.
   * @param data The non-null data to serialize.
   * @param stream The non-null and open stream to write to.
   */
  public void serialize(final LowLevelTimeSeriesData data,
                        final OutputStream stream);
}
