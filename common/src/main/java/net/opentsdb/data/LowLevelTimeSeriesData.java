// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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

import java.io.Closeable;

/**
 * This is an interface used by parsers to allow a downstream data consumer to
 * iterate over a set of one or more incoming time series data payloads. E.g.
 * a metric collection agent can send a text payload over a message bus or 
 * web service and the implementation of this interface will take a reference to
 * the payload, send it on to the next hop. When the destination begins
 * iterating by calling {@link #advance()}, the implementation should start 
 * parsing the payload and return each unique time series found. This will let
 * us avoid creating garbage in the form of strings or buffer slices when we
 * don't need to.
 * <p>
 * This is the base identifier and iterator for various time series data
 * including metrics, annotions, histograms, etc. Each type of data should extend
 * this interface.
 * <p>
 * To use this consumer, first call {@link #advance()}. If the response is true
 * then there is a valid time series to consume. The timestamp can be fetched 
 * from {@link #timestamp()} and if there are tags then the 
 * {@link #advanceTagPair()} method can be called to iterate over the pairs. The 
 * extending interface implementation will have the typed time series data ready 
 * to consume.
 * <p>
 * Note that implementers must reset the tag iteration for each call to 
 * {@link #advance()} in situations where the payload shares a common tag set
 * for multiple data.
 * <p>
 * Note also that an implementation may implement multiple data types while
 * iterating if the payload has different data types. In such a case each type
 * should set a {@code <type>HasData()} flag. 
 * @since 3.0
 */
public interface LowLevelTimeSeriesData extends Closeable {

  /** The format of data in the byte buffers for strings. */
  public static enum StringFormat {
    ASCII_STRING,
    UTF8_STRING,
    ENCODED
  }
  
  /** Called by the consumer to determine if there is any valid in this payload.
   * If true then data can be consumed by calling the other methods of this
   * interface. If false then either there is no more data to consume or there
   * was an error in parsing, see {@link #hasParsingError()}.
   * @return True if there was data to be read, false if there is no more data
   * to read or an error occured. */
  public boolean advance();
  
  /** @return Whether or not an error happened during parsing in which case there should
   * be information in {@link #parsingError()}. */
  public boolean hasParsingError();
  
  /** @return A descriptive message if parsing failed, null if not. */
  public String parsingError();
  
  /** @return The timestamp for the current entry. May not be null if 
   * {@link #advance()} returned true.. */
  public TimeStamp timestamp();
  
  /** ASSUMPTION: tags have been sorted and are consecutive stored in the buffer
   * in key<delim>value<delim>key<delim>value[...] format. The delimiter is 
   * given from {@link #tagDelimiter()} and is usually a null byte (0).
   * 
   * @return The non-null tag buffer if {@link #advance()} returned true. 
   * {@link #tagBufferStart()} and {@link #tagBufferLength()} can be used
   * to read the entire buffer or use {@link #tagKeyStart()} and 
   * {@link #tagValueStart()} when using the {@link #advanceTagPair()} method
   * of iterating over the tags. */
  public byte[] tagsBuffer();
  
  /** @return The starting offset of the tag set into the {@link #tagsBuffer()}. */
  public int tagBufferStart();
  
  /** @return The length of the tag set in bytes in the {@link #tagsBuffer()}. */
  public int tagBufferLength();
  
  /** @return The format of the strings in the {@link #tagsBuffer()}. */
  public StringFormat tagsFormat();

  /** @return The tag delimiter for use when reading the entire tag set in one go
   * instead of using {@link #advanceTagPair()}. */
  public byte tagDelimiter();
  
  /** @return The number of tag key/value pairs in the buffer. */
  public int tagSetCount();
  
  /** Used to iterate over the tag pairs. When used, use the key and value
   * starts and lengths to read from the {@link #tagsBuffer()}. The starts and
   * lengths do not include the delimiter character.
   * 
   * @return True if there was a pair read and the key and value starts and 
   * lengths are set, false if the end of the tag set has been reached.
   */
  public boolean advanceTagPair();

  /** @return The offset into the {@link #tagsBuffer()} for the current tag key
   * when using the {@link #advanceTagPair} method of iterating. */
  public int tagKeyStart();
  
  /** @return The length of the current tag key in bytes. */
  public int tagKeyLength();
  
  /** @return The offset into the {@link #tagsBuffer()} for the current tag value
   * when using the {@link #advanceTagPair} method of iterating. */
  public int tagValueStart();
  
  /** @return the length of the current tag value in bytes. */
  public int tagValueLength();
  
  /**
   * A time series data interface that computes hashes on the various time series
   * components during parsing to avoid having to compute hashes after consumption
   * when it may be less efficient.
   * <p>
   * For now we're using a signed long since we're in Java. Also note that the 
   * hash computation must be identical across all components for all time series.
   * 
   * @since 3.0
   */
  public interface HashedLowLevelTimeSeriesData extends LowLevelTimeSeriesData {
    /** @return The hash of the full time series. */
    public long timeSeriesHash();

    /** @return The hash of the tag set (remember it's sorted already). */
    public long tagsSetHash();

    /** @return The hash of a tag key/value pair when using the 
     * {@link #advanceTagPair()} iterative method. */
    public long tagPairHash();
    
    /** @return The hash of the current tag key when using the 
     * {@link #advanceTagPair()} method. */
    public long tagKeyHash();
    
    /** @return The hash of the current tag value when using the 
     * {@link #advanceTagPair()} method. */
    public long tagValueHash();
  }
  
  /**
   * A time series data interface that also includes a namespace or tenant ID.
   * 
   * @since 3.0
   */
  public interface NamespacedLowLevelTimeSeriesData extends LowLevelTimeSeriesData {
    
    /** @return The reference to a buffer containing the namespace. */
    public byte[] namespaceBuffer();
    
    /** @return The offset of the namespace in the buffer. */
    public int namespaceStart();
    
    /** @return The length of the namepsace string in bytes. */
    public int namespaceLength();
    
    /** @return The format of the namespace string. */
    public StringFormat namespaceFormat();
  }
  
  /**
   * A namespaced data set with the namespace hashed.
   * 
   * @since 3.0
   */
  public interface HashedNamespacedLowLevelTimeSeriesData extends 
      NamespacedLowLevelTimeSeriesData,
      HashedLowLevelTimeSeriesData {
    
    /** @return The hash of the namespace. */
    public long namespaceHash();
    
  }
}