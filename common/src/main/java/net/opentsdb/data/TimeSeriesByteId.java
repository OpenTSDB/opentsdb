//This file is part of OpenTSDB.
//Copyright (C) 2018  The OpenTSDB Authors.
//
//This program is free software: you can redistribute it and/or modify it
//under the terms of the GNU Lesser General Public License as published by
//the Free Software Foundation, either version 2.1 of the License, or (at your
//option) any later version.  This program is distributed in the hope that it
//will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
//of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
//General Public License for more details.  You should have received a copy
//of the GNU Lesser General Public License along with this program.  If not,
//see <http://www.gnu.org/licenses/>.
package net.opentsdb.data;

import java.util.List;

import com.stumbleupon.async.Deferred;

import net.opentsdb.stats.Span;
import net.opentsdb.storage.ReadableTimeSeriesDataStore;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes.ByteMap;

/**
 * An identifier for a time series. The identity can be as simple as the alias
 * or a combination of namespace, metrics, tags, etc.
 * <p>
 * TODO - further docs
 * 
 * @since 3.0
 */
public interface TimeSeriesByteId extends TimeSeriesId, 
                                          Comparable<TimeSeriesByteId> {

  /**
   * The data store schema associated with this time series byte id.
   * 
   * @return A non-null data store.
   */
  public ReadableTimeSeriesDataStore dataStore();
  
  /**
   * A simple id for identifying the time series. The alias may be null or
   * empty. If a value is present, the alias must be unique within a set of
   * time series.
   * 
   * @return A non-empty byte array or null if not used.
   */
  public byte[] alias();
  
  /**
   * An optional tenant or group name for the time series.
   * May be null or empty if namespaces are not in use for the platform.
   *  
   * @return A non-empty byte array if set, null if not used.
   */
  public byte[] namespace();
  
  /**
   * The metric component of the time series ID. This is a required value and
   * may not be null or empty.
   *  
   * @return A non-null and non-empty byte array.
   */
  public byte[] metric();
  
  /**
   * A map of tag name and value pairs included in the sources for this time 
   * series. If the underlying storage system does not support tags or none of
   * the source time series had tags pairs in common, this list may be empty.
   * The map key represents a tag name (tagk) and the map value represents a 
   * tag value (tagv).
   * <p>
   * Invariant: Each tag pair must appear in every source time series.
   * 
   * @return A non-null map of zero or more tag pairs.
   */
  public ByteMap<byte[]> tags();
  
  /**
   * A list of tag names (tagk) that were represented in every source series
   * but had one or more differing tag values (tagv). This list may be empty if
   * - The underlying store does not support tags.
   * - All of the source time series shared the same tag pairs.
   * - All of the source time series had disjoint tag sets.
   * <p>
   * Invariant: Each tag name must be present in every time series if it appears
   * in this list.
   * 
   * @return A non-null list of zero or more tag names.
   */
  public List<byte[]> aggregatedTags();
  
  /**
   * A list of tag names (tagk) that were represented in one or more source time
   * series but not every source time series. E.g. series A has tag name Z but
   * series B does not.
   * This list may be empty if:
   * - The underlying store does not support tags.
   * - All of the source time series shared the same tag pairs.
   * - All of the source time series shared the same tag names with different
   * values.
   * <p>
   * Invariant: Each tag name must be present in at least one but not all 
   * source series if it appears in this list.
   * 
   * @return A non-null list of zero or more tag names.
   */
  public List<byte[]> disjointTags();
  
  /**
   * A flattened list of unique identifiers for the time series that can be used
   * to determine the count of real series underlying the data.
   * @return A non-null set of unique identifiers.
   */
  public ByteSet uniqueIds();
  
  /** @return A flag to tell the {@link #decode(boolean, Span)} method 
   * whether or not the metric is encoded. This is due to metrics possibly
   * being overwritten during joins in expressions. */
  public boolean skipMetric();
  
  /**
   * Returns the string version of the time series ID based on the schema
   * used to encode the byte IDs.
   * 
   * @param cache Whether or not to cache the result.
   * @param span An optional tracing span.
   * @return A non-null deferred resolving to either an ID or an exception. 
   */
  public Deferred<TimeSeriesStringId> decode(final boolean cache, final Span span);
}
