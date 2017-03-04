// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.data;

import java.util.List;

import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes.ByteMap;

/**
 * An identifier for a time series. The identity can be as simple as the alias
 * or a combination of namespace, metrics, tags, etc.
 * <p>
 * Most of the fields return a list or map. These represent the aggregate of all
 * time series included in the sourcing of this series. This is applicable to
 * synthetic series when multiple time series are aggregated into one series.
 * <p>
 * In general, if a raw time series is the source then only one metric and
 * an optional set of tags should be present.
 * <p>
 * Most fields also include byte representations in case the underlying storage
 * system encodes identifiers using something other than a raw string 
 * representation. The encoded values can be passed in the identifiers to save
 * space when series are passed across the network or cached. In such cases the
 * encoded identifiers can be decoded prior to the final sink.
 * <p>
 * <b>Warning:</b> Underlying bytes are mutable so please make a copy before
 * making any modifications.
 * @since 3.0
 */
public interface TimeSeriesId {

  /**
   * @return True if the fields are encoded using a format specified by the 
   * storage engine.
   */
  public boolean encoded();
  
  /**
   * A simple string for identifying the time series. The alias may be null or
   * empty. If a value is present, the alias must be unique within a set of
   * time series.
   * <p>
   * Invariant: If all other fields for the identity are empty, then the alias
   * must be non-null and non-empty.
   * 
   * @return A string or null if not used.
   */
  public byte[] alias();
  
  /**
   * A list of namespace identifiers included in the sources for this time series.
   * May be empty if namespaces are not in use for the platform.
   *  
   * @return A non-null list if zero or more namespace identifiers.
   */
  public List<byte[]> namespaces();
  
  /**
   * A list of metric names included in the sources for this time series.
   * If this list is empty, check {@link #metrics()} for data.
   * <p>
   * Invariant: If this list is empty (meaning the series or set of series is 
   * not associated with a specific metric) then the {@link #alias()} must be 
   * non-null and unique.
   *  
   * @return A non-null list of zero or more metric names.
   */
  public List<byte[]> metrics();
  
  /**
   * A list of tag name and value pairs included in the sources for this time 
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
}
