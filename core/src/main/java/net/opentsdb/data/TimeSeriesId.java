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

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;

import net.opentsdb.core.Const;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes;
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
 * 
 * @since 3.0
 */
public abstract class TimeSeriesId implements Comparable<TimeSeriesId> {
  
  /** An optional alias. */
  protected byte[] alias;
  
  /** An optional list of namespaces for the ID. */
  protected List<byte[]> namespaces;
  
  /** An optional list of metrics for the ID. */
  protected List<byte[]> metrics;
  
  /** A map of tag key/value pairs for the ID. */
  protected ByteMap<byte[]> tags = new ByteMap<byte[]>();
  
  /** An optional list of aggregated tags for the ID. */
  protected List<byte[]> aggregated_tags;
  
  /** An optional list of disjoint tags for the ID. */
  protected List<byte[]> disjoint_tags;
  
  /** A list of unique IDs rolled up into the ID. */
  protected ByteSet unique_ids = new ByteSet();
  
  /**
   * @return True if the fields are encoded using a format specified by the 
   * storage engine.
   */
  public boolean encoded() {
    // TODO
    return false;
  }
  
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
  public byte[] alias() {
    return alias;
  }
  
  /**
   * A list of namespace identifiers included in the sources for this time series.
   * May be empty if namespaces are not in use for the platform.
   *  
   * @return A non-null list if zero or more namespace identifiers.
   */
  public List<byte[]> namespaces() {
    return namespaces == null ? Collections.<byte[]>emptyList() : namespaces;
  }
  
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
  public List<byte[]> metrics() {
    return metrics == null ? Collections.<byte[]>emptyList() : metrics;
  }
  
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
  public ByteMap<byte[]> tags() {
    // TODO - need an unmodifiable ByteMap.
    return tags;
  }

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
  public List<byte[]> aggregatedTags() {
    return aggregated_tags == null ? 
        Collections.<byte[]>emptyList() : aggregated_tags;
  }
  
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
  public List<byte[]> disjointTags() {
    return disjoint_tags == null ? 
        Collections.<byte[]>emptyList() : disjoint_tags;
  }
  
  /**
   * A flattened list of unique identifiers for the time series that can be used
   * to determine the count of real series underlying the data.
   * @return A non-null set of unique identifiers.
   */
  public ByteSet uniqueIds() {
    // TODO - need an unmodifiable ByteSet
    return unique_ids;
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof TimeSeriesId))
      return false;
    
    final TimeSeriesId id = (TimeSeriesId) o;
    
    // long slog through byte arrays.... :(
    if (Bytes.memcmpMaybeNull(alias(), id.alias()) != 0) {
      return false;
    }
    if (!Bytes.equals(namespaces(), id.namespaces())) {
      return false;
    }
    if (!Bytes.equals(metrics(), id.metrics())) {
      return false;
    }
    if (!Bytes.equals(tags(), id.tags())) {
      return false;
    }
    if (!Bytes.equals(aggregatedTags(), id.aggregatedTags())) {
      return false;
    }
    if (!Bytes.equals(disjointTags(), id.disjointTags())) {
      return false;
    }
    if (unique_ids != null && id.uniqueIds() == null) {
      return false;
    }
    if (unique_ids == null && id.uniqueIds() != null) {
      return false;
    }
    if (unique_ids != null && id.uniqueIds() != null) {
      return unique_ids.equals(id.uniqueIds());
    }
    return true;
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    return Const.HASH_FUNCTION().newHasher()
        .putBytes(alias)
        .putObject(namespaces, Bytes.BYTE_LIST_FUNNEL)
        .putObject(metrics, Bytes.BYTE_LIST_FUNNEL)
        .putObject(tags, Bytes.BYTE_MAP_FUNNEL)
        .putObject(aggregated_tags, Bytes.BYTE_LIST_FUNNEL)
        .putObject(disjoint_tags, Bytes.BYTE_LIST_FUNNEL)
        .putObject(unique_ids, ByteSet.BYTE_SET_FUNNEL)
        .hash();
  }
  
  @Override
  public int compareTo(final TimeSeriesId o) {
    return ComparisonChain.start()
        .compare(alias, o.alias(), Bytes.MEMCMP)
        .compare(namespaces, o.namespaces(), 
            Ordering.from(Bytes.MEMCMP).lexicographical().nullsFirst())
        .compare(metrics, o.metrics(), 
            Ordering.from(Bytes.MEMCMP).lexicographical().nullsFirst())
        .compare(tags, o.tags(), Bytes.BYTE_MAP_CMP)
        .compare(aggregated_tags, o.aggregatedTags(), 
            Ordering.from(Bytes.MEMCMP).lexicographical().nullsFirst())
        .compare(disjoint_tags, o.disjointTags(), 
            Ordering.from(Bytes.MEMCMP).lexicographical().nullsFirst())
        .compare(unique_ids, o.uniqueIds(), ByteSet.BYTE_SET_CMP)
        .result();
  }
  
  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder()
        .append("alias=")
        .append(alias != null ? new String(alias, Const.UTF8_CHARSET) : "null")
        .append(", namespaces=")
        .append(Bytes.toString(namespaces, Const.UTF8_CHARSET))
        .append(", metrics=")
        .append(Bytes.toString(metrics, Const.UTF8_CHARSET))
        .append(", tags=")
        .append(Bytes.toString(tags, Const.UTF8_CHARSET, Const.UTF8_CHARSET))
        .append(", aggregated_tags=")
        .append(Bytes.toString(aggregated_tags, Const.UTF8_CHARSET))
        .append(", disjoint_tags=")
        .append(Bytes.toString(disjoint_tags, Const.UTF8_CHARSET))
        .append(", uniqueIds=")
        .append(unique_ids);
    return buf.toString();
  }
  
}
