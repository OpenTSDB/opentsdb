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
package net.opentsdb.query.joins;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.openhft.hashing.LongHashFunction;
import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Bytes.ByteMap;

/**
 * A simple wrapper for single-sided joins that wraps the source 
 * ID with the proper alias for use as the alias <b>and<b> metric.
 * 
 * @since 3.0
 */
public class ByteIdOverride implements TimeSeriesByteId {
  
  /** The source ID. */
  private final TimeSeriesByteId id;
  
  /** The new alias. */
  private final String alias;
  
  /** A cached hash code ID. May have some */
  protected volatile long cached_hash; 
  
  /**
   * Default package private ctor.
   * @param id A non-null ID to use as the source.
   * @param alias A non-null alias.
   */
  ByteIdOverride(final TimeSeriesByteId id, final String alias) {
    this.id = id;
    this.alias = alias;
  }
  
  @Override
  public boolean encoded() {
    return id.encoded();
  }

  @Override
  public TypeToken<? extends TimeSeriesId> type() {
    return id.type();
  }

  @Override
  public int compareTo(final TimeSeriesByteId o) {
    return ComparisonChain.start()
        .compare(alias.getBytes(Const.UTF8_CHARSET), o.alias(), 
            Bytes.MEMCMPNULLS)
        .compare(id.namespace(), o.namespace(), Bytes.MEMCMPNULLS)
        .compare(alias.getBytes(Const.UTF8_CHARSET), o.metric(), 
            Bytes.MEMCMPNULLS)
        .compare(id.tags(), o.tags(), Bytes.BYTE_MAP_CMP)
        .compare(id.aggregatedTags(), o.aggregatedTags(), Bytes.BYTE_LIST_CMP)
        .compare(id.disjointTags(), o.disjointTags(), Bytes.BYTE_LIST_CMP)
        .compare(id.uniqueIds(), o.uniqueIds(), ByteSet.BYTE_SET_CMP)
        .result();
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof TimeSeriesByteId))
      return false;
    
    final TimeSeriesByteId id = (TimeSeriesByteId) o;
    
    if (Bytes.memcmpMaybeNull(alias.getBytes(Const.UTF8_CHARSET), 
        id.alias()) != 0) {
      return false;
    }
    if (Bytes.memcmpMaybeNull(namespace(), id.namespace()) != 0) {
      return false;
    }
    if (Bytes.memcmpMaybeNull(alias.getBytes(Const.UTF8_CHARSET), 
        id.metric()) != 0) {
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
    if (!uniqueIds().equals(id.uniqueIds())) {
      return false;
    }
    return true;
  }
  
  @Override
  public int hashCode() {
    if (cached_hash == 0) {
      cached_hash = buildHashCode();
    }
    return Long.hashCode(cached_hash);
  }
  
  @Override
  public long buildHashCode() {
    try {
      final ByteArrayOutputStream buf = new ByteArrayOutputStream();
      if (!Strings.isNullOrEmpty(alias)) {
        buf.write(alias.getBytes(Const.UTF8_CHARSET));
      }
      if (!Bytes.isNullOrEmpty(id.namespace())) {
        buf.write(id.namespace());
      }
      buf.write(alias.getBytes(Const.UTF8_CHARSET));
      if (id.tags() != null) {
        for (final Entry<byte[], byte[]> pair : id.tags().entrySet()) {
          buf.write(pair.getKey());
          buf.write(pair.getValue());
        }
      }
      if (id.aggregatedTags() != null) {
        for (final byte[] t : id.aggregatedTags()) {
          buf.write(t);
        }
      }
      if (id.disjointTags() != null) {
        for (final byte[] t : id.disjointTags()) {
          buf.write(t);
        }
      }
      if (id.uniqueIds() != null) {
        final List<byte[]> sorted = Lists.newArrayList(id.uniqueIds());
        Collections.sort(sorted, Bytes.MEMCMPNULLS);
        for (final byte[] id : sorted) {
          buf.write(id);
        }
      }
      return LongHashFunction.xx_r39().hashChars(buf.toString());
    } catch (IOException e) {
      throw new RuntimeException("Unexpected exception: " + e.getMessage(), e);
    }
  }

  @Override
  public TimeSeriesDataSourceFactory dataStore() {
    return id.dataStore();
  }

  @Override
  public byte[] alias() {
    return alias.getBytes(Const.UTF8_CHARSET);
  }

  @Override
  public byte[] namespace() {
    return id.namespace();
  }

  @Override
  public byte[] metric() {
    return alias.getBytes(Const.UTF8_CHARSET);
  }

  @Override
  public ByteMap<byte[]> tags() {
    return id.tags();
  }

  @Override
  public List<byte[]> aggregatedTags() {
    return id.aggregatedTags();
  }

  @Override
  public List<byte[]> disjointTags() {
    return id.disjointTags();
  }

  @Override
  public ByteSet uniqueIds() {
    return id.uniqueIds();
  }

  @Override
  public Deferred<TimeSeriesStringId> decode(boolean cache, Span span) {
    return id.dataStore().resolveByteId(this, span);
  }

  @Override
  public boolean skipMetric() {
    return true;
  }
  
}
