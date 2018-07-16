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

import java.util.List;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.ReadableTimeSeriesDataStore;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes.ByteMap;

/**
 * A simple wrapper for single-sided joins that wraps the source 
 * ID with the proper alias for use as the alias and metric.
 * 
 * @since 3.0
 */
public class ByteIdOverride implements TimeSeriesByteId {
  
  /** The source ID. */
  private final TimeSeriesByteId id;
  
  /** The new alias. */
  private final String alias;
  
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
  public int compareTo(TimeSeriesByteId o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public ReadableTimeSeriesDataStore dataStore() {
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
