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
import java.util.Map;
import java.util.Set;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;

/**
 * A simple wrapper for single-sided joins that wraps the source 
 * ID with the proper alias.
 * 
 * @since 3.0
 */
public class StringIdOverride implements TimeSeriesStringId {
  
  /** The source ID. */
  private final TimeSeriesStringId id;
  
  /** The new alias. */
  private final String alias;
  
  /**
   * Default package private ctor.
   * @param id A non-null ID to use as the source.
   * @param alias A non-null alias.
   */
  StringIdOverride(final TimeSeriesStringId id, final String alias) {
    this.id = id;
    this.alias = alias;
  }
  
  @Override
  public boolean encoded() {
    return false;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> type() {
    return id.type();
  }
  
  @Override
  public int compareTo(TimeSeriesStringId o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String alias() {
    return alias;
  }

  @Override
  public String namespace() {
    return id.namespace();
  }

  @Override
  public String metric() {
    return id.metric();
  }

  @Override
  public Map<String, String> tags() {
    return id.tags();
  }

  @Override
  public List<String> aggregatedTags() {
    return id.aggregatedTags();
  }

  @Override
  public List<String> disjointTags() {
    return id.disjointTags();
  }

  @Override
  public Set<String> uniqueIds() {
    return id.uniqueIds();
  }
  
}