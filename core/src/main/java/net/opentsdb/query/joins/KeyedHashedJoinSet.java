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

import com.google.common.collect.Lists;

import gnu.trove.map.hash.TLongObjectHashMap;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.utils.Bytes;

/**
 * A default implementation for the {@link BaseHashedJoinSet} that simply
 * routes a time series to the left or right map based on a string key.
 * Handles appending series to a list in the maps.
 * <p>
 * <bNote:</b> The implementation doesn't check to see if the same 
 * time series is in the left or right map.
 * 
 * @since 3.0
 */
public class KeyedHashedJoinSet extends BaseHashedJoinSet {
  
  /** The identifier that maps to the left map. */
  protected final byte[] left_key;
  
  /** The identifier that maps to the right map. */
  protected final byte[] right_key;
  
  /**
   * Default ctor.
   * @param type A non-null join type.
   * @param left_key A non-null and non-empty string mapping to the left
   * map.
   * @param right_key A non-null and non-empty string mapping to the right
   * map.
   * @throws IllegalArgumentException if any of the args were null or empty.
   */
  protected KeyedHashedJoinSet(final JoinType type, 
                               final byte[] left_key, 
                               final byte[] right_key) {
    super(type);
    if (type == null) {
      throw new IllegalArgumentException("Join type cannot be null.");
    }
    if (left_key == null || left_key.length < 1) {
      throw new IllegalArgumentException("Left key cannot be null.");
    }
    if (right_key == null || right_key.length < 1) {
      throw new IllegalArgumentException("Right key cannot be null.");
    }
    this.left_key = left_key;
    this.right_key = right_key;
  }
  
  /**
   * Package private method to add a value to the proper map.
   * @param key A non-null and non-empty key mapping to the left or right
   * map.
   * @param hash The hash for this time series.
   * @param ts A non-null time series.
   */
  void add(final byte[] key, final long hash, final TimeSeries ts) {
    if (ts == null) {
      throw new IllegalArgumentException("Time series can't be null.");
    }
    if (Bytes.memcmp(key, left_key) == 0) {
      if (left_map == null) {
        left_map = new TLongObjectHashMap<List<TimeSeries>>();
      }
      List<TimeSeries> series = left_map.get(hash);
      if (series == null) {
        series = Lists.newArrayList();
        left_map.put(hash, series);
      }
      series.add(ts);
    } else if (Bytes.memcmp(key, right_key) == 0) {
      if (right_map == null) {
        right_map = new TLongObjectHashMap<List<TimeSeries>>();
      }
      List<TimeSeries> series = right_map.get(hash);
      if (series == null) {
        series = Lists.newArrayList();
        right_map.put(hash, series);
      }
      series.add(ts);
    } else {
      throw new IllegalArgumentException("Key didn't match the left "
          + "or right.");
    }
  }
  
}