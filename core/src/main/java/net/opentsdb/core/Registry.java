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
package net.opentsdb.core;

import java.util.Collections;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.DataMerger;
import net.opentsdb.data.DataShardMerger;
import net.opentsdb.data.DataShardsGroup;
import net.opentsdb.data.types.numeric.NumericMergeLargest;

/**
 * TODO - stub
 *
 * @since 3.0
 */
public class Registry {
  
  private final Map<TypeToken<?>, DataMerger<?>> data_mergers;
  
  public Registry() {
    data_mergers = Maps.<TypeToken<?>, DataMerger<?>>newHashMap();
    initDataMergers();
  }
  
  /** @return An unmodifiable map of the data mergers. */
  public Map<TypeToken<?>, DataMerger<?>> dataMergers() {
    return Collections.unmodifiableMap(data_mergers);
  }
  
  /** @return Package private shutdown returning the deferred to wait on. */
  Deferred<Object> shutdown() { 
    return Deferred.fromResult(null);
  }
  
  private void initDataMergers() {
    final DataShardMerger shards_merger = new DataShardMerger();
    shards_merger.registerStrategy(new NumericMergeLargest());
    data_mergers.put(DataShardsGroup.TYPE, shards_merger);
  }
}
