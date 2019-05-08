// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.storage.schemas.tsdb1x;

import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.pools.BaseObjectPoolAllocator;
import net.opentsdb.pools.DefaultObjectPoolConfig;
import net.opentsdb.pools.ObjectPoolConfig;

/**
 * An allocator pool for 1x numeric summary PTS.
 * 
 * @since 3.0
 */
public class Tsdb1xNumericSummaryPartialTimeSeriesPool extends BaseObjectPoolAllocator {
  public static final String TYPE = "Tsdb1xNumericSummaryPartialTimeSeries";
  public static final TypeToken<?> TYPE_TOKEN = TypeToken.of(
      Tsdb1xNumericSummaryPartialTimeSeries.class);
  
  @Override
  public Object allocate() {
    return new Tsdb1xNumericSummaryPartialTimeSeries();
  }

  @Override
  public TypeToken<?> dataType() {
    return TYPE_TOKEN;
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    if (Strings.isNullOrEmpty(id)) {
      this.id = TYPE;
    } else {
      this.id = id;
    }
    
    registerConfigs(tsdb.getConfig(), TYPE);
    
    final ObjectPoolConfig config = DefaultObjectPoolConfig.newBuilder()
        .setAllocator(this)
        .setInitialCount(tsdb.getConfig().getInt(configKey(COUNT_KEY, TYPE)))
        .setMaxCount(tsdb.getConfig().getInt(configKey(COUNT_KEY, TYPE)))
        .setId(this.id)
        .build();
    try {
      createAndRegisterPool(tsdb, config, TYPE);
      return Deferred.fromResult(null);
    } catch (Exception e) {
      return Deferred.fromError(e);
    }
  }

}
