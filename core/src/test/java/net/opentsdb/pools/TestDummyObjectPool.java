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
package net.opentsdb.pools;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.temporal.ChronoUnit;

import org.junit.Test;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;

public class TestDummyObjectPool {

  @Test
  public void ctorAndClaim() {
    TSDB tsdb = mock(TSDB.class);
    StatsCollector stats = mock(StatsCollector.class);
    when(tsdb.getStatsCollector()).thenReturn(stats);
    ObjectPoolAllocator allocator = mock(ObjectPoolAllocator.class);
    Object obj = new Object();
    when(allocator.allocate()).thenReturn(obj);
    ObjectPoolConfig config = DefaultObjectPoolConfig.newBuilder()
        .setId("BA")
        .setAllocator(allocator)
        .setInitialCount(16)
        .setMaxCount(16)
        .build();
    
    DummyObjectPool pool = new DummyObjectPool(tsdb, config);
    assertSame(obj, pool.claim().object());
    assertSame(obj, pool.claim(1, ChronoUnit.SECONDS).object());
  }
  
}
