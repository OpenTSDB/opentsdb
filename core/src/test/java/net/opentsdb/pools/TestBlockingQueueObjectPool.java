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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.temporal.ChronoUnit;
import java.util.List;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;

public class TestBlockingQueueObjectPool {

  @Test
  public void ctorAndClaim() {
    TSDB tsdb = mock(TSDB.class);
    StatsCollector stats = mock(StatsCollector.class);
    when(tsdb.getStatsCollector()).thenReturn(stats);
    ObjectPoolAllocator allocator = mock(ObjectPoolAllocator.class);
    List<Object> objects = Lists.newArrayList();
    when(allocator.allocate()).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object obj = new Object();
        objects.add(obj);
        return obj;
      }
    });
    ObjectPoolConfig config = DefaultObjectPoolConfig.newBuilder()
        .setId("BA")
        .setAllocator(allocator)
        .setInitialCount(2)
        .setMaxCount(2)
        .build();
    BlockingQueueObjectPool pool = new BlockingQueueObjectPool(tsdb, config);
    
    assertEquals(2, objects.size());
    PooledObject po_a = pool.claim();
    assertSame(objects.get(0), po_a.object());
    
    PooledObject po_b = pool.claim(1, ChronoUnit.SECONDS);
    assertSame(objects.get(1), po_b.object());
    
    PooledObject po_c = pool.claim();
    assertNotSame(objects.get(0), po_c.object());
    assertNotSame(objects.get(1), po_c.object());
    
    po_b.release();
    po_c = pool.claim();
    assertSame(objects.get(1), po_c.object());
  }
  
}
