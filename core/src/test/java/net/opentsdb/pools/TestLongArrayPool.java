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
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.BeforeClass;
import org.junit.Test;

import net.opentsdb.core.MockTSDB;

public class TestLongArrayPool {
  private static MockTSDB TSDB;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB();
  }
  
  @Test
  public void initialize() throws Exception {
    ObjectPool pool = mock(ObjectPool.class);
    ObjectPoolFactory factory = mock(ObjectPoolFactory.class);
    when(factory.newPool(any(ObjectPoolConfig.class))).thenReturn(pool);
    
    LongArrayPool allocator = new LongArrayPool();
    assertNull(allocator.initialize(TSDB, null).join());
    assertEquals(LongArrayPool.TYPE, allocator.id());
    verify(TSDB.getRegistry(), atLeast(1)).registerObjectPool(
        any(DummyObjectPool.class));
    verify(TSDB.getRegistry(), never()).registerObjectPool(pool);
    assertEquals(4096, ((long[]) allocator.allocate()).length);
    
    when(TSDB.getRegistry().getPlugin(ObjectPoolFactory.class, null))
      .thenReturn(factory);
    assertNull(allocator.initialize(TSDB, null).join());
    verify(TSDB.getRegistry(), times(1)).registerObjectPool(pool);
    assertEquals(LongArrayPool.TYPE, allocator.id());
    assertEquals(4096, ((long[]) allocator.allocate()).length);
    
    allocator.id = "foo";
    allocator.registerConfigs(TSDB.config, LongArrayPool.TYPE);
    TSDB.config.override("objectpool.foo.pool.id", "myfactory");
    TSDB.config.override("objectpool.foo.count.initial", "42");
    TSDB.config.override("objectpool.foo.primitive.array.length", "16");
    ObjectPool pool2 = mock(ObjectPool.class);
    ObjectPoolFactory factory2 = mock(ObjectPoolFactory.class);
    when(factory2.newPool(any(ObjectPoolConfig.class))).thenReturn(pool2);
    when(TSDB.getRegistry().getPlugin(ObjectPoolFactory.class, "myfactory"))
      .thenReturn(factory);
    
    assertNull(allocator.initialize(TSDB, "foo").join());
    verify(TSDB.getRegistry(), never()).registerObjectPool(pool2);
    assertEquals(16, ((long[]) allocator.allocate()).length);
  }
  
}
