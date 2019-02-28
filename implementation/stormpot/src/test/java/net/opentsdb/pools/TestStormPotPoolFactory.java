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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.pools.TestStormPotPool.TestAllocator;

public class TestStormPotPoolFactory {
  private static MockTSDB TSDB;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB();
  }
  
  @Test
  public void initialize() throws Exception {
    StormPotPoolFactory factory = new StormPotPoolFactory();
    assertNull(factory.initialize(TSDB, "BA").join());
  }
  
  @Test
  public void newPool() throws Exception {
    StormPotPoolFactory factory = new StormPotPoolFactory();
    assertNull(factory.initialize(TSDB, "BA").join());
    
    TestAllocator allocator = new TestAllocator();
    ObjectPoolConfig config = DefaultObjectPoolConfig.newBuilder()
      .setId("BA")
      .setAllocator(allocator)
      .setInitialCount(16)
      .setMaxCount(16)
      .build();
    StormPotPool pool = (StormPotPool) factory.newPool(config);
    PooledObject obj = pool.claim();
    assertNotNull(obj);
    assertTrue(obj.object() instanceof byte[]);
  }
}
