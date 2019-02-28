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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.temporal.ChronoUnit;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.MockTSDB;

public class TestStormPotPool {
  private static MockTSDB TSDB;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB();
  }
  
  @Test
  public void ctor() throws Exception {
    TestAllocator allocator = new TestAllocator();
    ObjectPoolConfig config = DefaultObjectPoolConfig.newBuilder()
      .setId("BA")
      .setAllocator(allocator)
      .setInitialCount(16)
      .setMaxCount(16)
      .build();
    StormPotPool pool = new StormPotPool(TSDB, config);
    pool.shutdown();
  }
  
  @Test
  public void claim() throws Exception {
    TestAllocator allocator = new TestAllocator();
    ObjectPoolConfig config = DefaultObjectPoolConfig.newBuilder()
      .setId("BA")
      .setAllocator(allocator)
      .setInitialCount(16)
      .setMaxCount(16)
      .build();
    StormPotPool pool = new StormPotPool(TSDB, config);
    
    List<PooledObject> objs = Lists.newArrayList();
    for (int i = 0; i < 16; i++) {
      PooledObject obj = pool.claim();
      assertNotNull(obj);
      assertTrue(obj.object() instanceof byte[]);
      objs.add(obj);
    }
    
    assertEquals(0, pool.stormpot.getFailedAllocationCount());
    
    // now they're all checked out. It will hit our counter.
    PooledObject obj = pool.claim();
    assertNotNull(obj);
    assertTrue(obj.object() instanceof byte[]);
    assertEquals(0, pool.stormpot.getFailedAllocationCount());
    
    for (final PooledObject o : objs) {
      o.release();
    }
    
    obj = pool.claim();
    assertNotNull(obj);
    assertTrue(obj.object() instanceof byte[]);
    assertEquals(0, pool.stormpot.getFailedAllocationCount());
    
    pool.shutdown();
  }
  
  @Test
  public void claimTimed() throws Exception {
    TestAllocator allocator = new TestAllocator();
    ObjectPoolConfig config = DefaultObjectPoolConfig.newBuilder()
      .setId("BA")
      .setAllocator(allocator)
      .setInitialCount(16)
      .setMaxCount(16)
      .build();
    StormPotPool pool = new StormPotPool(TSDB, config);
    assertEquals(16, pool.stormpot.getTargetSize());
    
    List<PooledObject> objs = Lists.newArrayList();
    for (int i = 0; i < 16; i++) {
      PooledObject obj = pool.claim(1, ChronoUnit.SECONDS);
      assertNotNull(obj);
      assertTrue(obj.object() instanceof byte[]);
      objs.add(obj);
    }
    
    assertEquals(0, pool.stormpot.getFailedAllocationCount());
    
    // now they're all checked out. It will hit our counter.
    PooledObject obj = pool.claim(1, ChronoUnit.SECONDS);
    assertNotNull(obj);
    assertTrue(obj.object() instanceof byte[]);
    assertEquals(0, pool.stormpot.getFailedAllocationCount());
    
    for (final PooledObject o : objs) {
      o.release();
    }
    
    obj = pool.claim(1, ChronoUnit.SECONDS);
    assertNotNull(obj);
    assertTrue(obj.object() instanceof byte[]);
    assertEquals(0, pool.stormpot.getFailedAllocationCount());
    obj.release();
    
    obj = pool.claim(1, ChronoUnit.MILLIS);
    assertNotNull(obj);
    assertTrue(obj.object() instanceof byte[]);
    assertEquals(0, pool.stormpot.getFailedAllocationCount());
    obj.release();
    
    obj = pool.claim(1, ChronoUnit.MICROS);
    assertNotNull(obj);
    assertTrue(obj.object() instanceof byte[]);
    assertEquals(0, pool.stormpot.getFailedAllocationCount());
    obj.release();
    
    obj = pool.claim(1, ChronoUnit.NANOS);
    assertNotNull(obj);
    assertTrue(obj.object() instanceof byte[]);
    assertEquals(0, pool.stormpot.getFailedAllocationCount());
    obj.release();
    
    try {
      pool.claim(1, ChronoUnit.MINUTES);
      fail("Expected ObjectPoolException");
    } catch (ObjectPoolException e) { }
    
    pool.shutdown();
  }
  
  @Test
  public void shutdown() throws Exception {
    TestAllocator allocator = new TestAllocator();
    ObjectPoolConfig config = DefaultObjectPoolConfig.newBuilder()
      .setId("BA")
      .setAllocator(allocator)
      .setInitialCount(16)
      .setMaxCount(16)
      .build();
    StormPotPool pool = new StormPotPool(TSDB, config);
    assertNull(pool.shutdown().join());
  }
  
  static class TestAllocator implements ObjectPoolAllocator {
    
    @Override
    public String type() {
      return "ByteArray";
    }

    @Override
    public String id() {
      return "ByteArray";
    }

    @Override
    public Deferred<Object> initialize(net.opentsdb.core.TSDB tsdb, String id) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Deferred<Object> shutdown() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String version() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public int size() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public Object allocate() {
      return new byte[8];
    }

    @Override
    public void deallocate(Object object) {
      // no-op
    }

    @Override
    public TypeToken<?> dataType() {
      return TypeToken.of(byte[].class);
    }
    
  }
}
