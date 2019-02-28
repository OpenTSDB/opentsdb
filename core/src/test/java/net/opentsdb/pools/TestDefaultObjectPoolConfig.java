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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.junit.Test;

public class TestDefaultObjectPoolConfig {

  @Test
  public void build() {
    ObjectPoolAllocator allocator = mock(ObjectPoolAllocator.class);
    
    ObjectPoolConfig config = DefaultObjectPoolConfig.newBuilder()
        .setAllocator(allocator)
        .setInitialCount(42)
        .setMaxCount(100)
        .setId("pool")
        .build();
    assertSame(allocator, config.allocator());
    assertEquals(42, config.initialCount());
    assertEquals(100, config.maxCount());
    assertEquals("pool", config.id());
    
    try {
      DefaultObjectPoolConfig.newBuilder()
          //.setAllocator(allocator)
          .setInitialCount(42)
          .setMaxCount(100)
          .setId("pool")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DefaultObjectPoolConfig.newBuilder()
          .setAllocator(allocator)
          .setInitialCount(42)
          .setMaxCount(100)
          //.setId("pool")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DefaultObjectPoolConfig.newBuilder()
          .setAllocator(allocator)
          .setInitialCount(42)
          .setMaxCount(100)
          .setId("")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DefaultObjectPoolConfig.newBuilder()
          .setAllocator(allocator)
          .setInitialCount(100)
          .setMaxCount(42)
          .setId("pool")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
}
