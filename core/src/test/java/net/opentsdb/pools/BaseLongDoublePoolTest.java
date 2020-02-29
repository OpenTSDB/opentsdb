// This file is part of OpenTSDB.
// Copyright (C) 2020 The OpenTSDB Authors.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.BeforeClass;

import net.opentsdb.core.Registry;

/**
 * Base for iterator tests that may be using the long or double pool.
 * 
 * @since 3.0
 */
public class BaseLongDoublePoolTest {
  
  protected static net.opentsdb.core.TSDB TSDB;
  protected static MockArrayObjectPool LONG_POOL;
  protected static MockArrayObjectPool DOUBLE_POOL;
  
  @BeforeClass
  public static void beforeClass() {
    LONG_POOL = new MockArrayObjectPool(DefaultObjectPoolConfig.newBuilder()
        .setAllocator(new LongArrayPool())
        .setInitialCount(4)
        .setArrayLength(16)
        .setId(LongArrayPool.TYPE)
        .build());
    DOUBLE_POOL = new MockArrayObjectPool(DefaultObjectPoolConfig.newBuilder()
        .setAllocator(new DoubleArrayPool())
        .setInitialCount(4)
        .setArrayLength(16)
        .setId(DoubleArrayPool.TYPE)
        .build());
    TSDB = mock(net.opentsdb.core.TSDB.class);
    Registry registry = mock(Registry.class);
    when(TSDB.getRegistry()).thenReturn(registry);
    when(registry.getObjectPool(LongArrayPool.TYPE)).thenReturn(LONG_POOL);
    when(registry.getObjectPool(DoubleArrayPool.TYPE)).thenReturn(DOUBLE_POOL);
  }
  
  @Before
  public void before() {
    LONG_POOL.resetCounters();
    DOUBLE_POOL.resetCounters();
  }
  
  public static void assertPooledArrayEquals(final double[] expected, 
                                             final double[] tested,
                                             final double epsilon) {
    final double[] copy = Arrays.copyOf(tested, expected.length);
    assertArrayEquals(expected, copy, epsilon);
  }
  
  public static void assertPooledArrayEquals(final int[] expected, 
                                             final int[] tested) {
    final int[] copy = Arrays.copyOf(tested, expected.length);
    assertArrayEquals(expected, copy);
  }
  
  public static void assertPooledArrayEquals(final long[] expected, 
                                             final long[] tested) {
    final long[] copy = Arrays.copyOf(tested, expected.length);
    assertArrayEquals(expected, copy);
  }
  
  public static void assertPoolCounters(final MockArrayObjectPool pool,
                                        final int success,
                                        final int empty,
                                        final int too_big,
                                        final int released) {
    assertEquals(success, pool.claim_success);
    assertEquals(empty, pool.claim_empty_pool);
    assertEquals(too_big, pool.claim_too_big);
    assertEquals(released, pool.released);
  }

}
