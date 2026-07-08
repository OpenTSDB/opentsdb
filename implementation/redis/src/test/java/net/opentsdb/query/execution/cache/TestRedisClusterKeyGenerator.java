// This file is part of OpenTSDB.
// Copyright (C) 2017-2019  The OpenTSDB Authors.
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
package net.opentsdb.query.execution.cache;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.After;

import java.lang.reflect.Field;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import net.opentsdb.core.Const;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.query.readcache.DefaultReadCacheKeyGenerator;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.DateTime;

public class TestRedisClusterKeyGenerator {

  private MockedStatic<DateTime> mockedDateTime;

  private MockTSDB tsdb;
  
  @Before
  public void before() throws Exception {
    mockedDateTime = Mockito.mockStatic(DateTime.class);
    tsdb = new MockTSDB();
  }

  @After
  public void tearDownStaticMocks() {
    mockedDateTime.closeOnDemand();
  }

  @Test
  public void generate() throws Exception {
    final RedisClusterKeyGenerator generator = new RedisClusterKeyGenerator();
    generator.initialize(tsdb, null).join(1);

    mockedDateTime.when(DateTime::currentTimeMillis).thenReturn((long) ((1514764800L + (86400L * 2)) * 1000L));
    long[] expirations = new long[]{300000};
    byte[][] keys = generator.generate(42L,
         "1h",
         new int[]{1514764800},
         expirations);
    assertEquals(1, keys.length);
    assertArrayEquals(com.google.common.primitives.Bytes.concat(
         new byte[]{'{'},
         DefaultReadCacheKeyGenerator.CACHE_PREFIX,
         "1h".getBytes(Const.ASCII_CHARSET),
         Bytes.fromLong(42),
         new byte[]{'}'},
         Bytes.fromInt(1514764800)), keys[0]);
    assertEquals((86400L * 2) * 1000,
         expirations[0]);

    // now our query starts at the current time so we expire earlier.
    mockedDateTime.when(DateTime::currentTimeMillis).thenReturn((long) ((1514764800L + (300L * 2)) * 1000L));
    expirations[0] = 300000;
    keys = generator.generate(42L,
         "1h",
         new int[]{1514764800},
         expirations);
    assertEquals(1, keys.length);
    assertEquals(600000, expirations[0]);

    // if the times match or the segment is for the future, expire it immediately
    mockedDateTime.when(DateTime::currentTimeMillis).thenReturn((long) ((1514764800L) * 1000L));
    expirations[0] = 300000;
    keys = generator.generate(42L,
         "1h",
         new int[]{1514764800},
         expirations);
    assertEquals(1, keys.length);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_EXPIRATION, expirations[0]);

    // future
    mockedDateTime.when(DateTime::currentTimeMillis).thenReturn((long) ((1514764800L - 900L) * 1000L));
    expirations[0] = 300000;
    keys = generator.generate(42L,
         "1h",
         new int[]{1514764800},
         expirations);
    assertEquals(1, keys.length);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_EXPIRATION, expirations[0]);

    // historical cutoff
    mockedDateTime.when(DateTime::currentTimeMillis).thenReturn((long) ((1514764800L + (86400L * 2)) * 1000L));
    expirations[0] = 300000;
    Field historical_cutoffField = generator.getClass().getSuperclass().getDeclaredField("historical_cutoff");
    historical_cutoffField.setAccessible(true);
    historical_cutoffField.set(generator, 86400000L);
    keys = generator.generate(42L,
         "1h",
         new int[]{1514764800},
         expirations);
    assertEquals(1, keys.length);
    assertEquals(86400000L, expirations[0]);
  }

  @Test
  public void generateMulti() throws Exception {
    final RedisClusterKeyGenerator generator = new RedisClusterKeyGenerator();
    generator.initialize(tsdb, null).join(1);

    mockedDateTime.when(DateTime::currentTimeMillis).thenReturn((long) ((1514764800L + (86400L * 2)) * 1000L));
    long[] expirations = new long[]{300000, 0, 0, 0};
    byte[][] keys = generator.generate(42L,
         "1h",
         new int[]{1514764800,
              1514764800 + 3600,
              1514764800 + (3600 * 2),
              1514764800 + (3600 * 3)},
         expirations);
    assertEquals(4, keys.length);
    assertArrayEquals(com.google.common.primitives.Bytes.concat(
         new byte[]{'{'},
         DefaultReadCacheKeyGenerator.CACHE_PREFIX,
         "1h".getBytes(Const.ASCII_CHARSET),
         Bytes.fromLong(42),
         new byte[]{'}'},
         Bytes.fromInt(1514764800)), keys[0]);
    assertArrayEquals(com.google.common.primitives.Bytes.concat(
         new byte[]{'{'},
         DefaultReadCacheKeyGenerator.CACHE_PREFIX,
         "1h".getBytes(Const.ASCII_CHARSET),
         Bytes.fromLong(42),
         new byte[]{'}'},
         Bytes.fromInt(1514764800 + 3600)), keys[1]);
    assertArrayEquals(com.google.common.primitives.Bytes.concat(
         new byte[]{'{'},
         DefaultReadCacheKeyGenerator.CACHE_PREFIX,
         "1h".getBytes(Const.ASCII_CHARSET),
         Bytes.fromLong(42),
         new byte[]{'}'},
         Bytes.fromInt(1514764800 + (3600 * 2))), keys[2]);
    assertArrayEquals(com.google.common.primitives.Bytes.concat(
         new byte[]{'{'},
         DefaultReadCacheKeyGenerator.CACHE_PREFIX,
         "1h".getBytes(Const.ASCII_CHARSET),
         Bytes.fromLong(42),
         new byte[]{'}'},
         Bytes.fromInt(1514764800 + (3600 * 3))), keys[3]);
    assertEquals((86400L * 2) * 1000,
         expirations[0]);
    assertEquals(((86400L * 2) - 3600) * 1000,
         expirations[1]);
    assertEquals(((86400L * 2) - (3600 * 2)) * 1000,
         expirations[2]);
    assertEquals(((86400L * 2) - (3600 * 3)) * 1000,
         expirations[3]);

    // now our query starts at the current time so we expire earlier.
    mockedDateTime.when(DateTime::currentTimeMillis).thenReturn((long) ((1514764800L + (3600 * 3) + (300L * 2)) * 1000L));
    expirations = new long[]{300000, 0, 0, 0};
    keys = generator.generate(42L,
         "1h",
         new int[]{1514764800,
              1514764800 + 3600,
              1514764800 + (3600 * 2),
              1514764800 + (3600 * 3)},
         expirations);
    assertEquals(4, keys.length);
    assertEquals(11400000, expirations[0]);
    assertEquals(7800000, expirations[1]);
    assertEquals(4200000, expirations[2]);
    assertEquals(600000, expirations[3]);

    // if the times match or the segment is for the future, expire it immediately
    mockedDateTime.when(DateTime::currentTimeMillis).thenReturn((long) ((1514764800L) * 1000L));
    expirations = new long[]{300000, 0, 0, 0};
    keys = generator.generate(42L,
         "1h",
         new int[]{1514764800,
              1514764800 + 3600,
              1514764800 + (3600 * 2),
              1514764800 + (3600 * 3)},
         expirations);
    assertEquals(4, keys.length);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_EXPIRATION, expirations[0]);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_EXPIRATION, expirations[1]);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_EXPIRATION, expirations[2]);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_EXPIRATION, expirations[3]);

    // future
    mockedDateTime.when(DateTime::currentTimeMillis).thenReturn((long) ((1514764800L - 900L) * 1000L));
    expirations = new long[]{300000, 0, 0, 0};
    keys = generator.generate(42L,
         "1h",
         new int[]{1514764800,
              1514764800 + 3600,
              1514764800 + (3600 * 2),
              1514764800 + (3600 * 3)},
         expirations);
    assertEquals(4, keys.length);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_EXPIRATION, expirations[0]);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_EXPIRATION, expirations[1]);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_EXPIRATION, expirations[2]);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_EXPIRATION, expirations[3]);

    // historical cutoff
    mockedDateTime.when(DateTime::currentTimeMillis).thenReturn((long) ((1514764800L + (86400L * 2)) * 1000L));
    Field historical_cutoffField = generator.getClass().getSuperclass().getDeclaredField("historical_cutoff");
    historical_cutoffField.setAccessible(true);
    historical_cutoffField.set(generator, 86400000L);
    expirations = new long[]{300000, 0, 0, 0};
    keys = generator.generate(42L,
         "1h",
         new int[]{1514764800,
              1514764800 + 3600,
              1514764800 + (3600 * 2),
              1514764800 + (3600 * 3)},
         expirations);
    assertEquals(4, keys.length);
    assertEquals(86400000L, expirations[0]);
    assertEquals(86400000L, expirations[1]);
    assertEquals(86400000L, expirations[2]);
    assertEquals(86400000L, expirations[3]);
  }
}
