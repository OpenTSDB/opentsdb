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
package net.opentsdb.query.readcache;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.core.Const;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.DateTime;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DateTime.class, TimeSeriesQuery.class, Timespan.class })
public class TestDefaultReadCacheKeyGenerator {

  private MockTSDB tsdb;
  
  @Before
  public void before() throws Exception {
    tsdb = new MockTSDB();
    PowerMockito.mockStatic(DateTime.class);
  }
  
  @Test
  public void ctor() throws Exception {
    when(DateTime.parseDuration(anyString())).thenCallRealMethod();
    DefaultReadCacheKeyGenerator generator = 
        new DefaultReadCacheKeyGenerator();
    assertNull(generator.initialize(tsdb, null).join(1));
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_EXPIRATION, 
        generator.default_expiration);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_MAX_EXPIRATION, 
        generator.default_max_expiration);
    assertEquals(DateTime.parseDuration(DefaultReadCacheKeyGenerator.DEFAULT_INTERVAL), 
        generator.default_interval);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_HISTORICAL_CUTOFF, 
        generator.historical_cutoff);
    assertEquals(0, generator.step_interval);
    
    // overrides
    tsdb.config.override(DefaultReadCacheKeyGenerator.EXPIRATION_KEY, 120000L);
    tsdb.config.override(DefaultReadCacheKeyGenerator.MAX_EXPIRATION_KEY, 3600000L);
    tsdb.config.override(DefaultReadCacheKeyGenerator.INTERVAL_KEY, "30m");
    tsdb.config.override(DefaultReadCacheKeyGenerator.HISTORICAL_CUTOFF_KEY, 864000L);
    tsdb.config.override(DefaultReadCacheKeyGenerator.STEP_INTERVAL_KEY, 60000L);
    
    // overrides will update the configs via callback.
    assertNull(generator.initialize(tsdb, null).join(1));
    assertEquals(120000, generator.default_expiration);
    assertEquals(3600000, generator.default_max_expiration);
    assertEquals(1800000, generator.default_interval);
    assertEquals(864000, generator.historical_cutoff);
    assertEquals(60000, generator.step_interval);
  }
  
  @Test
  public void oneSegment() throws Exception {
    final DefaultReadCacheKeyGenerator generator = 
        new DefaultReadCacheKeyGenerator();
    generator.initialize(tsdb, null).join(1);
    
    when(DateTime.currentTimeMillis()).thenReturn((long) ((1514764800L + (86400L * 2)) * 1000L));
    long[] expirations = new long[] { 300000 };
    byte[][] keys = generator.generate(42L, 
        "1h", 
        new int[] { 1514764800 }, 
        expirations);
    assertEquals(1, keys.length);
    assertArrayEquals(com.google.common.primitives.Bytes.concat(
        DefaultReadCacheKeyGenerator.CACHE_PREFIX, 
        "1h".getBytes(Const.ASCII_CHARSET),
        Bytes.fromLong(42),
        Bytes.fromInt(1514764800)), keys[0]);
    assertEquals((86400L * 2) * 1000, 
        expirations[0]);
    
    // now our query starts at the current time so we expire earlier.
    when(DateTime.currentTimeMillis()).thenReturn((long) ((1514764800L + (300L * 2)) * 1000L));
    expirations[0] = 300000;
    keys = generator.generate(42L, 
        "1h", 
        new int[] { 1514764800 }, 
        expirations);
    assertEquals(1, keys.length);
    assertEquals(600000, expirations[0]);
    
    // if the times match or the segment is for the future, expire it immediately
    when(DateTime.currentTimeMillis()).thenReturn((long) ((1514764800L) * 1000L));
    expirations[0] = 300000;
    keys = generator.generate(42L, 
        "1h", 
        new int[] { 1514764800 }, 
        expirations);
    assertEquals(1, keys.length);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_EXPIRATION, expirations[0]);
    
    // future
    when(DateTime.currentTimeMillis()).thenReturn((long) ((1514764800L - 900L) * 1000L));
    expirations[0] = 300000;
    keys = generator.generate(42L, 
        "1h", 
        new int[] { 1514764800 }, 
        expirations);
    assertEquals(1, keys.length);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_EXPIRATION, expirations[0]);
    
    // historical cutoff
    when(DateTime.currentTimeMillis()).thenReturn((long) ((1514764800L + (86400L * 2)) * 1000L));
    expirations[0] = 300000;
    generator.historical_cutoff = 86400000L;
    keys = generator.generate(42L, 
        "1h", 
        new int[] { 1514764800 }, 
        expirations);
    assertEquals(1, keys.length);
    assertEquals(86400000L, expirations[0]);
  }
  
  @Test
  public void multipleSegments() throws Exception {
    final DefaultReadCacheKeyGenerator generator = 
        new DefaultReadCacheKeyGenerator();
    generator.initialize(tsdb, null).join(1);
    
    when(DateTime.currentTimeMillis()).thenReturn((long) ((1514764800L + (86400L * 2)) * 1000L));
    long[] expirations = new long[] { 300000, 0, 0, 0 };
    byte[][] keys = generator.generate(42L, 
        "1h", 
        new int[] { 1514764800, 
                    1514764800 + 3600, 
                    1514764800 + (3600 * 2), 
                    1514764800 + (3600 * 3) }, 
        expirations);
    assertEquals(4, keys.length);
    assertArrayEquals(com.google.common.primitives.Bytes.concat(
        DefaultReadCacheKeyGenerator.CACHE_PREFIX, 
        "1h".getBytes(Const.ASCII_CHARSET),
        Bytes.fromLong(42),
        Bytes.fromInt(1514764800)), keys[0]);
    assertArrayEquals(com.google.common.primitives.Bytes.concat(
        DefaultReadCacheKeyGenerator.CACHE_PREFIX, 
        "1h".getBytes(Const.ASCII_CHARSET),
        Bytes.fromLong(42),
        Bytes.fromInt(1514764800 + 3600)), keys[1]);
    assertArrayEquals(com.google.common.primitives.Bytes.concat(
        DefaultReadCacheKeyGenerator.CACHE_PREFIX, 
        "1h".getBytes(Const.ASCII_CHARSET),
        Bytes.fromLong(42),
        Bytes.fromInt(1514764800 + (3600 * 2))), keys[2]);
    assertArrayEquals(com.google.common.primitives.Bytes.concat(
        DefaultReadCacheKeyGenerator.CACHE_PREFIX, 
        "1h".getBytes(Const.ASCII_CHARSET),
        Bytes.fromLong(42),
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
    when(DateTime.currentTimeMillis()).thenReturn((long) ((1514764800L + (3600 * 3) + (300L * 2)) * 1000L));
    expirations = new long[] { 300000, 0, 0, 0 };
    keys = generator.generate(42L, 
        "1h", 
        new int[] { 1514764800, 
                    1514764800 + 3600, 
                    1514764800 + (3600 * 2), 
                    1514764800 + (3600 * 3) }, 
        expirations);
    assertEquals(4, keys.length);
    assertEquals(11400000, expirations[0]);
    assertEquals(7800000, expirations[1]);
    assertEquals(4200000,  expirations[2]);
    assertEquals(600000, expirations[3]);
    
    // if the times match or the segment is for the future, expire it immediately
    when(DateTime.currentTimeMillis()).thenReturn((long) ((1514764800L) * 1000L));
    expirations = new long[] { 300000, 0, 0, 0 };
    keys = generator.generate(42L, 
        "1h", 
        new int[] { 1514764800, 
                    1514764800 + 3600, 
                    1514764800 + (3600 * 2), 
                    1514764800 + (3600 * 3) }, 
        expirations);
    assertEquals(4, keys.length);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_EXPIRATION, expirations[0]);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_EXPIRATION, expirations[1]);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_EXPIRATION, expirations[2]);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_EXPIRATION, expirations[3]);
    
    // future
    when(DateTime.currentTimeMillis()).thenReturn((long) ((1514764800L - 900L) * 1000L));
    expirations = new long[] { 300000, 0, 0, 0 };
    keys = generator.generate(42L, 
        "1h", 
        new int[] { 1514764800, 
                    1514764800 + 3600, 
                    1514764800 + (3600 * 2), 
                    1514764800 + (3600 * 3) }, 
        expirations);
    assertEquals(4, keys.length);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_EXPIRATION, expirations[0]);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_EXPIRATION, expirations[1]);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_EXPIRATION, expirations[2]);
    assertEquals(DefaultReadCacheKeyGenerator.DEFAULT_EXPIRATION, expirations[3]);
    
    // historical cutoff
    when(DateTime.currentTimeMillis()).thenReturn((long) ((1514764800L + (86400L * 2)) * 1000L));
    generator.historical_cutoff = 86400000L;
    expirations = new long[] { 300000, 0, 0, 0 };
    keys = generator.generate(42L, 
        "1h", 
        new int[] { 1514764800, 
                    1514764800 + 3600, 
                    1514764800 + (3600 * 2), 
                    1514764800 + (3600 * 3) }, 
        expirations);
    assertEquals(4, keys.length);
    assertEquals(86400000L, expirations[0]);
    assertEquals(86400000L, expirations[1]);
    assertEquals(86400000L,  expirations[2]);
    assertEquals(86400000L, expirations[3]);
  }
  
}