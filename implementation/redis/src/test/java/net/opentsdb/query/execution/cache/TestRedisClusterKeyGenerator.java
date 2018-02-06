// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DateTime.class, TimeSeriesQuery.class, Timespan.class })
public class TestRedisClusterKeyGenerator {

  private DefaultTSDB tsdb;
  private Config config;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(DefaultTSDB.class);
    config = new Config(false);
    when(tsdb.getConfig()).thenReturn(config);
  }
  
  @Test
  public void generate() throws Exception {
    final RedisClusterKeyGenerator generator = new RedisClusterKeyGenerator();
    generator.initialize(tsdb).join(1);
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    
    byte[] timed_hash = query.buildHashCode().asBytes();
    byte[] timeless_hash = query.buildTimelessHashCode().asBytes();
    
    byte[] key = generator.generate(query, true);
    byte[] hash = Arrays.copyOfRange(key, 1, 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length + 1);

    assertEquals('{', key[0]);
    assertArrayEquals(DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX, hash);
    hash = Arrays.copyOfRange(key, 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length + 1, 
        key.length - 1);
    assertArrayEquals(timed_hash, hash);
    assertEquals('}', key[key.length - 1]);
    
    key = generator.generate(query, false);
    assertEquals('{', key[0]);
    hash = Arrays.copyOfRange(key, 1, 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length + 1);
    assertArrayEquals(DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX, hash);
    hash = Arrays.copyOfRange(key, 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length + 1, 
        key.length - 1);
    assertArrayEquals(timeless_hash, hash);
    assertEquals('}', key[key.length - 1]);
    System.out.println(Arrays.toString(key));
    try {
      generator.generate(null, true);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void generateMulti() throws Exception {
    final RedisClusterKeyGenerator generator = new RedisClusterKeyGenerator();
    generator.initialize(tsdb).join(1);
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    
    byte[] timeless_hash = query.buildTimelessHashCode().asBytes();
    
    TimeStamp[][] time_ranges = new TimeStamp[3][];
    time_ranges[0] = new TimeStamp[2];
    time_ranges[0][0] = new MillisecondTimeStamp(1493942400000L);
    time_ranges[0][1] = new MillisecondTimeStamp(1493946000000L);
    time_ranges[1] = new TimeStamp[2];
    time_ranges[1][0] = new MillisecondTimeStamp(1493946000000L);
    time_ranges[1][1] = new MillisecondTimeStamp(1493949600000L);
    time_ranges[2] = new TimeStamp[2];
    time_ranges[2][0] = new MillisecondTimeStamp(1493949600000L);
    time_ranges[2][0] = new MillisecondTimeStamp(1493953200000L);
    
    byte[][] keys = generator.generate(query, time_ranges);
    assertEquals(3, keys.length);
    System.out.println(Arrays.toString(keys[0]));
    // prefix
    assertEquals('{', keys[0][0]);
    byte[] hash = Arrays.copyOfRange(keys[0], 1, 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length + 1);
    assertArrayEquals(DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX, hash);
    
    // hash
    hash = Arrays.copyOfRange(keys[0], 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length + 1,
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length + 1 
          + timeless_hash.length);
    assertArrayEquals(timeless_hash, hash);
    assertEquals('}', keys[0][DefaultTimeSeriesCacheKeyGenerator
                              .CACHE_PREFIX.length + 1 + timeless_hash.length]);
    
    // timestamp
    hash = Arrays.copyOfRange(keys[0], 
        DefaultTimeSeriesCacheKeyGenerator
        .CACHE_PREFIX.length + 2 + timeless_hash.length, 
        keys[0].length);
    assertEquals(0, Bytes.memcmp(hash, Bytes.fromLong(time_ranges[0][0].msEpoch())));
    
    // prefix
    hash = Arrays.copyOfRange(keys[1], 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length + 1,
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length + 1 
          + timeless_hash.length);
    assertArrayEquals(timeless_hash, hash);
    assertEquals('}', keys[1][DefaultTimeSeriesCacheKeyGenerator
                              .CACHE_PREFIX.length + 1 + timeless_hash.length]);
    
    // hash
    hash = Arrays.copyOfRange(keys[1], 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length + 1,
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length + 1 
          + timeless_hash.length);
    assertArrayEquals(timeless_hash, hash);
    assertEquals('}', keys[1][DefaultTimeSeriesCacheKeyGenerator
                              .CACHE_PREFIX.length + 1 + timeless_hash.length]);
    
    // timestamp
    hash = Arrays.copyOfRange(keys[1], 
        DefaultTimeSeriesCacheKeyGenerator
        .CACHE_PREFIX.length + 2 + timeless_hash.length, 
        keys[1].length);
    assertEquals(0, Bytes.memcmp(hash, Bytes.fromLong(time_ranges[1][0].msEpoch())));
    
    // prefix
    hash = Arrays.copyOfRange(keys[2], 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length + 1,
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length + 1 
          + timeless_hash.length);
    assertArrayEquals(timeless_hash, hash);
    assertEquals('}', keys[2][DefaultTimeSeriesCacheKeyGenerator
                              .CACHE_PREFIX.length + 1 + timeless_hash.length]);
    
    // hash
    hash = Arrays.copyOfRange(keys[2], 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length + 1,
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length + 1 
          + timeless_hash.length);
    assertArrayEquals(timeless_hash, hash);
    assertEquals('}', keys[2][DefaultTimeSeriesCacheKeyGenerator
                              .CACHE_PREFIX.length + 1 + timeless_hash.length]);
    
    // timestamp
    hash = Arrays.copyOfRange(keys[2], 
        DefaultTimeSeriesCacheKeyGenerator
        .CACHE_PREFIX.length + 2 + timeless_hash.length, 
        keys[2].length);
    assertEquals(0, Bytes.memcmp(hash, Bytes.fromLong(time_ranges[2][0].msEpoch())));
    
    try {
      generator.generate(null, time_ranges);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      generator.generate(query, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      generator.generate(null, new TimeStamp[0][]);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
