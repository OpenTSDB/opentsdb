// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.query.execution.cache;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.pojo.Downsampler;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.DateTime;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DateTime.class, TimeSeriesQuery.class, Timespan.class })
public class TestDefaultTimeSeriesCacheKeyGenerator {

  @Test
  public void generate() throws Exception {
    final DefaultTimeSeriesCacheKeyGenerator generator = 
        new DefaultTimeSeriesCacheKeyGenerator(60000, 120000);
    
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
    byte[] hash = Arrays.copyOfRange(key, 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length, key.length);

    assertEquals(0, Bytes.memcmp(key, DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX, 
        0, DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length));
    assertArrayEquals(hash, timed_hash);
    
    key = generator.generate(query, false);
    hash = Arrays.copyOfRange(key, 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length, key.length);
    assertEquals(0, Bytes.memcmp(key, DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX, 
        0, DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length));
    assertArrayEquals(hash, timeless_hash);
    
    try {
      generator.generate(null, true);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void generateMulti() throws Exception {
    final DefaultTimeSeriesCacheKeyGenerator generator = 
        new DefaultTimeSeriesCacheKeyGenerator(60000, 120000);
    
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
    
    // prefix
    assertEquals(0, Bytes.memcmp(keys[0], 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX, 
        0, DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length));
    
    // hash
    byte[] hash = Arrays.copyOfRange(keys[0], 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length, 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length + timeless_hash.length);
    assertEquals(0, Bytes.memcmp(hash, timeless_hash));
    
    hash = Arrays.copyOfRange(keys[0], 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length + timeless_hash.length, 
        keys[0].length);
    assertEquals(0, Bytes.memcmp(hash, Bytes.fromLong(time_ranges[0][0].msEpoch())));
    
    // prefix
    assertEquals(0, Bytes.memcmp(keys[1], 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX, 
        0, DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length));
    
    // hash
    hash = Arrays.copyOfRange(keys[1], 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length, 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length + timeless_hash.length);
    assertEquals(0, Bytes.memcmp(hash, timeless_hash));
    
    hash = Arrays.copyOfRange(keys[1], 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length + timeless_hash.length, 
        keys[0].length);
    assertEquals(0, Bytes.memcmp(hash, Bytes.fromLong(time_ranges[1][0].msEpoch())));
    
    // prefix
    assertEquals(0, Bytes.memcmp(keys[2], 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX, 
        0, DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length));
    
    // hash
    hash = Arrays.copyOfRange(keys[2], 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length, 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length + timeless_hash.length);
    assertEquals(0, Bytes.memcmp(hash, timeless_hash));
    
    hash = Arrays.copyOfRange(keys[2], 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length + timeless_hash.length, 
        keys[0].length);
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
  
  @Test
  public void expiration() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.parseDuration(anyString())).thenReturn(60000L);
    final DefaultTimeSeriesCacheKeyGenerator generator = 
        new DefaultTimeSeriesCacheKeyGenerator(60000, 120000);
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago")
            .setDownsampler(Downsampler.newBuilder()
                .setAggregator("sum")
                .setInterval("1m")))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493514769084L);
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString()))
      .thenReturn(1493514769000L);
    assertEquals(49084, generator.expiration(query, -1));
    
    // old so it's cached at the max
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString()))
      .thenReturn(1493414769000L);
    assertEquals(120000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString()))
      .thenReturn(1493514769000L);
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    assertEquals(60000, generator.expiration(query, -1));
    
    // regular
    assertEquals(0, generator.expiration(query, 0));
    assertEquals(30000, generator.expiration(query, 30000));
    
    assertEquals(60000, generator.expiration(null, -1));
  }
}
