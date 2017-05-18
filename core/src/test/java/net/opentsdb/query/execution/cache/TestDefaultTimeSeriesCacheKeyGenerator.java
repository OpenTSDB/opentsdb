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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.pojo.Downsampler;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DateTime.class, TimeSeriesQuery.class, Timespan.class })
public class TestDefaultTimeSeriesCacheKeyGenerator {

  private TSDB tsdb;
  private Config config;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(TSDB.class);
    config = new Config(false);
    when(tsdb.getConfig()).thenReturn(config);
  }
  
  @Test
  public void ctor() throws Exception {
    DefaultTimeSeriesCacheKeyGenerator generator = 
        new DefaultTimeSeriesCacheKeyGenerator();
    assertNull(generator.initialize(tsdb).join(1));
    assertEquals(DefaultTimeSeriesCacheKeyGenerator.DEFAULT_EXPIRATION, 
        generator.default_expiration);
    assertEquals(DefaultTimeSeriesCacheKeyGenerator.DEFAULT_MAX_EXPIRATION, 
        generator.default_max_expiration);
    assertEquals(DefaultTimeSeriesCacheKeyGenerator.DEFAULT_INTERVAL, 
        generator.default_interval);
    assertEquals(DefaultTimeSeriesCacheKeyGenerator.DEFAULT_HISTORICAL_CUTOFF, 
        generator.historical_cutoff);
    assertEquals(0, generator.step_interval);
    
    // overrides
    config.overrideConfig("tsd.query.cache.expiration", "120000");
    config.overrideConfig("tsd.query.cache.max_expiration", "3600000");
    config.overrideConfig("tsd.query.cache.default_interval", "30m");
    config.overrideConfig("tsd.query.cache.historical_cutoff", "864000");
    config.overrideConfig("tsd.query.cache.step_interval", "60000");
    
    generator = new DefaultTimeSeriesCacheKeyGenerator();
    assertNull(generator.initialize(tsdb).join(1));
    assertEquals(120000, generator.default_expiration);
    assertEquals(3600000, generator.default_max_expiration);
    assertEquals(1800000, generator.default_interval);
    assertEquals(864000, generator.historical_cutoff);
    assertEquals(60000, generator.step_interval);
    
    // bad parse
    config.overrideConfig("tsd.query.cache.expiration", "not a number");
    generator = new DefaultTimeSeriesCacheKeyGenerator();
    try {
      generator.initialize(tsdb).join(1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void generate() throws Exception {
    final DefaultTimeSeriesCacheKeyGenerator generator = 
        new DefaultTimeSeriesCacheKeyGenerator();
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
        new DefaultTimeSeriesCacheKeyGenerator();
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
        new DefaultTimeSeriesCacheKeyGenerator();
    generator.initialize(tsdb).join(1);
    
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
    assertEquals(10916, generator.expiration(query, -1));
    
    // old but no cutoff provided
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString()))
      .thenReturn(1493414769000L);
    assertEquals(10916, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString()))
      .thenReturn(1493514769000L);
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    assertEquals(10916, generator.expiration(query, -1));
    
    // regular
    assertEquals(0, generator.expiration(query, 0));
    assertEquals(30000, generator.expiration(query, 30000));
    
    assertEquals(60000, generator.expiration(null, -1));
  }
  
  @Test
  public void expirationWithCutoff() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.parseDuration(anyString())).thenReturn(60000L);
    config.overrideConfig("tsd.query.cache.historical_cutoff", "120000");
    config.overrideConfig("tsd.query.cache.max_expiration", "120000");
    final DefaultTimeSeriesCacheKeyGenerator generator = 
        new DefaultTimeSeriesCacheKeyGenerator();
    generator.initialize(tsdb).join(1);
    
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
    assertEquals(10916, generator.expiration(query, -1));
    
    // old so it's cached at the max
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString()))
      .thenReturn(1493414769000L);
    assertEquals(120000, generator.expiration(query, -1));
  }
  
  @Test
  public void expirationDefaultsNoDS() throws Exception {
    final DefaultTimeSeriesCacheKeyGenerator generator = 
        new DefaultTimeSeriesCacheKeyGenerator();
    generator.initialize(tsdb).join(1);
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493514769084L);
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString()))
      .thenReturn(1493514769000L);
    assertEquals(10916, generator.expiration(query, -1));
    
    // old but no cutoff
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString()))
      .thenReturn(1493414769000L);
    assertEquals(10916, generator.expiration(query, -1));
  }
  
  @Test
  public void expirationDefaultsDSDiffInterval() throws Exception {
    final DefaultTimeSeriesCacheKeyGenerator generator = 
        new DefaultTimeSeriesCacheKeyGenerator();
    generator.initialize(tsdb).join(1);
    
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.parseDuration(anyString())).thenReturn(300000L);
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago")
            .setDownsampler(Downsampler.newBuilder()
                .setAggregator("sum")
                .setInterval("5m")))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493514769084L);
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString()))
      .thenReturn(1493514769000L);
    assertEquals(130916, generator.expiration(query, -1));
    
    // old but no cutoff
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString()))
      .thenReturn(1493414769000L);
    assertEquals(130916, generator.expiration(query, -1));
  }
  
  @Test
  public void expirationNoDSDiffInterval() throws Exception {
    config.overrideConfig("tsd.query.cache.default_interval", "5m");
    final DefaultTimeSeriesCacheKeyGenerator generator = 
        new DefaultTimeSeriesCacheKeyGenerator();
    generator.initialize(tsdb).join(1);
    
    PowerMockito.mockStatic(DateTime.class);
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493514769084L);
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString()))
      .thenReturn(1493514769000L);
    assertEquals(130916, generator.expiration(query, -1));
    
    // old but no cutoff
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString()))
      .thenReturn(1493414769000L);
    assertEquals(130916, generator.expiration(query, -1));
  }

  @Test
  public void expirationStepDown() throws Exception {
    // 1 hour step interval
    // 6 hour historical cutoff
    config.overrideConfig("tsd.query.cache.max_expiration", "86400000");
    config.overrideConfig("tsd.query.cache.historical_cutoff", "21600000");
    config.overrideConfig("tsd.query.cache.step_interval", "3600000");
    final DefaultTimeSeriesCacheKeyGenerator generator = 
        new DefaultTimeSeriesCacheKeyGenerator();
    generator.initialize(tsdb).join(1);
    
    PowerMockito.mockStatic(DateTime.class);
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString()))
      .thenReturn(1493514000000L); // Sun, 30 Apr 2017 01:00:00 GMT  BLOCK TIME
    
    // present block
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493511663000L); //Sun, 30 Apr 2017 00:21:03 GMT 
    assertEquals(57000, generator.expiration(query, -1));
   
    // present block aligned
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493511660000L); //Sun, 30 Apr 2017 00:21:00 GMT 
    assertEquals(60000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493515260000L); //Sun, 30 Apr 2017 01:21:00 GMT adjacent block 1
      // isn't cached for long
    assertEquals(60000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493518860000L); // Sun, 30 Apr 2017 02:21:00 GMT  block 2
    assertEquals(600000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493522460000L); // Sun, 30 Apr 2017 03:21:00 GMT  block 3
    assertEquals(1200000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493529660000L); // Sun, 30 Apr 2017 05:21:00 GMT  block 5
    assertEquals(2400000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493533260000L); // Sun, 30 Apr 2017 06:21:00 GMT  block 6
    assertEquals(3000000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493536860000L); // Sun, 30 Apr 2017 07:21:00 GMT  block 7 hits max
    assertEquals(86400000, generator.expiration(query, -1));
  }
  
  @Test
  public void expirationStepDownAligned() throws Exception {
    // 1 hour step interval
    // 6 hour historical cutoff
    config.overrideConfig("tsd.query.cache.max_expiration", "86400000");
    config.overrideConfig("tsd.query.cache.historical_cutoff", "21600000");
    config.overrideConfig("tsd.query.cache.step_interval", "3600000");
    final DefaultTimeSeriesCacheKeyGenerator generator = 
        new DefaultTimeSeriesCacheKeyGenerator();
    generator.initialize(tsdb).join(1);
    
    PowerMockito.mockStatic(DateTime.class);
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString()))
      .thenReturn(1493514000000L); // Sun, 30 Apr 2017 01:00:00 GMT  BLOCK TIME
    
    // present block
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493514000000L); //Sun, 30 Apr 2017 01:00:00 GMT 
    assertEquals(60000, generator.expiration(query, -1));

    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493517600000L); //Sun, 30 Apr 2017 02:00:00 GMT adjacent block 1
    assertEquals(600000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493521200000L); // Sun, 30 Apr 2017 03:00:00 GMT  block 2
    assertEquals(1200000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493528400000L); // Sun, 30 Apr 2017 05:00:00 GMT  block 4
    assertEquals(2400000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493532000000L); // Sun, 30 Apr 2017 06:00:00 GMT  block 5
    assertEquals(3000000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493535600000L); // Sun, 30 Apr 2017 07:00:00 GMT  block 6 
    assertEquals(3600000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
    .thenReturn(1493539200000L); // Sun, 30 Apr 2017 08:00:00 GMT  block 7 hits max 
  assertEquals(86400000, generator.expiration(query, -1));
  }
}
