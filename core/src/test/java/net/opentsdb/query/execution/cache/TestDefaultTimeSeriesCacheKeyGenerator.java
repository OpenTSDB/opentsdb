// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.core.MockTSDB;
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

  private MockTSDB tsdb;
  
  @Before
  public void before() throws Exception {
    tsdb = new MockTSDB();
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
    assertEquals(DateTime.parseDuration(DefaultTimeSeriesCacheKeyGenerator.DEFAULT_INTERVAL), 
        generator.default_interval);
    assertEquals(DefaultTimeSeriesCacheKeyGenerator.DEFAULT_HISTORICAL_CUTOFF, 
        generator.historical_cutoff);
    assertEquals(0, generator.step_interval);
    
    // overrides
    tsdb.config.override(DefaultTimeSeriesCacheKeyGenerator.EXPIRATION_KEY, 120000L);
    tsdb.config.override(DefaultTimeSeriesCacheKeyGenerator.MAX_EXPIRATION_KEY, 3600000L);
    tsdb.config.override(DefaultTimeSeriesCacheKeyGenerator.INTERVAL_KEY, "30m");
    tsdb.config.override(DefaultTimeSeriesCacheKeyGenerator.HISTORICAL_CUTOFF_KEY, 864000L);
    tsdb.config.override(DefaultTimeSeriesCacheKeyGenerator.STEP_INTERVAL_KEY, 60000L);
    
    // overrides will update the configs via callback.
    assertNull(generator.initialize(tsdb).join(1));
    assertEquals(120000, generator.default_expiration);
    assertEquals(3600000, generator.default_max_expiration);
    assertEquals(1800000, generator.default_interval);
    assertEquals(864000, generator.historical_cutoff);
    assertEquals(60000, generator.step_interval);
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
    // 84 milliseconds difference.
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
  public void expirationMetricDownsampler() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.parseDuration(anyString())).thenReturn(60000L);
    final DefaultTimeSeriesCacheKeyGenerator generator = 
        new DefaultTimeSeriesCacheKeyGenerator();
    generator.initialize(tsdb).join(1);
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setDownsampler(Downsampler.newBuilder()
                .setAggregator("sum")
                .setInterval("1m")))
        .build();
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493514769084L);
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString()))
      .thenReturn(1493514769000L);
    // 84 milliseconds difference.
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
    final DefaultTimeSeriesCacheKeyGenerator generator = 
        new DefaultTimeSeriesCacheKeyGenerator();
    generator.initialize(tsdb).join(1);
    tsdb.config.override(DefaultTimeSeriesCacheKeyGenerator.HISTORICAL_CUTOFF_KEY, 120000L);
    tsdb.config.override(DefaultTimeSeriesCacheKeyGenerator.MAX_EXPIRATION_KEY, 120000L);
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493514769084L);
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString()))
      .thenReturn(1493514769000L);
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
    
    assertEquals(10916, generator.expiration(query, -1));
    
    // old so it's cached at the max
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString()))
      .thenReturn(1493414769000L);
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago")
            .setDownsampler(Downsampler.newBuilder()
                .setAggregator("sum")
                .setInterval("1m")))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
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
    final DefaultTimeSeriesCacheKeyGenerator generator = 
        new DefaultTimeSeriesCacheKeyGenerator();
    generator.initialize(tsdb).join(1);
    tsdb.config.override(DefaultTimeSeriesCacheKeyGenerator.INTERVAL_KEY, "5m");
    
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
    final DefaultTimeSeriesCacheKeyGenerator generator = 
        new DefaultTimeSeriesCacheKeyGenerator();
    generator.initialize(tsdb).join(1);
    
    // 1 hour step interval
    // 6 hour historical cutoff
    tsdb.config.override(DefaultTimeSeriesCacheKeyGenerator.MAX_EXPIRATION_KEY, 86400000L);
    tsdb.config.override(DefaultTimeSeriesCacheKeyGenerator.HISTORICAL_CUTOFF_KEY, 21600000L);
    tsdb.config.override(DefaultTimeSeriesCacheKeyGenerator.STEP_INTERVAL_KEY, 3600000L);
    
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString()))
      .thenReturn(1493514000000L); // Sun, 30 Apr 2017 01:00:00 GMT  BLOCK TIME
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493511663000L); //Sun, 30 Apr 2017 00:21:03 GMT 
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    
    // present block
    assertEquals(57000, generator.expiration(query, -1));
   
    // present block aligned
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493511660000L); //Sun, 30 Apr 2017 00:21:00 GMT 
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    assertEquals(60000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493515260000L); //Sun, 30 Apr 2017 01:21:00 GMT adjacent block 1
      // isn't cached for long
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    assertEquals(60000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493518860000L); // Sun, 30 Apr 2017 02:21:00 GMT  block 2
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    assertEquals(600000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493522460000L); // Sun, 30 Apr 2017 03:21:00 GMT  block 3
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    assertEquals(1200000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493529660000L); // Sun, 30 Apr 2017 05:21:00 GMT  block 5
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    assertEquals(2400000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493533260000L); // Sun, 30 Apr 2017 06:21:00 GMT  block 6
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    assertEquals(3000000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493536860000L); // Sun, 30 Apr 2017 07:21:00 GMT  block 7 hits max
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    assertEquals(86400000, generator.expiration(query, -1));
  }
  
  @Test
  public void expirationStepDownAligned() throws Exception {
    final DefaultTimeSeriesCacheKeyGenerator generator = 
        new DefaultTimeSeriesCacheKeyGenerator();
    generator.initialize(tsdb).join(1);
    // 1 hour step interval
    // 6 hour historical cutoff
    tsdb.config.override(DefaultTimeSeriesCacheKeyGenerator.MAX_EXPIRATION_KEY, 86400000L);
    tsdb.config.override(DefaultTimeSeriesCacheKeyGenerator.HISTORICAL_CUTOFF_KEY, 21600000L);
    tsdb.config.override(DefaultTimeSeriesCacheKeyGenerator.STEP_INTERVAL_KEY, 3600000L);
    
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString()))
      .thenReturn(1493514000000L); // Sun, 30 Apr 2017 01:00:00 GMT  BLOCK TIME
    PowerMockito.when(DateTime.currentTimeMillis())
    .thenReturn(1493514000000L); //Sun, 30 Apr 2017 01:00:00 GMT 
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    
    // present block
    assertEquals(60000, generator.expiration(query, -1));

    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493517600000L); //Sun, 30 Apr 2017 02:00:00 GMT adjacent block 1
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    assertEquals(600000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493521200000L); // Sun, 30 Apr 2017 03:00:00 GMT  block 2
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    assertEquals(1200000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493528400000L); // Sun, 30 Apr 2017 05:00:00 GMT  block 4
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    assertEquals(2400000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493532000000L); // Sun, 30 Apr 2017 06:00:00 GMT  block 5
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    assertEquals(3000000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493535600000L); // Sun, 30 Apr 2017 07:00:00 GMT  block 6 
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    assertEquals(3600000, generator.expiration(query, -1));
    
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenReturn(1493539200000L); // Sun, 30 Apr 2017 08:00:00 GMT  block 7 hits max 
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("3h-ago")
            .setEnd("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    assertEquals(86400000, generator.expiration(query, -1));
  }
}
