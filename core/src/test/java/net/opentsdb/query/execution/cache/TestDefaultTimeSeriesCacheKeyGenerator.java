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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.Test;

import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.utils.Bytes;

public class TestDefaultTimeSeriesCacheKeyGenerator {

  @Test
  public void generate() throws Exception {
    final DefaultTimeSeriesCacheKeyGenerator generator = 
        new DefaultTimeSeriesCacheKeyGenerator();
    
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

    assertTrue(Bytes.memcmp(key, DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX, 
        0, DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length) == 0);
    assertArrayEquals(hash, timed_hash);
    
    key = generator.generate(query, false);
    hash = Arrays.copyOfRange(key, 
        DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length, key.length);
    assertTrue(Bytes.memcmp(key, DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX, 
        0, DefaultTimeSeriesCacheKeyGenerator.CACHE_PREFIX.length) == 0);
    assertArrayEquals(hash, timeless_hash);
    
    try {
      generator.generate(null, true);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
