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
package net.opentsdb.storage.schemas.tsdb1x;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.exceptions.IllegalDataException;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupUtils;
import net.opentsdb.utils.Bytes;

public class TestTsdb1xNumericSummaryPartialTimeSeries {
  private static final TimeStamp BASE_TIME = new SecondTimeStamp(1514764800);
  private static final PartialTimeSeriesSet SET = mock(PartialTimeSeriesSet.class);
  private static DefaultRollupConfig ROLLUP_CONFIG = DefaultRollupConfig.newBuilder()
      .addAggregationId("sum", 0)
      .addAggregationId("count", 1)
      .addAggregationId("min", 2)
      .addAggregationId("max", 3)
      .addInterval(RollupInterval.builder()
        .setInterval("1h")
        .setRowSpan("1d")
        .setTable("tsdb-rollup-1h")
        .setPreAggregationTable("tsdb-rollup-1h")
        .build())
      .build();
  
  private ObjectPool pool;
  private RollupInterval interval;
  
  @Before
  public void before() throws Exception {
    pool = mock(ObjectPool.class);
    when(pool.claim()).thenAnswer(new Answer<PooledObject>() {
      @Override
      public PooledObject answer(InvocationOnMock invocation) throws Throwable {
        PooledObject obj = mock(PooledObject.class);
        byte[] array = new byte[4096];
        when(obj.object()).thenReturn(array);
        return obj;
      }
    });
    interval = spy(RollupInterval.builder()
        .setInterval("1h")
        .setRowSpan("1d")
        .setTable("tsdb-rollup-1h")
        .setPreAggregationTable("tsdb-rollup-1h")
        .build());
    when(interval.rollupConfig()).thenReturn(ROLLUP_CONFIG);
  }
  
  @Test
  public void addColumnPutsNumericPrefix() throws Exception {
    Tsdb1xNumericSummaryPartialTimeSeries pts = new Tsdb1xNumericSummaryPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, interval);
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch(), (short) 0, 0, interval), new byte[] { 42 });
    assertEquals(42, pts.idHash());
    assertSame(SET, pts.set());
    assertEquals(0, pts.value().offset());
    assertEquals(12, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(42, pts.value().data()[11]); // data
    
    pts.addColumn((byte) 2, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch() + 3600, (short) 0, 2, interval), new byte[] { 24 });
    assertEquals(0, pts.value().offset());
    assertEquals(24, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(42, pts.value().data()[11]); // data
    assertEquals(BASE_TIME.epoch() + 3600, Bytes.getLong(pts.value().data(), 12));
    assertEquals(1, pts.value().data()[20]); // count
    assertEquals(2, pts.value().data()[21]); // type
    assertEquals(0, pts.value().data()[22]); // flags
    assertEquals(24, pts.value().data()[23]); // data
    
    pts.addColumn((byte) 2, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch() + (3600 * 2), (short) 3, 2, interval), Bytes.fromInt(1024));
    assertEquals(0, pts.value().offset());
    assertEquals(39, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(42, pts.value().data()[11]); // data
    assertEquals(BASE_TIME.epoch() + 3600, Bytes.getLong(pts.value().data(), 12));
    assertEquals(1, pts.value().data()[20]); // count
    assertEquals(2, pts.value().data()[21]); // type
    assertEquals(0, pts.value().data()[22]); // flags
    assertEquals(24, pts.value().data()[23]); // data
    assertEquals(BASE_TIME.epoch() + (3600 * 2), Bytes.getLong(pts.value().data(), 24));
    assertEquals(1, pts.value().data()[32]); // count
    assertEquals(2, pts.value().data()[33]); // type
    assertEquals(3, pts.value().data()[34]); // flags
    assertEquals(0, pts.value().data()[35]); // data
    assertEquals(0, pts.value().data()[36]); // data
    assertEquals(4, pts.value().data()[37]); // data
    assertEquals(0, pts.value().data()[38]); // data
    
    pts.addColumn((byte) 1, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch() + (3600 * 3), (short) 0XB, 1, interval), 
        Bytes.fromInt(Float.floatToRawIntBits(42.5F)));
    assertEquals(0, pts.value().offset());
    assertEquals(54, pts.value().end());
    assertEquals(BASE_TIME.epoch() + (3600 * 3), Bytes.getLong(pts.value().data(), 39));
    assertEquals(1, pts.value().data()[47]); // count
    assertEquals(1, pts.value().data()[48]); // type
    assertEquals(0xB, pts.value().data()[49]); // flags
    assertEquals(42.5, Float.intBitsToFloat(Bytes.getInt(pts.value().data(), 50)), 0.001);
    
    pts.addColumn((byte) 1, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch() + (3600 * 4), (short) 0XF, 0, interval), 
        Bytes.fromLong(Double.doubleToRawLongBits(24.75F)));
    assertEquals(0, pts.value().offset());
    assertEquals(73, pts.value().end());
    assertEquals(BASE_TIME.epoch() + (3600 * 4), Bytes.getLong(pts.value().data(), 54));
    assertEquals(1, pts.value().data()[62]); // count
    assertEquals(0, pts.value().data()[63]); // type
    assertEquals(0xF, pts.value().data()[64]); // flags
    assertEquals(24.75, Double.longBitsToDouble(Bytes.getLong(pts.value().data(), 65)), 0.001);
  }
  
  @Test
  public void addColumnPutsStringPrefix() throws Exception {
    Tsdb1xNumericSummaryPartialTimeSeries pts = new Tsdb1xNumericSummaryPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, interval);
    pts.addColumn((byte) 0, RollupUtils.buildStringRollupQualifier(
        "SUM", BASE_TIME.epoch(), (short) 0, interval), new byte[] { 42 });
    assertEquals(42, pts.idHash());
    assertSame(SET, pts.set());
    assertEquals(0, pts.value().offset());
    assertEquals(12, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(42, pts.value().data()[11]); // data
    
    pts.addColumn((byte) 2, RollupUtils.buildStringRollupQualifier(
        "MIN", BASE_TIME.epoch() + 3600, (short) 0, interval), new byte[] { 24 });
    assertEquals(0, pts.value().offset());
    assertEquals(24, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(42, pts.value().data()[11]); // data
    assertEquals(BASE_TIME.epoch() + 3600, Bytes.getLong(pts.value().data(), 12));
    assertEquals(1, pts.value().data()[20]); // count
    assertEquals(2, pts.value().data()[21]); // type
    assertEquals(0, pts.value().data()[22]); // flags
    assertEquals(24, pts.value().data()[23]); // data
    
    pts.addColumn((byte) 2, RollupUtils.buildStringRollupQualifier(
        "MIN", BASE_TIME.epoch() + (3600 * 2), (short) 3, interval), Bytes.fromInt(1024));
    assertEquals(0, pts.value().offset());
    assertEquals(39, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(42, pts.value().data()[11]); // data
    assertEquals(BASE_TIME.epoch() + 3600, Bytes.getLong(pts.value().data(), 12));
    assertEquals(1, pts.value().data()[20]); // count
    assertEquals(2, pts.value().data()[21]); // type
    assertEquals(0, pts.value().data()[22]); // flags
    assertEquals(24, pts.value().data()[23]); // data
    assertEquals(BASE_TIME.epoch() + (3600 * 2), Bytes.getLong(pts.value().data(), 24));
    assertEquals(1, pts.value().data()[32]); // count
    assertEquals(2, pts.value().data()[33]); // type
    assertEquals(3, pts.value().data()[34]); // flags
    assertEquals(0, pts.value().data()[35]); // data
    assertEquals(0, pts.value().data()[36]); // data
    assertEquals(4, pts.value().data()[37]); // data
    assertEquals(0, pts.value().data()[38]); // data
    
    pts.addColumn((byte) 1, RollupUtils.buildStringRollupQualifier(
        "COUNT", BASE_TIME.epoch() + (3600 * 3), (short) 0xB, interval), 
        Bytes.fromInt(Float.floatToRawIntBits(42.5F)));
    assertEquals(0, pts.value().offset());
    assertEquals(54, pts.value().end());
    assertEquals(BASE_TIME.epoch() + (3600 * 3), Bytes.getLong(pts.value().data(), 39));
    assertEquals(1, pts.value().data()[47]); // count
    assertEquals(1, pts.value().data()[48]); // type
    assertEquals(0xB, pts.value().data()[49]); // flags
    assertEquals(42.5, Float.intBitsToFloat(Bytes.getInt(pts.value().data(), 50)), 0.001);
    
    pts.addColumn((byte) 1, RollupUtils.buildStringRollupQualifier(
        "SUM", BASE_TIME.epoch() + (3600 * 4), (short) 0xF, interval), 
        Bytes.fromLong(Double.doubleToRawLongBits(24.75F)));
    assertEquals(0, pts.value().offset());
    assertEquals(73, pts.value().end());
    assertEquals(BASE_TIME.epoch() + (3600 * 4), Bytes.getLong(pts.value().data(), 54));
    assertEquals(1, pts.value().data()[62]); // count
    assertEquals(0, pts.value().data()[63]); // type
    assertEquals(0xF, pts.value().data()[64]); // flags
    assertEquals(24.75, Double.longBitsToDouble(Bytes.getLong(pts.value().data(), 65)), 0.001);
  }

  @Test
  public void addColumnAggregated() throws Exception {
    Tsdb1xNumericSummaryPartialTimeSeries pts = new Tsdb1xNumericSummaryPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, interval);
    
    pts.addColumn((byte) 0, new byte[] { 0 }, RollupUtils.buildAppendRollupValue(
        BASE_TIME.epoch(), (short) 0, interval, new byte[] { 42 }));
    assertEquals(42, pts.idHash());
    assertSame(SET, pts.set());
    assertEquals(0, pts.value().offset());
    assertEquals(12, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(42, pts.value().data()[11]); // data
    
    pts.addColumn((byte) 2, new byte[] { 2 }, RollupUtils.buildAppendRollupValue(
        BASE_TIME.epoch() + 3600, (short) 0, interval, new byte[] { 24 }));
    assertEquals(0, pts.value().offset());
    assertEquals(24, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(42, pts.value().data()[11]); // data
    assertEquals(BASE_TIME.epoch() + 3600, Bytes.getLong(pts.value().data(), 12));
    assertEquals(1, pts.value().data()[20]); // count
    assertEquals(2, pts.value().data()[21]); // type
    assertEquals(0, pts.value().data()[22]); // flags
    assertEquals(24, pts.value().data()[23]); // data
    
    pts.addColumn((byte) 2, new byte[] { 2 }, RollupUtils.buildAppendRollupValue(
        BASE_TIME.epoch() + (3600 * 2), (short) 3, interval, Bytes.fromInt(1024)));
    assertEquals(0, pts.value().offset());
    assertEquals(39, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(42, pts.value().data()[11]); // data
    assertEquals(BASE_TIME.epoch() + 3600, Bytes.getLong(pts.value().data(), 12));
    assertEquals(1, pts.value().data()[20]); // count
    assertEquals(2, pts.value().data()[21]); // type
    assertEquals(0, pts.value().data()[22]); // flags
    assertEquals(24, pts.value().data()[23]); // data
    assertEquals(BASE_TIME.epoch() + (3600 * 2), Bytes.getLong(pts.value().data(), 24));
    assertEquals(1, pts.value().data()[32]); // count
    assertEquals(2, pts.value().data()[33]); // type
    assertEquals(3, pts.value().data()[34]); // flags
    assertEquals(0, pts.value().data()[35]); // data
    assertEquals(0, pts.value().data()[36]); // data
    assertEquals(4, pts.value().data()[37]); // data
    assertEquals(0, pts.value().data()[38]); // data
    
    pts.addColumn((byte) 1, new byte[] { 1 }, RollupUtils.buildAppendRollupValue(
        BASE_TIME.epoch() + (3600 * 3), (short) 0xB, interval, 
        Bytes.fromInt(Float.floatToRawIntBits(42.5F))));
    assertEquals(0, pts.value().offset());
    assertEquals(54, pts.value().end());
    assertEquals(BASE_TIME.epoch() + (3600 * 3), Bytes.getLong(pts.value().data(), 39));
    assertEquals(1, pts.value().data()[47]); // count
    assertEquals(1, pts.value().data()[48]); // type
    assertEquals(0xB, pts.value().data()[49]); // flags
    assertEquals(42.5, Float.intBitsToFloat(Bytes.getInt(pts.value().data(), 50)), 0.001);
    
    pts.addColumn((byte) 0, new byte[] { 0 }, RollupUtils.buildAppendRollupValue(
        BASE_TIME.epoch() + (3600 * 4), (short) 0xF, interval, 
        Bytes.fromLong(Double.doubleToRawLongBits(24.75F))));
    assertEquals(0, pts.value().offset());
    assertEquals(73, pts.value().end());
    assertEquals(BASE_TIME.epoch() + (3600 * 4), Bytes.getLong(pts.value().data(), 54));
    assertEquals(1, pts.value().data()[62]); // count
    assertEquals(0, pts.value().data()[63]); // type
    assertEquals(0xF, pts.value().data()[64]); // flags
    assertEquals(24.75, Double.longBitsToDouble(Bytes.getLong(pts.value().data(), 65)), 0.001);
  }
  
  @Test
  public void addColumnMixed() throws Exception {
    Tsdb1xNumericSummaryPartialTimeSeries pts = new Tsdb1xNumericSummaryPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, interval);
    
    pts.addColumn((byte) 0, new byte[] { 0 }, RollupUtils.buildAppendRollupValue(
        BASE_TIME.epoch(), (short) 0, interval, new byte[] { 42 }));
    assertEquals(42, pts.idHash());
    assertSame(SET, pts.set());
    assertEquals(0, pts.value().offset());
    assertEquals(12, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(42, pts.value().data()[11]); // data
    
    pts.addColumn((byte) 2, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch() + 3600, (short) 0, 2, interval), new byte[] { 24 });
    assertEquals(0, pts.value().offset());
    assertEquals(24, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(42, pts.value().data()[11]); // data
    assertEquals(BASE_TIME.epoch() + 3600, Bytes.getLong(pts.value().data(), 12));
    assertEquals(1, pts.value().data()[20]); // count
    assertEquals(2, pts.value().data()[21]); // type
    assertEquals(0, pts.value().data()[22]); // flags
    assertEquals(24, pts.value().data()[23]); // data
    
    pts.addColumn((byte) 2, RollupUtils.buildStringRollupQualifier(
        "MIN", BASE_TIME.epoch() + (3600 * 2), (short) 3, interval), Bytes.fromInt(1024));
    assertEquals(0, pts.value().offset());
    assertEquals(39, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(42, pts.value().data()[11]); // data
    assertEquals(BASE_TIME.epoch() + 3600, Bytes.getLong(pts.value().data(), 12));
    assertEquals(1, pts.value().data()[20]); // count
    assertEquals(2, pts.value().data()[21]); // type
    assertEquals(0, pts.value().data()[22]); // flags
    assertEquals(24, pts.value().data()[23]); // data
    assertEquals(BASE_TIME.epoch() + (3600 * 2), Bytes.getLong(pts.value().data(), 24));
    assertEquals(1, pts.value().data()[32]); // count
    assertEquals(2, pts.value().data()[33]); // type
    assertEquals(3, pts.value().data()[34]); // flags
    assertEquals(0, pts.value().data()[35]); // data
    assertEquals(0, pts.value().data()[36]); // data
    assertEquals(4, pts.value().data()[37]); // data
    assertEquals(0, pts.value().data()[38]); // data
    
    pts.addColumn((byte) 1, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch() + (3600 * 3), (short) 0XB, 1, interval), 
        Bytes.fromInt(Float.floatToRawIntBits(42.5F)));
    assertEquals(0, pts.value().offset());
    assertEquals(54, pts.value().end());
    assertEquals(BASE_TIME.epoch() + (3600 * 3), Bytes.getLong(pts.value().data(), 39));
    assertEquals(1, pts.value().data()[47]); // count
    assertEquals(1, pts.value().data()[48]); // type
    assertEquals(0xB, pts.value().data()[49]); // flags
    assertEquals(42.5, Float.intBitsToFloat(Bytes.getInt(pts.value().data(), 50)), 0.001);
    
    pts.addColumn((byte) 0, new byte[] { 0 }, RollupUtils.buildAppendRollupValue(
        BASE_TIME.epoch() + (3600 * 4), (short) 0xF, interval, 
        Bytes.fromLong(Double.doubleToRawLongBits(24.75F))));
    assertEquals(0, pts.value().offset());
    assertEquals(73, pts.value().end());
    assertEquals(BASE_TIME.epoch() + (3600 * 4), Bytes.getLong(pts.value().data(), 54));
    assertEquals(1, pts.value().data()[62]); // count
    assertEquals(0, pts.value().data()[63]); // type
    assertEquals(0xF, pts.value().data()[64]); // flags
    assertEquals(24.75, Double.longBitsToDouble(Bytes.getLong(pts.value().data(), 65)), 0.001);
  }

  @Test
  public void addColumnExceptionsAndBadData() throws Exception {
    Tsdb1xNumericSummaryPartialTimeSeries pts = new Tsdb1xNumericSummaryPartialTimeSeries();
    try {
      // not set yet
      pts.addColumn((byte) 0, 
          NumericCodec.buildSecondQualifier(0, (short) 0), 
          new byte[] { 42 });
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    try {
      pts.reset(BASE_TIME, 42, pool, SET, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {  }
    
    pts.reset(BASE_TIME, 42, pool, SET, interval);
    try {
      pts.addColumn((byte) 0, 
          null, 
          new byte[] { 42 });
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    try {
      pts.addColumn((byte) 0, 
          NumericCodec.buildSecondQualifier(0, (short) 0), 
          null);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    // bad qualifier
    try {
      pts.addColumn((byte) 0,
          new byte[0], 
          new byte[] { 42 });
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    // bad value
    try {
      pts.addColumn((byte) 0, 
          NumericCodec.buildSecondQualifier(0, (short) 0), 
          new byte[] { });
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
  }
  
  @Test
  public void dedupeSortedSameType() throws Exception {
    Tsdb1xNumericSummaryPartialTimeSeries pts = new Tsdb1xNumericSummaryPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, interval);
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch(), (short) 0, 0, interval), new byte[] { 42 });
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch() + 3600, (short) 0, 0, interval), new byte[] { 24 });
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch() + (3600 * 2), (short) 0, 0, interval), new byte[] { 1 });
    assertEquals(0, pts.value().offset());
    assertEquals(36, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(42, pts.value().data()[11]); // data
    assertEquals(BASE_TIME.epoch() + 3600, Bytes.getLong(pts.value().data(), 12));
    assertEquals(1, pts.value().data()[20]); // count
    assertEquals(0, pts.value().data()[21]); // type
    assertEquals(0, pts.value().data()[22]); // flags
    assertEquals(24, pts.value().data()[23]); // data
    assertEquals(BASE_TIME.epoch() + (3600 * 2), Bytes.getLong(pts.value().data(), 24));
    assertEquals(1, pts.value().data()[32]); // count
    assertEquals(0, pts.value().data()[33]); // type
    assertEquals(0, pts.value().data()[34]); // flags
    assertEquals(1, pts.value().data()[35]); // data
    assertFalse(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(0, pts.value().offset());
    assertEquals(36, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(42, pts.value().data()[11]); // data
    assertEquals(BASE_TIME.epoch() + 3600, Bytes.getLong(pts.value().data(), 12));
    assertEquals(1, pts.value().data()[20]); // count
    assertEquals(0, pts.value().data()[21]); // type
    assertEquals(0, pts.value().data()[22]); // flags
    assertEquals(24, pts.value().data()[23]); // data
    assertEquals(BASE_TIME.epoch() + (3600 * 2), Bytes.getLong(pts.value().data(), 24));
    assertEquals(1, pts.value().data()[32]); // count
    assertEquals(0, pts.value().data()[33]); // type
    assertEquals(0, pts.value().data()[34]); // flags
    assertEquals(1, pts.value().data()[35]); // data
    assertFalse(pts.needs_repair);
  }
  
  @Test
  public void dedupeOrderSameType() throws Exception {
    Tsdb1xNumericSummaryPartialTimeSeries pts = new Tsdb1xNumericSummaryPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, interval);
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch() + (3600 * 2), (short) 0, 0, interval), new byte[] { 1 });
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch(), (short) 0, 0, interval), new byte[] { 42 });
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch() + (3600 * 3), (short) 0, 0, interval), new byte[] { -1 });
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch() + 3600, (short) 0, 0, interval), new byte[] { 24 });
    assertEquals(0, pts.value().offset());
    assertEquals(48, pts.value().end());
    assertEquals(BASE_TIME.epoch() + (3600 * 2), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(1, pts.value().data()[11]); // data
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 12));
    assertEquals(1, pts.value().data()[20]); // count
    assertEquals(0, pts.value().data()[21]); // type
    assertEquals(0, pts.value().data()[22]); // flags
    assertEquals(42, pts.value().data()[23]); // data
    assertEquals(BASE_TIME.epoch() + (3600 * 3), Bytes.getLong(pts.value().data(), 24));
    assertEquals(1, pts.value().data()[32]); // count
    assertEquals(0, pts.value().data()[33]); // type
    assertEquals(0, pts.value().data()[34]); // flags
    assertEquals(-1, pts.value().data()[35]); // data
    assertEquals(BASE_TIME.epoch() + 3600, Bytes.getLong(pts.value().data(), 36));
    assertEquals(1, pts.value().data()[44]); // count
    assertEquals(0, pts.value().data()[45]); // type
    assertEquals(0, pts.value().data()[46]); // flags
    assertEquals(24, pts.value().data()[47]); // data
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(0, pts.value().offset());
    assertEquals(48, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(42, pts.value().data()[11]); // data
    assertEquals(BASE_TIME.epoch() + 3600, Bytes.getLong(pts.value().data(), 12));
    assertEquals(1, pts.value().data()[20]); // count
    assertEquals(0, pts.value().data()[21]); // type
    assertEquals(0, pts.value().data()[22]); // flags
    assertEquals(24, pts.value().data()[23]); // data
    assertEquals(BASE_TIME.epoch() + (3600 * 2), Bytes.getLong(pts.value().data(), 24));
    assertEquals(1, pts.value().data()[32]); // count
    assertEquals(0, pts.value().data()[33]); // type
    assertEquals(0, pts.value().data()[34]); // flags
    assertEquals(1, pts.value().data()[35]); // data
    assertFalse(pts.needs_repair);
  }
  
  @Test
  public void dedupeLastSameType() throws Exception {
    Tsdb1xNumericSummaryPartialTimeSeries pts = new Tsdb1xNumericSummaryPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, interval);
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch() + (3600 * 2), (short) 0, 0, interval), new byte[] { 1 });
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch(), (short) 0, 0, interval), new byte[] { 42 });
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch() + (3600 * 2), (short) 0, 0, interval), new byte[] { -1 });
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch(), (short) 0, 0, interval), new byte[] { 24 });
    assertEquals(0, pts.value().offset());
    assertEquals(48, pts.value().end());
    assertEquals(BASE_TIME.epoch() + (3600 * 2), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(1, pts.value().data()[11]); // data
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 12));
    assertEquals(1, pts.value().data()[20]); // count
    assertEquals(0, pts.value().data()[21]); // type
    assertEquals(0, pts.value().data()[22]); // flags
    assertEquals(42, pts.value().data()[23]); // data
    assertEquals(BASE_TIME.epoch() + (3600 * 2), Bytes.getLong(pts.value().data(), 24));
    assertEquals(1, pts.value().data()[32]); // count
    assertEquals(0, pts.value().data()[33]); // type
    assertEquals(0, pts.value().data()[34]); // flags
    assertEquals(-1, pts.value().data()[35]); // data
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 36));
    assertEquals(1, pts.value().data()[44]); // count
    assertEquals(0, pts.value().data()[45]); // type
    assertEquals(0, pts.value().data()[46]); // flags
    assertEquals(24, pts.value().data()[47]); // data
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(0, pts.value().offset());
    assertEquals(24, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(24, pts.value().data()[11]); // data
    assertEquals(BASE_TIME.epoch() + (3600 * 2), Bytes.getLong(pts.value().data(), 12));
    assertEquals(1, pts.value().data()[20]); // count
    assertEquals(0, pts.value().data()[21]); // type
    assertEquals(0, pts.value().data()[22]); // flags
    assertEquals(-1, pts.value().data()[23]); // data
    assertFalse(pts.needs_repair);
  }
  
  @Test
  public void dedupeFirstSameType() throws Exception {
    Tsdb1xNumericSummaryPartialTimeSeries pts = new Tsdb1xNumericSummaryPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, interval);
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch() + (3600 * 2), (short) 0, 0, interval), new byte[] { 1 });
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch(), (short) 0, 0, interval), new byte[] { 42 });
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch() + (3600 * 2), (short) 0, 0, interval), new byte[] { -1 });
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch(), (short) 0, 0, interval), new byte[] { 24 });
    assertEquals(0, pts.value().offset());
    assertEquals(48, pts.value().end());
    assertEquals(BASE_TIME.epoch() + (3600 * 2), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(1, pts.value().data()[11]); // data
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 12));
    assertEquals(1, pts.value().data()[20]); // count
    assertEquals(0, pts.value().data()[21]); // type
    assertEquals(0, pts.value().data()[22]); // flags
    assertEquals(42, pts.value().data()[23]); // data
    assertEquals(BASE_TIME.epoch() + (3600 * 2), Bytes.getLong(pts.value().data(), 24));
    assertEquals(1, pts.value().data()[32]); // count
    assertEquals(0, pts.value().data()[33]); // type
    assertEquals(0, pts.value().data()[34]); // flags
    assertEquals(-1, pts.value().data()[35]); // data
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 36));
    assertEquals(1, pts.value().data()[44]); // count
    assertEquals(0, pts.value().data()[45]); // type
    assertEquals(0, pts.value().data()[46]); // flags
    assertEquals(24, pts.value().data()[47]); // data
    assertTrue(pts.needs_repair);
    
    pts.dedupe(true, false);
    assertEquals(0, pts.value().offset());
    assertEquals(24, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(42, pts.value().data()[11]); // data
    assertEquals(BASE_TIME.epoch() + (3600 * 2), Bytes.getLong(pts.value().data(), 12));
    assertEquals(1, pts.value().data()[20]); // count
    assertEquals(0, pts.value().data()[21]); // type
    assertEquals(0, pts.value().data()[22]); // flags
    assertEquals(1, pts.value().data()[23]); // data
    assertFalse(pts.needs_repair);
  }
  
  // This is currently a no-op for dedupe.
  @Test
  public void dedupeSortedDiffTypes() throws Exception {
    Tsdb1xNumericSummaryPartialTimeSeries pts = new Tsdb1xNumericSummaryPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, interval);
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch(), (short) 0, 0, interval), new byte[] { 42 });
    pts.addColumn((byte) 2, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch(), (short) 0, 2, interval), new byte[] { 24 });
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch() + (3600 * 2), (short) 0, 0, interval), new byte[] { 1 });
    pts.addColumn((byte) 2, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch() + (3600 * 2), (short) 0, 2, interval), new byte[] { -1 });
    assertEquals(0, pts.value().offset());
    assertEquals(48, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(42, pts.value().data()[11]); // data
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 12));
    assertEquals(1, pts.value().data()[20]); // count
    assertEquals(2, pts.value().data()[21]); // type
    assertEquals(0, pts.value().data()[22]); // flags
    assertEquals(24, pts.value().data()[23]); // data
    assertEquals(BASE_TIME.epoch() + (3600 * 2), Bytes.getLong(pts.value().data(), 24));
    assertEquals(1, pts.value().data()[32]); // count
    assertEquals(0, pts.value().data()[33]); // type
    assertEquals(0, pts.value().data()[34]); // flags
    assertEquals(1, pts.value().data()[35]); // data
    assertEquals(BASE_TIME.epoch() + (3600 * 2), Bytes.getLong(pts.value().data(), 36));
    assertEquals(1, pts.value().data()[44]); // count
    assertEquals(2, pts.value().data()[45]); // type
    assertEquals(0, pts.value().data()[46]); // flags
    assertEquals(-1, pts.value().data()[47]); // data
    assertFalse(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(0, pts.value().offset());
    assertEquals(48, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(42, pts.value().data()[11]); // data
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 12));
    assertEquals(1, pts.value().data()[20]); // count
    assertEquals(2, pts.value().data()[21]); // type
    assertEquals(0, pts.value().data()[22]); // flags
    assertEquals(24, pts.value().data()[23]); // data
    assertEquals(BASE_TIME.epoch() + (3600 * 2), Bytes.getLong(pts.value().data(), 24));
    assertEquals(1, pts.value().data()[32]); // count
    assertEquals(0, pts.value().data()[33]); // type
    assertEquals(0, pts.value().data()[34]); // flags
    assertEquals(1, pts.value().data()[35]); // data
    assertEquals(BASE_TIME.epoch() + (3600 * 2), Bytes.getLong(pts.value().data(), 36));
    assertEquals(1, pts.value().data()[44]); // count
    assertEquals(2, pts.value().data()[45]); // type
    assertEquals(0, pts.value().data()[46]); // flags
    assertEquals(-1, pts.value().data()[47]); // data
    assertFalse(pts.needs_repair);
  }
  
  @Test
  public void dedupedDiffTypes() throws Exception {
    Tsdb1xNumericSummaryPartialTimeSeries pts = new Tsdb1xNumericSummaryPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, interval);
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch(), (short) 0, 0, interval), new byte[] { 42 });
    pts.addColumn((byte) 2, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch() + (3600 * 2), (short) 0, 2, interval), new byte[] { -1 });
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch() + (3600 * 2), (short) 0, 0, interval), new byte[] { 1 });
    pts.addColumn((byte) 2, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch(), (short) 0, 2, interval), new byte[] { 24 });
    assertEquals(0, pts.value().offset());
    assertEquals(48, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(42, pts.value().data()[11]); // data
    assertEquals(BASE_TIME.epoch() + (3600 * 2), Bytes.getLong(pts.value().data(), 12));
    assertEquals(1, pts.value().data()[20]); // count
    assertEquals(2, pts.value().data()[21]); // type
    assertEquals(0, pts.value().data()[22]); // flags
    assertEquals(-1, pts.value().data()[23]); // data
    assertEquals(BASE_TIME.epoch() + (3600 * 2), Bytes.getLong(pts.value().data(), 24));
    assertEquals(1, pts.value().data()[32]); // count
    assertEquals(0, pts.value().data()[33]); // type
    assertEquals(0, pts.value().data()[34]); // flags
    assertEquals(1, pts.value().data()[35]); // data
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 36));
    assertEquals(1, pts.value().data()[44]); // count
    assertEquals(2, pts.value().data()[45]); // type
    assertEquals(0, pts.value().data()[46]); // flags
    assertEquals(24, pts.value().data()[47]); // data
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(0, pts.value().offset());
    assertEquals(30, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(2, pts.value().data()[8]); // count !!! TWO!
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(42, pts.value().data()[11]); // data
    // next type
    assertEquals(2, pts.value().data()[12]); // type
    assertEquals(0, pts.value().data()[13]); // flags
    assertEquals(24, pts.value().data()[14]); // data
    assertEquals(BASE_TIME.epoch() + (3600 * 2), Bytes.getLong(pts.value().data(), 15));
    assertEquals(2, pts.value().data()[23]); // count !!! TWO!
    assertEquals(0, pts.value().data()[24]); // type
    assertEquals(0, pts.value().data()[25]); // flags
    assertEquals(1, pts.value().data()[26]); // data
    // next type
    assertEquals(2, pts.value().data()[27]); // type
    assertEquals(0, pts.value().data()[28]); // flags
    assertEquals(-1, pts.value().data()[29]); // data
    assertFalse(pts.needs_repair);
  }

  @Test
  public void growOnAdd() throws Exception {
    pool = mock(ObjectPool.class);
    final PooledObject obj = mock(PooledObject.class);
    byte[] arr = new byte[14];
    when(obj.object()).thenReturn(arr);
    when(pool.claim()).thenAnswer(new Answer<PooledObject>() {
      @Override
      public PooledObject answer(InvocationOnMock invocation) throws Throwable {
        return obj;
      }
    });
    
    Tsdb1xNumericSummaryPartialTimeSeries pts = new Tsdb1xNumericSummaryPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, interval);
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch(), (short) 0, 0, interval), new byte[] { 42 });
    PooledObject array = pts.pooled_array;
    verify(obj, never()).release();
    assertEquals(14, ((byte[]) array.object()).length);
    
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch() + 3600, (short) 0, 0, interval), new byte[] { 42 });
    verify(obj, times(1)).release();
    assertNotSame(array, pts.pooled_array);
    assertEquals(76, ((byte[]) pts.pooled_array.object()).length);
    
    // grow
    for (int i = 2; i < 8; i++) {
      pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
          BASE_TIME.epoch() + (3600 * i), (short) 0, 0, interval), new byte[] { 42 });
    }
    assertEquals(136, ((byte[]) pts.pooled_array.object()).length);
  }
  
  @Test
  public void resetNoPool() throws Exception {
    Tsdb1xNumericSummaryPartialTimeSeries pts = new Tsdb1xNumericSummaryPartialTimeSeries();
    pts.reset(BASE_TIME, 42, null, SET, interval);
    pts.addColumn((byte) 0, RollupUtils.buildRollupQualifier(
        BASE_TIME.epoch(), (short) 0, 0, interval), new byte[] { 42 });
    assertEquals(42, pts.idHash());
    assertSame(SET, pts.set());
    assertEquals(0, pts.value().offset());
    assertEquals(12, pts.value().end());
    assertEquals(BASE_TIME.epoch(), Bytes.getLong(pts.value().data(), 0));
    assertEquals(1, pts.value().data()[8]); // count
    assertEquals(0, pts.value().data()[9]); // type
    assertEquals(0, pts.value().data()[10]); // flags
    assertEquals(42, pts.value().data()[11]); // data
  }
}
