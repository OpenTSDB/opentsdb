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
import net.opentsdb.data.types.numeric.NumericLongArrayType;
import net.opentsdb.exceptions.IllegalDataException;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.storage.schemas.tsdb1x.NumericCodec;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.NumericCodec.OffsetResolution;
import net.opentsdb.utils.Bytes;

public class TestTsdb1xNumericPartialTimeSeries {
  private static final TimeStamp BASE_TIME = new SecondTimeStamp(1514764800);
  private static final byte[] APPEND_Q = 
      new byte[] { Schema.APPENDS_PREFIX, 0, 0 };
  private static final PartialTimeSeriesSet SET = mock(PartialTimeSeriesSet.class);
  private ObjectPool pool;
  
  @Before
  public void before() throws Exception {
    pool = mock(ObjectPool.class);
    when(pool.claim()).thenAnswer(new Answer<PooledObject>() {
      @Override
      public PooledObject answer(InvocationOnMock invocation) throws Throwable {
        PooledObject obj = mock(PooledObject.class);
        long[] array = new long[4096];
        when(obj.object()).thenReturn(array);
        return obj;
      }
    });
  }
  
  @Test
  public void addColumnPuts() throws Exception {
    Tsdb1xNumericPartialTimeSeries pts = new Tsdb1xNumericPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn((byte) 0, 
        NumericCodec.buildSecondQualifier(0, (short) 0), 
        new byte[] { 42 });
    assertEquals(42, pts.idHash());
    assertSame(SET, pts.set());
    assertEquals(0, pts.value().offset());
    assertEquals(2, pts.value().end());
    assertEquals(BASE_TIME.epoch(), pts.value().data()[0]); // no flags here
    assertEquals(42, pts.value().data()[1]);
    assertEquals(0, pts.value().data()[2]); // no terminal
    assertFalse(pts.needs_repair);
    
    pts.addColumn((byte) 0,
        NumericCodec.buildSecondQualifier(60, (short) 0), 
        new byte[] { 24 });
    assertEquals(0, pts.value().offset());
    assertEquals(4, pts.value().end());
    assertEquals(BASE_TIME.epoch(), pts.value().data()[0]); // no flags here
    assertEquals(42, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 60, pts.value().data()[2]); // no flags here
    assertEquals(24, pts.value().data()[3]);
    assertEquals(0, pts.value().data()[4]); // no terminal
    assertFalse(pts.needs_repair);
    
    // out of order
    pts.addColumn((byte) 0,
        NumericCodec.buildSecondQualifier(30, (short) 0), 
        new byte[] { 1 });
    assertEquals(0, pts.value().offset());
    assertEquals(6, pts.value().end());
    assertEquals(BASE_TIME.epoch(), pts.value().data()[0]); // no flags here
    assertEquals(42, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 60, pts.value().data()[2]); // no flags here
    assertEquals(24, pts.value().data()[3]);
    assertEquals(BASE_TIME.epoch() + 30, pts.value().data()[4]); // no flags here
    assertEquals(1, pts.value().data()[5]);
    assertEquals(0, pts.value().data()[6]); // no terminal
    assertTrue(pts.needs_repair);
    
    // add ms
    pts.addColumn((byte) 0,
        NumericCodec.buildMsQualifier(500, (short) 0), 
        new byte[] { 2 });
    assertEquals(0, pts.value().offset());
    assertEquals(8, pts.value().end());
    assertEquals(BASE_TIME.epoch(), pts.value().data()[0]); // no flags here
    assertEquals(42, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 60, pts.value().data()[2]); // no flags here
    assertEquals(24, pts.value().data()[3]);
    assertEquals(BASE_TIME.epoch() + 30, pts.value().data()[4]); // no flags here
    assertEquals(1, pts.value().data()[5]);
    assertEquals((BASE_TIME.msEpoch() + 500) | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[6]); // ms flag
    assertEquals(2, pts.value().data()[7]);
    assertEquals(0, pts.value().data()[8]); // no terminal
    assertTrue(pts.needs_repair);
    
    // add nano
    pts.addColumn((byte) 0,
        NumericCodec.buildNanoQualifier(25000, (short) 0), 
        new byte[] { -1 });
    assertEquals(0, pts.value().offset());
    assertEquals(11, pts.value().end());
    assertEquals(BASE_TIME.epoch(), pts.value().data()[0]); // no flags here
    assertEquals(42, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 60, pts.value().data()[2]); // no flags here
    assertEquals(24, pts.value().data()[3]);
    assertEquals(BASE_TIME.epoch() + 30, pts.value().data()[4]); // no flags here
    assertEquals(1, pts.value().data()[5]);
    assertEquals((BASE_TIME.msEpoch() + 500) | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[6]); // ms flag
    assertEquals(2, pts.value().data()[7]);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[8]); // nano flag
    assertEquals(25000, pts.value().data()[9]); // nanos
    assertEquals(-1, pts.value().data()[10]);
    assertEquals(0, pts.value().data()[11]); // no terminal
    assertTrue(pts.needs_repair);
    
    // floats
    pts.addColumn((byte) 0,
        NumericCodec.buildSecondQualifier(300, (short) (NumericCodec.FLAG_FLOAT | (short) 3)), 
        Bytes.fromInt(Float.floatToIntBits(0.5F)));
    assertEquals(0, pts.value().offset());
    assertEquals(13, pts.value().end());
    assertEquals((BASE_TIME.epoch() + 300) | NumericLongArrayType.FLOAT_FLAG, pts.value().data()[11]);
    assertEquals(Double.doubleToRawLongBits(0.5F), pts.value().data()[12]);
    assertEquals(0, pts.value().data()[13]); // no terminal
    assertTrue(pts.needs_repair);
    
    pts.addColumn((byte) 0,
        NumericCodec.buildSecondQualifier(900, (short) (NumericCodec.FLAG_FLOAT | (short) 7)), 
        Bytes.fromLong(Double.doubleToRawLongBits(1.42)));
    assertEquals(0, pts.value().offset());
    assertEquals(15, pts.value().end());
    assertEquals((BASE_TIME.epoch() + 900) | NumericLongArrayType.FLOAT_FLAG, pts.value().data()[13]);
    assertEquals(Double.doubleToRawLongBits(1.42), pts.value().data()[14]);
    assertEquals(0, pts.value().data()[15]); // no terminal
    assertTrue(pts.needs_repair);
    
    // add ms double
    pts.addColumn((byte) 0,
        NumericCodec.buildMsQualifier(500000, (short) (NumericCodec.FLAG_FLOAT | (short) 7)), 
        Bytes.fromLong(Double.doubleToRawLongBits(42.5)));
    assertEquals(0, pts.value().offset());
    assertEquals(17, pts.value().end());
    assertEquals((BASE_TIME.msEpoch() + 500000) | NumericLongArrayType.FLOAT_FLAG | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[15]); // ms and float flag
    assertEquals(Double.doubleToRawLongBits(42.5), pts.value().data()[16]);
    assertEquals(0, pts.value().data()[17]); // no terminal
    assertTrue(pts.needs_repair);
    
    // add ns double
    pts.addColumn((byte) 0,
        NumericCodec.buildNanoQualifier((1800L * 1000L * 1000L * 1000L) + 750L, (short) (NumericCodec.FLAG_FLOAT | (short) 7)), 
        Bytes.fromLong(Double.doubleToRawLongBits(0.005)));
    assertEquals(0, pts.value().offset());
    assertEquals(20, pts.value().end());
    assertEquals(((BASE_TIME.epoch() + 1800)| NumericLongArrayType.FLOAT_FLAG | NumericLongArrayType.NANOSECOND_FLAG), pts.value().data()[17]); // ns and float flag
    assertEquals(750, pts.value().data()[18]);
    assertEquals(Double.doubleToRawLongBits(0.005), pts.value().data()[19]);
    assertEquals(0, pts.value().data()[20]); // no terminal
    assertTrue(pts.needs_repair);
    
    verify(pool, times(1)).claim();
  }

  @Test
  public void addColumnAppends() throws Exception {
    Tsdb1xNumericPartialTimeSeries pts = new Tsdb1xNumericPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX,
        APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 42));
    assertEquals(42, pts.idHash());
    assertSame(SET, pts.set());
    assertEquals(0, pts.value().offset());
    assertEquals(2, pts.value().end());
    assertEquals(BASE_TIME.epoch(), pts.value().data()[0]); // no flags here
    assertEquals(42, pts.value().data()[1]);
    assertEquals(0, pts.value().data()[2]); // no terminal
    assertFalse(pts.needs_repair);
    
    pts.addColumn(Schema.APPENDS_PREFIX,
        APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 24));
    assertEquals(0, pts.value().offset());
    assertEquals(4, pts.value().end());
    assertEquals(BASE_TIME.epoch(), pts.value().data()[0]); // no flags here
    assertEquals(42, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 30, pts.value().data()[2]); // no flags here
    assertEquals(24, pts.value().data()[3]);
    assertEquals(0, pts.value().data()[4]); // no terminal
    assertFalse(pts.needs_repair);
    
    pts.addColumn(Schema.APPENDS_PREFIX,
        APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1));
    assertEquals(0, pts.value().offset());
    assertEquals(6, pts.value().end());
    assertEquals(BASE_TIME.epoch(), pts.value().data()[0]); // no flags here
    assertEquals(42, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 30, pts.value().data()[2]); // no flags here
    assertEquals(24, pts.value().data()[3]);
    assertEquals((BASE_TIME.msEpoch() + 500) | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[4]); // ms flag
    assertEquals(1, pts.value().data()[5]);
    assertEquals(0, pts.value().data()[6]); // no terminal
    assertTrue(pts.needs_repair);
    
    pts.addColumn(Schema.APPENDS_PREFIX, 
        APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 25000, -1));
    assertEquals(0, pts.value().offset());
    assertEquals(9, pts.value().end());
    assertEquals(BASE_TIME.epoch(), pts.value().data()[0]); // no flags here
    assertEquals(42, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 30, pts.value().data()[2]); // no flags here
    assertEquals(24, pts.value().data()[3]);
    assertEquals((BASE_TIME.msEpoch() + 500) | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[4]); // ms flag
    assertEquals(1, pts.value().data()[5]);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[6]); // Nanos flag
    assertEquals(25000, pts.value().data()[7]);
    assertEquals(-1, pts.value().data()[8]);
    assertEquals(0, pts.value().data()[9]); // no terminal
    assertTrue(pts.needs_repair);
    
    // floats
    pts.addColumn(Schema.APPENDS_PREFIX,
        APPEND_Q,
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 0.5));
    assertEquals(0, pts.value().offset());
    assertEquals(11, pts.value().end());
    assertEquals((BASE_TIME.epoch() + 60) | NumericLongArrayType.FLOAT_FLAG, pts.value().data()[9]);
    assertEquals(Double.doubleToRawLongBits(0.5F), pts.value().data()[10]);
    assertEquals(0, pts.value().data()[11]); // no terminal
    assertTrue(pts.needs_repair);
    
    pts.addColumn(Schema.APPENDS_PREFIX,
        APPEND_Q,
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 1.42));
    assertEquals(0, pts.value().offset());
    assertEquals(13, pts.value().end());
    assertEquals((BASE_TIME.epoch() + 120) | NumericLongArrayType.FLOAT_FLAG, pts.value().data()[11]);
    assertEquals(Double.doubleToRawLongBits(1.42), pts.value().data()[12]);
    assertEquals(0, pts.value().data()[13]); // no terminal
    assertTrue(pts.needs_repair);
    
    // add ms double
    pts.addColumn(Schema.APPENDS_PREFIX, 
        APPEND_Q,
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500000, 42.5));
    assertEquals(0, pts.value().offset());
    assertEquals(15, pts.value().end());
    assertEquals((BASE_TIME.msEpoch() + 500000) | NumericLongArrayType.FLOAT_FLAG | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[13]); // ms and float flag
    assertEquals(Double.doubleToRawLongBits(42.5), pts.value().data()[14]);
    assertEquals(0, pts.value().data()[15]); // no terminal
    assertTrue(pts.needs_repair);
    
    // add ns double
    pts.addColumn(Schema.APPENDS_PREFIX, 
        APPEND_Q,
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, (1800L * 1000L * 1000L * 1000L) + 750L, 0.005));
    assertEquals(0, pts.value().offset());
    assertEquals(18, pts.value().end());
    assertEquals(((BASE_TIME.epoch() + 1800)| NumericLongArrayType.FLOAT_FLAG | NumericLongArrayType.NANOSECOND_FLAG), pts.value().data()[15]); // ns and float flag
    assertEquals(750, pts.value().data()[16]);
    assertEquals(Double.doubleToRawLongBits(0.005), pts.value().data()[17]);
    assertEquals(0, pts.value().data()[18]); // no terminal
    assertTrue(pts.needs_repair);
    
    // multiples
    pts.addColumn(Schema.APPENDS_PREFIX, 
        APPEND_Q, 
        com.google.common.primitives.Bytes.concat(
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 2600, 5),
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 2660, 6),
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 2720, 7)));
    assertEquals(0, pts.value().offset());
    assertEquals(24, pts.value().end());
    assertEquals(BASE_TIME.epoch() + 2600, pts.value().data()[18]);
    assertEquals(5, pts.value().data()[19]);
    assertEquals(BASE_TIME.epoch() + 2660, pts.value().data()[20]);
    assertEquals(6, pts.value().data()[21]);
    assertEquals(BASE_TIME.epoch() + 2720, pts.value().data()[22]);
    assertEquals(7, pts.value().data()[23]);
    assertEquals(0, pts.value().data()[24]); // no terminal
    assertTrue(pts.needs_repair);
    
    verify(pool, times(1)).claim();
  }

  @Test
  public void addColumnMixed() throws Exception {
    Tsdb1xNumericPartialTimeSeries pts = new Tsdb1xNumericPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX,
        APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 42));
    assertEquals(42, pts.idHash());
    assertSame(SET, pts.set());
    assertEquals(0, pts.value().offset());
    assertEquals(2, pts.value().end());
    assertEquals(BASE_TIME.epoch(), pts.value().data()[0]); // no flags here
    assertEquals(42, pts.value().data()[1]);
    assertEquals(0, pts.value().data()[2]); // no terminal
    assertFalse(pts.needs_repair);
    
    pts.addColumn((byte) 0, 
        NumericCodec.buildSecondQualifier(60, (short) 0), 
        new byte[] { 24 });
    assertEquals(0, pts.value().offset());
    assertEquals(4, pts.value().end());
    assertEquals(BASE_TIME.epoch(), pts.value().data()[0]); // no flags here
    assertEquals(42, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 60, pts.value().data()[2]); // no flags here
    assertEquals(24, pts.value().data()[3]);
    assertEquals(0, pts.value().data()[4]); // no terminal
    assertFalse(pts.needs_repair);
    
    pts.addColumn(Schema.APPENDS_PREFIX, 
        APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 120_500, 1));
    assertEquals(0, pts.value().offset());
    assertEquals(6, pts.value().end());
    assertEquals(BASE_TIME.epoch(), pts.value().data()[0]); // no flags here
    assertEquals(42, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 60, pts.value().data()[2]); // no flags here
    assertEquals(24, pts.value().data()[3]);
    assertEquals((BASE_TIME.msEpoch() + 120500) | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[4]);
    assertEquals(1, pts.value().data()[5]);
    assertEquals(0, pts.value().data()[6]); // no terminal
    assertFalse(pts.needs_repair);
    
    pts.addColumn((byte) 0, 
        NumericCodec.buildNanoQualifier(180_000_025_000L, (short) 0), 
        new byte[] { -1 });
    assertEquals(0, pts.value().offset());
    assertEquals(9, pts.value().end());
    assertEquals(BASE_TIME.epoch(), pts.value().data()[0]); // no flags here
    assertEquals(42, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 60, pts.value().data()[2]); // no flags here
    assertEquals(24, pts.value().data()[3]);
    assertEquals((BASE_TIME.msEpoch() + 120500) | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[4]);
    assertEquals(1, pts.value().data()[5]);
    assertEquals(BASE_TIME.epoch() + 180 | NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[6]);
    assertEquals(25_000, pts.value().data()[7]);
    assertEquals(-1, pts.value().data()[8]);
    assertEquals(0, pts.value().data()[9]); // no terminal
    assertFalse(pts.needs_repair);
  }
  
  @Test
  public void addColumnExceptionsAndBadData() throws Exception {
    Tsdb1xNumericPartialTimeSeries pts = new Tsdb1xNumericPartialTimeSeries();
    
    try {
      // not set yet
      pts.addColumn((byte) 0, 
          NumericCodec.buildSecondQualifier(0, (short) 0), 
          new byte[] { 42 });
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    pts.reset(BASE_TIME, 42, pool, SET, null);
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
  public void addColumnCompactedSeconds() throws Exception {
    byte[] compacted_value = com.google.common.primitives.Bytes.concat(
        NumericCodec.vleEncodeLong(42), 
        NumericCodec.vleEncodeLong(24),
        NumericCodec.vleEncodeLong(1),
        NumericCodec.vleEncodeLong(-1),
        new byte[1]
    );
    byte[] compacted_qualifier = com.google.common.primitives.Bytes.concat(
        NumericCodec.buildSecondQualifier(0, (short) 0),
        NumericCodec.buildSecondQualifier(60, (short) 0),
        NumericCodec.buildSecondQualifier(120, (short) 0),
        NumericCodec.buildSecondQualifier(180, (short) 0)
    );
    
    Tsdb1xNumericPartialTimeSeries pts = new Tsdb1xNumericPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn((byte) 0, 
        compacted_qualifier, 
        compacted_value);
    assertEquals(42, pts.idHash());
    assertSame(SET, pts.set());
    assertEquals(0, pts.value().offset());
    assertEquals(8, pts.value().end());
    assertEquals(BASE_TIME.epoch(), pts.value().data()[0]); // no flags here
    assertEquals(42, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 60, pts.value().data()[2]); // no flags here
    assertEquals(24, pts.value().data()[3]);
    assertEquals(BASE_TIME.epoch() + 120, pts.value().data()[4]); // no flags here
    assertEquals(1, pts.value().data()[5]);
    assertEquals(BASE_TIME.epoch() + 180, pts.value().data()[6]); // no flags here
    assertEquals(-1, pts.value().data()[7]);
    assertEquals(0, pts.value().data()[8]); // no terminal
    assertFalse(pts.needs_repair);
  }
  
  @Test
  public void addColumnCompactedMixed() throws Exception {
    byte[] compacted_value = com.google.common.primitives.Bytes.concat(
        NumericCodec.vleEncodeLong(42), 
        NumericCodec.vleEncodeLong(24),
        NumericCodec.vleEncodeLong(1),
        NumericCodec.vleEncodeLong(-1),
        new byte[] { NumericCodec.MS_MIXED_COMPACT }
    );
    byte[] compacted_qualifier = com.google.common.primitives.Bytes.concat(
        NumericCodec.buildSecondQualifier(0, (short) 0),
        NumericCodec.buildMsQualifier(60000, (short) 0),
        NumericCodec.buildSecondQualifier(120, (short) 0),
        NumericCodec.buildMsQualifier(180000, (short) 0)
    );
    
    Tsdb1xNumericPartialTimeSeries pts = new Tsdb1xNumericPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn((byte) 0, 
        compacted_qualifier, 
        compacted_value);
    assertEquals(42, pts.idHash());
    assertSame(SET, pts.set());
    assertEquals(0, pts.value().offset());
    assertEquals(8, pts.value().end());
    assertEquals(BASE_TIME.epoch(), pts.value().data()[0]); // no flags here
    assertEquals(42, pts.value().data()[1]);
    assertEquals((BASE_TIME.msEpoch() + 60000) | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[2]); // ms flag
    assertEquals(24, pts.value().data()[3]);
    assertEquals(BASE_TIME.epoch() + 120, pts.value().data()[4]); // no flags here
    assertEquals(1, pts.value().data()[5]);
    assertEquals((BASE_TIME.msEpoch() + 180000) | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[6]); // ms flag
    assertEquals(-1, pts.value().data()[7]);
    assertEquals(0, pts.value().data()[8]); // no terminal
    assertFalse(pts.needs_repair);
  }
  
  @Test
  public void addColumnFixOldTSDIssues() throws Exception {
    Tsdb1xNumericPartialTimeSeries pts = new Tsdb1xNumericPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn((byte) 0, 
        NumericCodec.buildSecondQualifier(500, (short) 1),
        net.opentsdb.utils.Bytes.fromLong(42));
    assertEquals(42, pts.idHash());
    assertSame(SET, pts.set());
    assertEquals(0, pts.value().offset());
    assertEquals(2, pts.value().end());
    assertEquals(BASE_TIME.epoch() + 500, pts.value().data()[0]); // no flags here
    assertEquals(42, pts.value().data()[1]);
    assertEquals(0, pts.value().data()[2]); // no terminal
    assertFalse(pts.needs_repair);
    
    pts.addColumn((byte) 0, 
        NumericCodec.buildSecondQualifier(560, (short) 0),
        net.opentsdb.utils.Bytes.fromLong(24));
    assertEquals(0, pts.value().offset());
    assertEquals(4, pts.value().end());
    assertEquals(BASE_TIME.epoch() + 560, pts.value().data()[2]); // no flags here
    assertEquals(24, pts.value().data()[3]);
    assertEquals(0, pts.value().data()[4]); // no terminal
    assertFalse(pts.needs_repair);
    
    pts.addColumn((byte) 0, 
        NumericCodec.buildSecondQualifier(620, (short) 7),
        net.opentsdb.utils.Bytes.fromInt(1));
    assertEquals(0, pts.value().offset());
    assertEquals(6, pts.value().end());
    assertEquals(BASE_TIME.epoch() + 620, pts.value().data()[4]); // no flags here
    assertEquals(1, pts.value().data()[5]);
    assertEquals(0, pts.value().data()[6]); // no terminal
    assertFalse(pts.needs_repair);
    
    pts.reset(BASE_TIME, 42, pool, SET, null); // reset!
    pts.addColumn((byte) 0, 
        NumericCodec.buildSecondQualifier(500, (short) (7 | NumericCodec.FLAG_FLOAT)),
        net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F)));
    assertEquals(0, pts.value().offset());
    assertEquals(2, pts.value().end());
    assertEquals(BASE_TIME.epoch() + 500 | NumericLongArrayType.FLOAT_FLAG, pts.value().data()[0]); // float
    assertEquals(Double.doubleToRawLongBits(42.5), pts.value().data()[1]);
    assertFalse(pts.needs_repair);

    // old style incorrect length floating point value
    pts.addColumn((byte) 0, 
        NumericCodec.buildSecondQualifier(560, (short) (3 | NumericCodec.FLAG_FLOAT)),
        com.google.common.primitives.Bytes.concat(new byte[4], 
            net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(24.5F))));
    assertEquals(0, pts.value().offset());
    assertEquals(4, pts.value().end());
    assertEquals(BASE_TIME.epoch() + 560 | NumericLongArrayType.FLOAT_FLAG, pts.value().data()[2]); // float
    assertEquals(Double.doubleToRawLongBits(24.5), pts.value().data()[3]);
    assertFalse(pts.needs_repair);
  }

  @Test
  public void dedupeSortedSeconds() throws Exception {
    Tsdb1xNumericPartialTimeSeries pts = new Tsdb1xNumericPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, 
        APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, 
        APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, 
        APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, 1));
    assertEquals(42, pts.idHash());
    assertSame(SET, pts.set());
    assertEquals(0, pts.value().offset());
    assertEquals(6, pts.value().end());
    assertEquals(BASE_TIME.epoch() + 60, pts.value().data()[0]); // no flags here
    assertEquals(42, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 120, pts.value().data()[2]); // no flags here
    assertEquals(24, pts.value().data()[3]);
    assertEquals(BASE_TIME.epoch() + 180, pts.value().data()[4]); // no flags here
    assertEquals(1, pts.value().data()[5]);
    assertEquals(0, pts.value().data()[6]);
    assertFalse(pts.needs_repair);

    pts.dedupe(false, false);
    assertEquals(0, pts.value().offset());
    assertEquals(6, pts.value().end());
    assertEquals(BASE_TIME.epoch() + 60, pts.value().data()[0]); // no flags here
    assertEquals(42, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 120, pts.value().data()[2]); // no flags here
    assertEquals(24, pts.value().data()[3]);
    assertEquals(BASE_TIME.epoch() + 180, pts.value().data()[4]); // no flags here
    assertEquals(1, pts.value().data()[5]);
    assertFalse(pts.needs_repair);
  }
  
  @Test
  public void dedupeSeconds() throws Exception {
    Tsdb1xNumericPartialTimeSeries pts = new Tsdb1xNumericPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(0, pts.value().offset());
    assertEquals(8, pts.value().end());
    assertEquals(BASE_TIME.epoch() + 30, pts.value().data()[0]); // no flags here
    assertEquals(2, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 60, pts.value().data()[2]); // no flags here
    assertEquals(42, pts.value().data()[3]);
    assertEquals(BASE_TIME.epoch() + 120, pts.value().data()[4]); // no flags here
    assertEquals(24, pts.value().data()[5]);
    assertEquals(BASE_TIME.epoch() + 180, pts.value().data()[6]); // no flags here
    assertEquals(1, pts.value().data()[7]);
    assertFalse(pts.needs_repair);

    // reset to reverse in the tree
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, true);
    assertEquals(0, pts.value().offset());
    assertEquals(8, pts.value().end());
    assertEquals(BASE_TIME.epoch() + 180, pts.value().data()[0]); // no flags here
    assertEquals(1, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 120, pts.value().data()[2]); // no flags here
    assertEquals(24, pts.value().data()[3]);
    assertEquals(BASE_TIME.epoch() + 60, pts.value().data()[4]); // no flags here
    assertEquals(42, pts.value().data()[5]);
    assertEquals(BASE_TIME.epoch() + 30, pts.value().data()[6]); // no flags here
    assertEquals(2, pts.value().data()[7]);
    assertFalse(pts.needs_repair);
    
    // consecutive dupe
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(true, false);
    assertEquals(0, pts.value().offset());
    assertEquals(6, pts.value().end());
    assertEquals(BASE_TIME.epoch() + 30, pts.value().data()[0]); // no flags here
    assertEquals(2, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 60, pts.value().data()[2]); // no flags here
    assertEquals(42, pts.value().data()[3]);
    assertEquals(BASE_TIME.epoch() + 120, pts.value().data()[4]); // no flags here
    assertEquals(24, pts.value().data()[5]);
    assertFalse(pts.needs_repair);
    
    // keep first
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(0, pts.value().offset());
    assertEquals(6, pts.value().end());
    assertEquals(BASE_TIME.epoch() + 30, pts.value().data()[0]); // no flags here
    assertEquals(2, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 60, pts.value().data()[2]); // no flags here
    assertEquals(1, pts.value().data()[3]);
    assertEquals(BASE_TIME.epoch() + 120, pts.value().data()[4]); // no flags here
    assertEquals(24, pts.value().data()[5]);
    assertFalse(pts.needs_repair);
    
    // non-consecutive dupe
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(true, false);
    assertEquals(0, pts.value().offset());
    assertEquals(6, pts.value().end());
    assertEquals(BASE_TIME.epoch() + 30, pts.value().data()[0]); // no flags here
    assertEquals(2, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 60, pts.value().data()[2]); // no flags here
    assertEquals(42, pts.value().data()[3]);
    assertEquals(BASE_TIME.epoch() + 120, pts.value().data()[4]); // no flags here
    assertEquals(24, pts.value().data()[5]);
    assertFalse(pts.needs_repair);
    
    // keep first
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(0, pts.value().offset());
    assertEquals(6, pts.value().end());
    assertEquals(BASE_TIME.epoch() + 30, pts.value().data()[0]); // no flags here
    assertEquals(2, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 60, pts.value().data()[2]); // no flags here
    assertEquals(42, pts.value().data()[3]);
    assertEquals(BASE_TIME.epoch() + 120, pts.value().data()[4]); // no flags here
    assertEquals(1, pts.value().data()[5]);
    assertFalse(pts.needs_repair);
  }

  @Test
  public void dedupeMilliSeconds() throws Exception {
    Tsdb1xNumericPartialTimeSeries pts = new Tsdb1xNumericPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 1000, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 2));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(0, pts.value().offset());
    assertEquals(8, pts.value().end());
    assertEquals(BASE_TIME.msEpoch() + 250 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[0]);
    assertEquals(2, pts.value().data()[1]);
    assertEquals(BASE_TIME.msEpoch() + 500 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[2]);
    assertEquals(42, pts.value().data()[3]);
    assertEquals(BASE_TIME.msEpoch() + 750 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[4]);
    assertEquals(24, pts.value().data()[5]);
    assertEquals(BASE_TIME.msEpoch() + 1000 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[6]);
    assertEquals(1, pts.value().data()[7]);
    assertFalse(pts.needs_repair);

    // reset to reverse in the tree
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 1000, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 2));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, true);
    assertEquals(0, pts.value().offset());
    assertEquals(8, pts.value().end());
    assertEquals(BASE_TIME.msEpoch() + 1000 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[0]);
    assertEquals(1, pts.value().data()[1]);
    assertEquals(BASE_TIME.msEpoch() + 750 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[2]);
    assertEquals(24, pts.value().data()[3]);
    assertEquals(BASE_TIME.msEpoch() + 500 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[4]);
    assertEquals(42, pts.value().data()[5]);
    assertEquals(BASE_TIME.msEpoch() + 250 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[6]);
    assertEquals(2, pts.value().data()[7]);
    assertFalse(pts.needs_repair);

    // consecutive dupe
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 2));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(0, pts.value().offset());
    assertEquals(6, pts.value().end());
    assertEquals(BASE_TIME.msEpoch() + 250 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[0]);
    assertEquals(2, pts.value().data()[1]);
    assertEquals(BASE_TIME.msEpoch() + 500 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[2]);
    assertEquals(1, pts.value().data()[3]);
    assertEquals(BASE_TIME.msEpoch() + 750 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[4]);
    assertEquals(24, pts.value().data()[5]);
    assertFalse(pts.needs_repair);

    // keep first
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 2));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(true, false);
    assertEquals(0, pts.value().offset());
    assertEquals(6, pts.value().end());
    assertEquals(BASE_TIME.msEpoch() + 250 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[0]);
    assertEquals(2, pts.value().data()[1]);
    assertEquals(BASE_TIME.msEpoch() + 500 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[2]);
    assertEquals(42, pts.value().data()[3]);
    assertEquals(BASE_TIME.msEpoch() + 750 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[4]);
    assertEquals(24, pts.value().data()[5]);
    assertFalse(pts.needs_repair);
    
    // non-consecutive dupe
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 2));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(0, pts.value().offset());
    assertEquals(6, pts.value().end());
    assertEquals(BASE_TIME.msEpoch() + 250 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[0]);
    assertEquals(2, pts.value().data()[1]);
    assertEquals(BASE_TIME.msEpoch() + 500 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[2]);
    assertEquals(42, pts.value().data()[3]);
    assertEquals(BASE_TIME.msEpoch() + 750 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[4]);
    assertEquals(1, pts.value().data()[5]);
    assertFalse(pts.needs_repair);

    // keep first
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 2));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(true, false);
    assertEquals(0, pts.value().offset());
    assertEquals(6, pts.value().end());
    assertEquals(BASE_TIME.msEpoch() + 250 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[0]);
    assertEquals(2, pts.value().data()[1]);
    assertEquals(BASE_TIME.msEpoch() + 500 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[2]);
    assertEquals(42, pts.value().data()[3]);
    assertEquals(BASE_TIME.msEpoch() + 750 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[4]);
    assertEquals(24, pts.value().data()[5]);
    assertFalse(pts.needs_repair);
  }
  
  @Test
  public void dedupeNanoSeconds() throws Exception {
    Tsdb1xNumericPartialTimeSeries pts = new Tsdb1xNumericPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 2));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(0, pts.value().offset());
    assertEquals(12, pts.value().end());
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[0]);
    assertEquals(250, pts.value().data()[1]);
    assertEquals(2, pts.value().data()[2]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[3]);
    assertEquals(500, pts.value().data()[4]);
    assertEquals(42, pts.value().data()[5]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[6]);
    assertEquals(750, pts.value().data()[7]);
    assertEquals(24, pts.value().data()[8]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[9]);
    assertEquals(1000, pts.value().data()[10]);
    assertEquals(1, pts.value().data()[11]);
    assertFalse(pts.needs_repair);

    // reset to reverse in the tree
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 2));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, true);
    assertEquals(0, pts.value().offset());
    assertEquals(12, pts.value().end());
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[0]);
    assertEquals(1000, pts.value().data()[1]);
    assertEquals(1, pts.value().data()[2]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[3]);
    assertEquals(750, pts.value().data()[4]);
    assertEquals(24, pts.value().data()[5]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[6]);
    assertEquals(500, pts.value().data()[7]);
    assertEquals(42, pts.value().data()[8]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[9]);
    assertEquals(250, pts.value().data()[10]);
    assertEquals(2, pts.value().data()[11]);
    assertFalse(pts.needs_repair);
    
    // consecutive dupe
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 2));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(0, pts.value().offset());
    assertEquals(9, pts.value().end());
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[0]);
    assertEquals(250, pts.value().data()[1]);
    assertEquals(2, pts.value().data()[2]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[3]);
    assertEquals(500, pts.value().data()[4]);
    assertEquals(1, pts.value().data()[5]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[6]);
    assertEquals(750, pts.value().data()[7]);
    assertEquals(24, pts.value().data()[8]);
    assertFalse(pts.needs_repair);
    
    // keep first
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 2));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(true, false);
    assertEquals(0, pts.value().offset());
    assertEquals(9, pts.value().end());
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[0]);
    assertEquals(250, pts.value().data()[1]);
    assertEquals(2, pts.value().data()[2]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[3]);
    assertEquals(500, pts.value().data()[4]);
    assertEquals(42, pts.value().data()[5]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[6]);
    assertEquals(750, pts.value().data()[7]);
    assertEquals(24, pts.value().data()[8]);
    assertFalse(pts.needs_repair);
    
    // non-consecutive dupe
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 2));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(0, pts.value().offset());
    assertEquals(9, pts.value().end());
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[0]);
    assertEquals(250, pts.value().data()[1]);
    assertEquals(2, pts.value().data()[2]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[3]);
    assertEquals(500, pts.value().data()[4]);
    assertEquals(42, pts.value().data()[5]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[6]);
    assertEquals(750, pts.value().data()[7]);
    assertEquals(1, pts.value().data()[8]);
    assertFalse(pts.needs_repair);
    
    // keep first
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 2));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(true, false);
    assertEquals(0, pts.value().offset());
    assertEquals(9, pts.value().end());
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[0]);
    assertEquals(250, pts.value().data()[1]);
    assertEquals(2, pts.value().data()[2]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[3]);
    assertEquals(500, pts.value().data()[4]);
    assertEquals(42, pts.value().data()[5]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[6]);
    assertEquals(750, pts.value().data()[7]);
    assertEquals(24, pts.value().data()[8]);
    assertFalse(pts.needs_repair);
  }
  
  @Test
  public void dedupeSortedMilliSeconds() throws Exception {
    Tsdb1xNumericPartialTimeSeries pts = new Tsdb1xNumericPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 1));
    assertFalse(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(0, pts.value().offset());
    assertEquals(6, pts.value().end());
    assertEquals(BASE_TIME.msEpoch() + 250 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[0]);
    assertEquals(42, pts.value().data()[1]);
    assertEquals(BASE_TIME.msEpoch() + 500 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[2]);
    assertEquals(24, pts.value().data()[3]);
    assertEquals(BASE_TIME.msEpoch() + 750 + NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[4]);
    assertEquals(1, pts.value().data()[5]);
    assertFalse(pts.needs_repair);
  }
  
  @Test
  public void dedupeSortedNanoSeconds() throws Exception {
    Tsdb1xNumericPartialTimeSeries pts = new Tsdb1xNumericPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, -42));
    assertFalse(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(0, pts.value().offset());
    assertEquals(9, pts.value().end());
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[0]);
    assertEquals(250, pts.value().data()[1]);
    assertEquals(42, pts.value().data()[2]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[3]);
    assertEquals(500, pts.value().data()[4]);
    assertEquals(24, pts.value().data()[5]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[6]);
    assertEquals(750, pts.value().data()[7]);
    assertEquals(-42, pts.value().data()[8]);
    assertFalse(pts.needs_repair);
  }
  
  @Test
  public void dedupeMixed() throws Exception {
    Tsdb1xNumericPartialTimeSeries pts = new Tsdb1xNumericPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, -24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 120000, -42));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(0, pts.value().offset());
    assertEquals(10, pts.value().end());
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[0]);
    assertEquals(250, pts.value().data()[1]);
    assertEquals(42, pts.value().data()[2]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[3]);
    assertEquals(1000, pts.value().data()[4]);
    assertEquals(-24, pts.value().data()[5]);
    assertEquals((BASE_TIME.msEpoch() + 500) | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[6]);
    assertEquals(1, pts.value().data()[7]);
    assertEquals((BASE_TIME.msEpoch() + 120_000L) | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[8]);
    assertEquals(-42, pts.value().data()[9]);
    assertFalse(pts.needs_repair);
    
    // keep first
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, -24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 120000, -42));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(true, false);
    assertEquals(0, pts.value().offset());
    assertEquals(10, pts.value().end());
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[0]);
    assertEquals(250, pts.value().data()[1]);
    assertEquals(42, pts.value().data()[2]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[3]);
    assertEquals(1000, pts.value().data()[4]);
    assertEquals(-24, pts.value().data()[5]);
    assertEquals((BASE_TIME.msEpoch() + 500) | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[6]);
    assertEquals(1, pts.value().data()[7]);
    assertEquals(BASE_TIME.epoch() + 120, pts.value().data()[8]);
    assertEquals(24, pts.value().data()[9]);
    assertFalse(pts.needs_repair);
    
    // all the same
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 120000000000L, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 120000L, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 120000000000L, -24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 120000, -42));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(0, pts.value().offset());
    assertEquals(2, pts.value().end());
    assertEquals((BASE_TIME.msEpoch() + 120_000) | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[0]);
    assertEquals(-42, pts.value().data()[1]);
    assertFalse(pts.needs_repair);

    // keep earliest
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 120000000000L, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 120000L, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 120000000000L, -24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 120000, -42));
    assertTrue(pts.needs_repair);
    
    pts.dedupe(true, false);
    assertEquals(0, pts.value().offset());
    assertEquals(3, pts.value().end());
    assertEquals(BASE_TIME.epoch() + 120 | NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[0]);
    assertEquals(0, pts.value().data()[1]);
    assertEquals(42, pts.value().data()[2]);
    assertFalse(pts.needs_repair);
  }

  @Test
  public void reverseOneCell() throws Exception {
    Tsdb1xNumericPartialTimeSeries pts = new Tsdb1xNumericPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, null);
    assertFalse(pts.needs_repair);
    
    // no-op
    pts.dedupe(false, true);
    assertEquals(0, pts.write_idx);
    
    // seconds
    pts.addColumn((byte) 0, 
        NumericCodec.buildSecondQualifier(0, (short) 0), new byte[] { 42 });
    pts.dedupe(false, true);
    assertEquals(0, pts.value().offset());
    assertEquals(2, pts.value().end());
    assertEquals(BASE_TIME.epoch(), pts.value().data()[0]);
    assertEquals(42, pts.value().data()[1]);
    
    // millis
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn((byte) 0, NumericCodec.buildMsQualifier(500, (short) 0), 
        new byte[] { 1 });
    pts.dedupe(false, true);
    assertEquals(0, pts.value().offset());
    assertEquals(2, pts.value().end());
    assertEquals(BASE_TIME.msEpoch() + 500 | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[0]);
    assertEquals(1, pts.value().data()[1]);
    
    // nanos
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn((byte) 0, NumericCodec.buildNanoQualifier(25000, (short) 0), 
        new byte[] { -1 });
    pts.dedupe(false, true);
    assertEquals(0, pts.value().offset());
    assertEquals(3, pts.value().end());
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[0]);
    assertEquals(25000, pts.value().data()[1]);
    assertEquals(-1, pts.value().data()[2]);
  }
  
  @Test
  public void reverseSeconds() throws Exception {
    Tsdb1xNumericPartialTimeSeries pts = new Tsdb1xNumericPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, null);
    // two
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    pts.dedupe(false, true);
    assertEquals(0, pts.value().offset());
    assertEquals(4, pts.value().end());
    assertEquals(BASE_TIME.epoch() + 120, pts.value().data()[0]);
    assertEquals(24, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 60, pts.value().data()[2]);
    assertEquals(42, pts.value().data()[3]);
    
    // three
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, -42));
    pts.dedupe(false, true);
    assertEquals(0, pts.value().offset());
    assertEquals(6, pts.value().end());
    assertEquals(BASE_TIME.epoch() + 180, pts.value().data()[0]);
    assertEquals(-42, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 120, pts.value().data()[2]);
    assertEquals(24, pts.value().data()[3]);
    assertEquals(BASE_TIME.epoch() + 60, pts.value().data()[4]);
    assertEquals(42, pts.value().data()[5]);
    
    // four
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, -42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 240, -24));
    pts.dedupe(false, true);
    assertEquals(0, pts.value().offset());
    assertEquals(8, pts.value().end());
    assertEquals(BASE_TIME.epoch() + 240, pts.value().data()[0]);
    assertEquals(-24, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 180, pts.value().data()[2]);
    assertEquals(-42, pts.value().data()[3]);
    assertEquals(BASE_TIME.epoch() + 120, pts.value().data()[4]);
    assertEquals(24, pts.value().data()[5]);
    assertEquals(BASE_TIME.epoch() + 60, pts.value().data()[6]);
    assertEquals(42, pts.value().data()[7]);
  }
  
  @Test
  public void reverseMilliSeconds() throws Exception {
    Tsdb1xNumericPartialTimeSeries pts = new Tsdb1xNumericPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, null);
    // two
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 24));
    pts.dedupe(false, true);
    assertEquals(0, pts.value().offset());
    assertEquals(4, pts.value().end());
    assertEquals(BASE_TIME.msEpoch() + 500 | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[0]);
    assertEquals(24, pts.value().data()[1]);
    assertEquals(BASE_TIME.msEpoch() + 250 | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[2]);
    assertEquals(42, pts.value().data()[3]);
    
    // three
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, -42));
    pts.dedupe(false, true);
    assertEquals(0, pts.value().offset());
    assertEquals(6, pts.value().end());
    assertEquals(BASE_TIME.msEpoch() + 750 | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[0]);
    assertEquals(-42, pts.value().data()[1]);
    assertEquals(BASE_TIME.msEpoch() + 500 | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[2]);
    assertEquals(24, pts.value().data()[3]);
    assertEquals(BASE_TIME.msEpoch() + 250 | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[4]);
    assertEquals(42, pts.value().data()[5]);
    
    // four
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, -42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 1000, -24));
    pts.dedupe(false, true);
    assertEquals(0, pts.value().offset());
    assertEquals(8, pts.value().end());
    assertEquals(BASE_TIME.msEpoch() + 1000 | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[0]);
    assertEquals(-24, pts.value().data()[1]);
    assertEquals(BASE_TIME.msEpoch() + 750 | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[2]);
    assertEquals(-42, pts.value().data()[3]);
    assertEquals(BASE_TIME.msEpoch() + 500 | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[4]);
    assertEquals(24, pts.value().data()[5]);
    assertEquals(BASE_TIME.msEpoch() + 250 | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[6]);
    assertEquals(42, pts.value().data()[7]);
  }
  
  @Test
  public void reverseNanoSeconds() throws Exception {
    Tsdb1xNumericPartialTimeSeries pts = new Tsdb1xNumericPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, null);
    // two
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 24));
    pts.dedupe(false, true);
    assertEquals(0, pts.value().offset());
    assertEquals(6, pts.value().end());
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[0]);
    assertEquals(500, pts.value().data()[1]);
    assertEquals(24, pts.value().data()[2]);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[3]);
    assertEquals(250, pts.value().data()[4]);
    assertEquals(42, pts.value().data()[5]);
    
    // three
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, -42));
    pts.dedupe(false, true);
    assertEquals(0, pts.value().offset());
    assertEquals(9, pts.value().end());
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[0]);
    assertEquals(750, pts.value().data()[1]);
    assertEquals(-42, pts.value().data()[2]);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[3]);
    assertEquals(500, pts.value().data()[4]);
    assertEquals(24, pts.value().data()[5]);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[6]);
    assertEquals(250, pts.value().data()[7]);
    assertEquals(42, pts.value().data()[8]);
    
    // four
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, -42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, -24));
    pts.dedupe(false, true);
    assertEquals(0, pts.value().offset());
    assertEquals(12, pts.value().end());
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[0]);
    assertEquals(1000, pts.value().data()[1]);
    assertEquals(-24, pts.value().data()[2]);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[3]);
    assertEquals(750, pts.value().data()[4]);
    assertEquals(-42, pts.value().data()[5]);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[6]);
    assertEquals(500, pts.value().data()[7]);
    assertEquals(24, pts.value().data()[8]);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[9]);
    assertEquals(250, pts.value().data()[10]);
    assertEquals(42, pts.value().data()[11]);
  }

  @Test
  public void reverseMixed() throws Exception {
    Tsdb1xNumericPartialTimeSeries pts = new Tsdb1xNumericPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, -24));
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, -42));
    pts.dedupe(false, true);
    assertEquals(0, pts.value().offset());
    assertEquals(12, pts.value().end());
    assertEquals(BASE_TIME.epoch() + 180, pts.value().data()[0]);
    assertEquals(-42, pts.value().data()[1]);
    assertEquals(BASE_TIME.epoch() + 120, pts.value().data()[2]);
    assertEquals(24, pts.value().data()[3]);
    assertEquals(BASE_TIME.msEpoch() + 500 | NumericLongArrayType.MILLISECOND_FLAG, pts.value().data()[4]);
    assertEquals(1, pts.value().data()[5]);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[6]);
    assertEquals(1000, pts.value().data()[7]);
    assertEquals(-24, pts.value().data()[8]);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, pts.value().data()[9]);
    assertEquals(250, pts.value().data()[10]);
    assertEquals(42, pts.value().data()[11]);
  }

  @Test
  public void growOnAdd() throws Exception {
    pool = mock(ObjectPool.class);
    final PooledObject obj = mock(PooledObject.class);
    long[] arr = new long[3];
    when(obj.object()).thenReturn(arr);
    when(pool.claim()).thenAnswer(new Answer<PooledObject>() {
      @Override
      public PooledObject answer(InvocationOnMock invocation) throws Throwable {
        return obj;
      }
      
    });
    
    Tsdb1xNumericPartialTimeSeries pts = new Tsdb1xNumericPartialTimeSeries();
    pts.reset(BASE_TIME, 42, pool, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    PooledObject array = pts.pooled_array;
    verify(obj, never()).release();
    assertEquals(3, ((long[]) array.object()).length);
    
    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    verify(obj, times(1)).release();
    assertNotSame(array, pts.pooled_array);
    
    assertEquals(0, pts.value().offset());
    assertEquals(4, pts.value().end());
    assertEquals(BASE_TIME.epoch() + 60, ((long[]) pts.pooled_array.object())[0]);
    assertEquals(42, ((long[]) pts.pooled_array.object())[1]);
    assertEquals(BASE_TIME.epoch() + 120, ((long[]) pts.pooled_array.object())[2]);
    assertEquals(24, ((long[]) pts.pooled_array.object())[3]);
    assertEquals(0, ((long[]) pts.pooled_array.object())[4]);
    assertEquals(18, ((long[])pts.pooled_array.object()).length);
    
    pts.dedupe(false, false);
    assertEquals(18, ((long[])pts.pooled_array.object()).length);
    
    // grow again
    int offset = 160;
    for (int i = 0; i < 9; i++) {
      pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, offset, i));
      offset += 60;
    }
    
    assertEquals(0, pts.value().offset());
    assertEquals(22, pts.value().end());
    assertEquals(32, ((long[])pts.pooled_array.object()).length);
  }
  
  @Test
  public void resetNoPool() throws Exception {
    Tsdb1xNumericPartialTimeSeries pts = new Tsdb1xNumericPartialTimeSeries();
    pts.reset(BASE_TIME, 42, null, SET, null);
    pts.addColumn(Schema.APPENDS_PREFIX,  APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    PooledObject array = pts.pooled_array;
    assertEquals(2048, ((long[]) array.object()).length);
    
    pts.addColumn(Schema.APPENDS_PREFIX,  APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    
    assertEquals(0, pts.value().offset());
    assertEquals(4, pts.value().end());
    assertEquals(BASE_TIME.epoch() + 60, ((long[]) pts.pooled_array.object())[0]);
    assertEquals(42, ((long[]) pts.pooled_array.object())[1]);
    assertEquals(BASE_TIME.epoch() + 120, ((long[]) pts.pooled_array.object())[2]);
    assertEquals(24, ((long[]) pts.pooled_array.object())[3]);
    assertEquals(0, ((long[]) pts.pooled_array.object())[4]);
    
    pts.dedupe(false, false);
    assertEquals(2048, ((long[])pts.pooled_array.object()).length);
  }
}
