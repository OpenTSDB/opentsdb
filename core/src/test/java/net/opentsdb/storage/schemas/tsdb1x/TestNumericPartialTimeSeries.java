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
import static org.junit.Assert.assertNull;
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

public class TestNumericPartialTimeSeries {
  private static final TimeStamp BASE_TIME = new SecondTimeStamp(1514764800);
  private static final byte[] APPEND_Q = 
      new byte[] { Schema.APPENDS_PREFIX, 0, 0 };
  private static final PartialTimeSeriesSet SET = mock(PartialTimeSeriesSet.class);
  private ObjectPool pool;
  private PooledObject object;
  private long[] array;
  
  @Before
  public void before() throws Exception {
    pool = mock(ObjectPool.class);
    object = mock(PooledObject.class);
    array = new long[4096];
    when(pool.claim()).thenReturn(object);
    when(object.object()).thenReturn(array);
  }
  
  @Test
  public void addColumnPuts() throws Exception {
    NumericPartialTimeSeries pts = new NumericPartialTimeSeries();
    pts.addColumn((byte) 0, 
        BASE_TIME,
        NumericCodec.buildSecondQualifier(0, (short) 0), 
        new byte[] { 42 },
        pool,
        42,
        SET);
    assertEquals(42, pts.idHash());
    assertSame(SET, pts.set());
    assertEquals(2, pts.write_idx);
    assertEquals(BASE_TIME.epoch(), array[0]); // no flags here
    assertEquals(42, array[1]);
    assertEquals(0, array[2]); // no terminal
    assertFalse(pts.needs_repair);
     
    pts.addColumn((byte) 0,
        BASE_TIME,
        NumericCodec.buildSecondQualifier(60, (short) 0), 
        new byte[] { 24 },
        pool,
        42,
        SET);
    assertEquals(4, pts.write_idx);
    assertEquals(BASE_TIME.epoch(), array[0]); // no flags here
    assertEquals(42, array[1]);
    assertEquals(BASE_TIME.epoch() + 60, array[2]); // no flags here
    assertEquals(24, array[3]);
    assertEquals(0, array[4]); // no terminal
    assertFalse(pts.needs_repair);
    
    // out of order
    pts.addColumn((byte) 0, 
        BASE_TIME,
        NumericCodec.buildSecondQualifier(30, (short) 0), 
        new byte[] { 1 },
        pool,
        42,
        SET);
    assertEquals(6, pts.write_idx);
    assertEquals(BASE_TIME.epoch(), array[0]); // no flags here
    assertEquals(42, array[1]);
    assertEquals(BASE_TIME.epoch() + 60, array[2]); // no flags here
    assertEquals(24, array[3]);
    assertEquals(BASE_TIME.epoch() + 30, array[4]); // no flags here
    assertEquals(1, array[5]);
    assertEquals(0, array[6]); // no terminal
    assertTrue(pts.needs_repair);
    
    // add ms
    pts.addColumn((byte) 0, 
        BASE_TIME,
        NumericCodec.buildMsQualifier(500, (short) 0), 
        new byte[] { 2 },
        pool,
        42,
        SET);
    assertEquals(8, pts.write_idx);
    assertEquals(BASE_TIME.epoch(), array[0]); // no flags here
    assertEquals(42, array[1]);
    assertEquals(BASE_TIME.epoch() + 60, array[2]); // no flags here
    assertEquals(24, array[3]);
    assertEquals(BASE_TIME.epoch() + 30, array[4]); // no flags here
    assertEquals(1, array[5]);
    assertEquals((BASE_TIME.msEpoch() + 500) | NumericLongArrayType.MILLISECOND_FLAG, array[6]); // ms flag
    assertEquals(2, array[7]);
    assertEquals(0, array[8]); // no terminal
    assertTrue(pts.needs_repair);
    
    // add nano
    pts.addColumn((byte) 0,
        BASE_TIME,
        NumericCodec.buildNanoQualifier(25000, (short) 0), 
        new byte[] { -1 },
        pool,
        42,
        SET);
    assertEquals(11, pts.write_idx);
    assertEquals(BASE_TIME.epoch(), array[0]); // no flags here
    assertEquals(42, array[1]);
    assertEquals(BASE_TIME.epoch() + 60, array[2]); // no flags here
    assertEquals(24, array[3]);
    assertEquals(BASE_TIME.epoch() + 30, array[4]); // no flags here
    assertEquals(1, array[5]);
    assertEquals((BASE_TIME.msEpoch() + 500) | NumericLongArrayType.MILLISECOND_FLAG, array[6]); // ms flag
    assertEquals(2, array[7]);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, array[8]); // nano flag
    assertEquals(25000, array[9]); // nanos
    assertEquals(-1, array[10]);
    assertEquals(0, array[11]); // no terminal
    assertTrue(pts.needs_repair);
    
    // floats
    pts.addColumn((byte) 0,
        BASE_TIME,
        NumericCodec.buildSecondQualifier(300, (short) (NumericCodec.FLAG_FLOAT | (short) 3)), 
        Bytes.fromInt(Float.floatToIntBits(0.5F)),
        pool,
        42,
        SET);
    assertEquals(13, pts.write_idx);
    assertEquals((BASE_TIME.epoch() + 300) | NumericLongArrayType.FLOAT_FLAG, array[11]);
    assertEquals(Double.doubleToRawLongBits(0.5F), array[12]);
    assertEquals(0, array[13]); // no terminal
    assertTrue(pts.needs_repair);
    
    pts.addColumn((byte) 0,
        BASE_TIME,
        NumericCodec.buildSecondQualifier(900, (short) (NumericCodec.FLAG_FLOAT | (short) 7)), 
        Bytes.fromLong(Double.doubleToRawLongBits(1.42)),
        pool,
        42,
        SET);
    assertEquals(15, pts.write_idx);
    assertEquals((BASE_TIME.epoch() + 900) | NumericLongArrayType.FLOAT_FLAG, array[13]);
    assertEquals(Double.doubleToRawLongBits(1.42), array[14]);
    assertEquals(0, array[15]); // no terminal
    assertTrue(pts.needs_repair);
    
    // add ms double
    pts.addColumn((byte) 0, 
        BASE_TIME,
        NumericCodec.buildMsQualifier(500000, (short) (NumericCodec.FLAG_FLOAT | (short) 7)), 
        Bytes.fromLong(Double.doubleToRawLongBits(42.5)),
        pool,
        42,
        SET);
    assertEquals(17, pts.write_idx);
    assertEquals((BASE_TIME.msEpoch() + 500000) | NumericLongArrayType.FLOAT_FLAG | NumericLongArrayType.MILLISECOND_FLAG, array[15]); // ms and float flag
    assertEquals(Double.doubleToRawLongBits(42.5), array[16]);
    assertEquals(0, array[17]); // no terminal
    assertTrue(pts.needs_repair);
    
    // add ns double
    pts.addColumn((byte) 0, 
        BASE_TIME,
        NumericCodec.buildNanoQualifier((1800L * 1000L * 1000L * 1000L) + 750L, (short) (NumericCodec.FLAG_FLOAT | (short) 7)), 
        Bytes.fromLong(Double.doubleToRawLongBits(0.005)),
        pool,
        42,
        SET);
    assertEquals(20, pts.write_idx);
    assertEquals(((BASE_TIME.epoch() + 1800)| NumericLongArrayType.FLOAT_FLAG | NumericLongArrayType.NANOSECOND_FLAG), array[17]); // ns and float flag
    assertEquals(750, array[18]);
    assertEquals(Double.doubleToRawLongBits(0.005), array[19]);
    assertEquals(0, array[20]); // no terminal
    assertTrue(pts.needs_repair);
    
    verify(pool, times(1)).claim();
  }

  @Test
  public void addColumnAppends() throws Exception {
    NumericPartialTimeSeries pts = new NumericPartialTimeSeries();
    
    pts.addColumn(Schema.APPENDS_PREFIX, 
        BASE_TIME,
        APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 42),
        pool,
        42,
        SET);
    assertEquals(42, pts.idHash());
    assertSame(SET, pts.set());
    assertEquals(2, pts.write_idx);
    assertEquals(BASE_TIME.epoch(), array[0]); // no flags here
    assertEquals(42, array[1]);
    assertEquals(0, array[2]); // no terminal
    assertFalse(pts.needs_repair);
    
    pts.addColumn(Schema.APPENDS_PREFIX, 
        BASE_TIME,
        APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 24),
        pool,
        42,
        SET);
    assertEquals(4, pts.write_idx);
    assertEquals(BASE_TIME.epoch(), array[0]); // no flags here
    assertEquals(42, array[1]);
    assertEquals(BASE_TIME.epoch() + 30, array[2]); // no flags here
    assertEquals(24, array[3]);
    assertEquals(0, array[4]); // no terminal
    assertFalse(pts.needs_repair);
    
    pts.addColumn(Schema.APPENDS_PREFIX, 
        BASE_TIME,
        APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1),
        pool,
        42,
        SET);
    assertEquals(6, pts.write_idx);
    assertEquals(BASE_TIME.epoch(), array[0]); // no flags here
    assertEquals(42, array[1]);
    assertEquals(BASE_TIME.epoch() + 30, array[2]); // no flags here
    assertEquals(24, array[3]);
    assertEquals((BASE_TIME.msEpoch() + 500) | NumericLongArrayType.MILLISECOND_FLAG, array[4]); // ms flag
    assertEquals(1, array[5]);
    assertEquals(0, array[6]); // no terminal
    assertTrue(pts.needs_repair);
    
    pts.addColumn(Schema.APPENDS_PREFIX, 
        BASE_TIME,
        APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 25000, -1),
        pool,
        42,
        SET);
    assertEquals(9, pts.write_idx);
    assertEquals(BASE_TIME.epoch(), array[0]); // no flags here
    assertEquals(42, array[1]);
    assertEquals(BASE_TIME.epoch() + 30, array[2]); // no flags here
    assertEquals(24, array[3]);
    assertEquals((BASE_TIME.msEpoch() + 500) | NumericLongArrayType.MILLISECOND_FLAG, array[4]); // ms flag
    assertEquals(1, array[5]);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, array[6]); // Nanos flag
    assertEquals(25000, array[7]);
    assertEquals(-1, array[8]);
    assertEquals(0, array[9]); // no terminal
    assertTrue(pts.needs_repair);
    
    // floats
    pts.addColumn(Schema.APPENDS_PREFIX,
        BASE_TIME,
        APPEND_Q,
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 0.5),
        pool,
        42,
        SET);
    assertEquals(11, pts.write_idx);
    assertEquals((BASE_TIME.epoch() + 60) | NumericLongArrayType.FLOAT_FLAG, array[9]);
    assertEquals(Double.doubleToRawLongBits(0.5F), array[10]);
    assertEquals(0, array[11]); // no terminal
    assertTrue(pts.needs_repair);
    
    pts.addColumn(Schema.APPENDS_PREFIX,
        BASE_TIME,
        APPEND_Q,
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 1.42),
        pool,
        42,
        SET);
    assertEquals(13, pts.write_idx);
    assertEquals((BASE_TIME.epoch() + 120) | NumericLongArrayType.FLOAT_FLAG, array[11]);
    assertEquals(Double.doubleToRawLongBits(1.42), array[12]);
    assertEquals(0, array[13]); // no terminal
    assertTrue(pts.needs_repair);
    
    // add ms double
    pts.addColumn(Schema.APPENDS_PREFIX, 
        BASE_TIME,
        APPEND_Q,
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500000, 42.5),
        pool,
        42,
        SET);
    assertEquals(15, pts.write_idx);
    assertEquals((BASE_TIME.msEpoch() + 500000) | NumericLongArrayType.FLOAT_FLAG | NumericLongArrayType.MILLISECOND_FLAG, array[13]); // ms and float flag
    assertEquals(Double.doubleToRawLongBits(42.5), array[14]);
    assertEquals(0, array[15]); // no terminal
    assertTrue(pts.needs_repair);
    
    // add ns double
    pts.addColumn(Schema.APPENDS_PREFIX, 
        BASE_TIME,
        APPEND_Q,
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, (1800L * 1000L * 1000L * 1000L) + 750L, 0.005),
        pool,
        42,
        SET);
    assertEquals(18, pts.write_idx);
    assertEquals(((BASE_TIME.epoch() + 1800)| NumericLongArrayType.FLOAT_FLAG | NumericLongArrayType.NANOSECOND_FLAG), array[15]); // ns and float flag
    assertEquals(750, array[16]);
    assertEquals(Double.doubleToRawLongBits(0.005), array[17]);
    assertEquals(0, array[18]); // no terminal
    assertTrue(pts.needs_repair);
    
    // multiples
    pts.addColumn(Schema.APPENDS_PREFIX, 
        BASE_TIME,
        APPEND_Q, 
        com.google.common.primitives.Bytes.concat(
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 2600, 5),
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 2660, 6),
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 2720, 7)),
        pool,
        42,
        SET);
    assertEquals(24, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + 2600, array[18]);
    assertEquals(5, array[19]);
    assertEquals(BASE_TIME.epoch() + 2660, array[20]);
    assertEquals(6, array[21]);
    assertEquals(BASE_TIME.epoch() + 2720, array[22]);
    assertEquals(7, array[23]);
    assertEquals(0, array[24]); // no terminal
    assertTrue(pts.needs_repair);
    
    verify(pool, times(1)).claim();
  }

  @Test
  public void addColumnMixed() throws Exception {
    NumericPartialTimeSeries pts = new NumericPartialTimeSeries();
    
    pts.addColumn(Schema.APPENDS_PREFIX,
        BASE_TIME,
        APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 42),
        pool,
        42,
        SET);
    assertEquals(42, pts.idHash());
    assertSame(SET, pts.set());
    assertEquals(2, pts.write_idx);
    assertEquals(BASE_TIME.epoch(), array[0]); // no flags here
    assertEquals(42, array[1]);
    assertEquals(0, array[2]); // no terminal
    assertFalse(pts.needs_repair);
    
    pts.addColumn((byte) 0, 
        BASE_TIME,
        NumericCodec.buildSecondQualifier(60, (short) 0), 
        new byte[] { 24 },
        pool,
        42,
        SET);
    assertEquals(4, pts.write_idx);
    assertEquals(BASE_TIME.epoch(), array[0]); // no flags here
    assertEquals(42, array[1]);
    assertEquals(BASE_TIME.epoch() + 60, array[2]); // no flags here
    assertEquals(24, array[3]);
    assertEquals(0, array[4]); // no terminal
    assertFalse(pts.needs_repair);
    
    pts.addColumn(Schema.APPENDS_PREFIX, 
        BASE_TIME,
        APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 120_500, 1),
        pool,
        42,
        SET);
    assertEquals(6, pts.write_idx);
    assertEquals(BASE_TIME.epoch(), array[0]); // no flags here
    assertEquals(42, array[1]);
    assertEquals(BASE_TIME.epoch() + 60, array[2]); // no flags here
    assertEquals(24, array[3]);
    assertEquals((BASE_TIME.msEpoch() + 120500) | NumericLongArrayType.MILLISECOND_FLAG, array[4]);
    assertEquals(1, array[5]);
    assertEquals(0, array[6]); // no terminal
    assertFalse(pts.needs_repair);
    
    pts.addColumn((byte) 0, 
        BASE_TIME,
        NumericCodec.buildNanoQualifier(180_000_025_000L, (short) 0), 
        new byte[] { -1 },
        pool,
        42,
        SET);
    assertEquals(9, pts.write_idx);
    assertEquals(BASE_TIME.epoch(), array[0]); // no flags here
    assertEquals(42, array[1]);
    assertEquals(BASE_TIME.epoch() + 60, array[2]); // no flags here
    assertEquals(24, array[3]);
    assertEquals((BASE_TIME.msEpoch() + 120500) | NumericLongArrayType.MILLISECOND_FLAG, array[4]);
    assertEquals(1, array[5]);
    assertEquals(BASE_TIME.epoch() + 180 | NumericLongArrayType.NANOSECOND_FLAG, array[6]);
    assertEquals(25_000, array[7]);
    assertEquals(-1, array[8]);
    assertEquals(0, array[9]); // no terminal
    assertFalse(pts.needs_repair);
  }
  
  @Test
  public void addColumnExceptionsAndBadData() throws Exception {
    NumericPartialTimeSeries pts = new NumericPartialTimeSeries();
    try {
      pts.addColumn((byte) 0, 
          BASE_TIME,
          null, 
          new byte[] { 42 },
          pool,
          42,
          SET);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
    
    try {
      pts.addColumn((byte) 0, 
          BASE_TIME,
          NumericCodec.buildSecondQualifier(0, (short) 0), 
          null,
          pool,
          42,
          SET);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
    
    try {
      pts.addColumn((byte) 0, 
          BASE_TIME,
          NumericCodec.buildSecondQualifier(0, (short) 0), 
          new byte[] { 42 },
          null,
          42,
          SET);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
    
    // bad qualifier
    try {
      pts.addColumn((byte) 0, 
          BASE_TIME,
          new byte[0], 
          new byte[] { 42 },
          pool,
          42,
          SET);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    // bad value
    try {
      pts.addColumn((byte) 0, 
          BASE_TIME,
          NumericCodec.buildSecondQualifier(0, (short) 0), 
          new byte[] { },
          pool,
          42,
          SET);
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
    
    NumericPartialTimeSeries pts = new NumericPartialTimeSeries();
    pts.addColumn((byte) 0, 
        BASE_TIME,
        compacted_qualifier, 
        compacted_value,
        pool,
        42,
        SET);
    assertEquals(42, pts.idHash());
    assertSame(SET, pts.set());
    assertEquals(8, pts.write_idx);
    assertEquals(BASE_TIME.epoch(), array[0]); // no flags here
    assertEquals(42, array[1]);
    assertEquals(BASE_TIME.epoch() + 60, array[2]); // no flags here
    assertEquals(24, array[3]);
    assertEquals(BASE_TIME.epoch() + 120, array[4]); // no flags here
    assertEquals(1, array[5]);
    assertEquals(BASE_TIME.epoch() + 180, array[6]); // no flags here
    assertEquals(-1, array[7]);
    assertEquals(0, array[8]); // no terminal
    assertFalse(pts.needs_repair);
    
//    pts.dedupe(false, false);
//    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
//        OffsetResolution.SECONDS, 0, 42), 
//        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 24),
//        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 1),
//        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, -1)), 
//          pts.data);
//    
//    pts = new NumericRowpts(BASE_TIME);
//    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
//        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 42));
//    pts.addColumn((byte) 0, compacted_qualifier, compacted_value);
//    
//    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
//        OffsetResolution.SECONDS, 0, 42), 
//        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 42),
//        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 24),
//        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 1),
//        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, -1)), 
//          pts.data);
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
    
    NumericPartialTimeSeries pts = new NumericPartialTimeSeries();
    pts.addColumn((byte) 0, 
        BASE_TIME,
        compacted_qualifier, 
        compacted_value,
        pool,
        42,
        SET);
    assertEquals(42, pts.idHash());
    assertSame(SET, pts.set());
    assertEquals(8, pts.write_idx);
    assertEquals(BASE_TIME.epoch(), array[0]); // no flags here
    assertEquals(42, array[1]);
    assertEquals((BASE_TIME.msEpoch() + 60000) | NumericLongArrayType.MILLISECOND_FLAG, array[2]); // ms flag
    assertEquals(24, array[3]);
    assertEquals(BASE_TIME.epoch() + 120, array[4]); // no flags here
    assertEquals(1, array[5]);
    assertEquals((BASE_TIME.msEpoch() + 180000) | NumericLongArrayType.MILLISECOND_FLAG, array[6]); // ms flag
    assertEquals(-1, array[7]);
    assertEquals(0, array[8]); // no terminal
    assertFalse(pts.needs_repair);
    
//    pts.dedupe(false, false);
//    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
//        OffsetResolution.SECONDS, 0, 42), 
//        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 60000, 24),
//        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 1),
//        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 180000, -1)), 
//          pts.data);
//    
//    pts = new NumericRowpts(BASE_TIME);
//    pts.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
//        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 42));
//    pts.addColumn((byte) 0, compacted_qualifier, compacted_value);
//    
//    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
//        OffsetResolution.SECONDS, 0, 42), 
//        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 42),
//        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 60000, 24),
//        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 1),
//        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 180000, -1)), 
//          pts.data);
  }
  
  @Test
  public void addColumnFixOldTSDIssues() throws Exception {
    NumericPartialTimeSeries pts = new NumericPartialTimeSeries();
    
    pts.addColumn((byte) 0, 
        BASE_TIME,
        NumericCodec.buildSecondQualifier(500, (short) 1),
        net.opentsdb.utils.Bytes.fromLong(42),
        pool,
        42,
        SET);
    assertEquals(42, pts.idHash());
    assertSame(SET, pts.set());
    assertEquals(2, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + 500, array[0]); // no flags here
    assertEquals(42, array[1]);
    assertEquals(0, array[2]); // no terminal
    assertFalse(pts.needs_repair);
    
    pts.addColumn((byte) 0, 
        BASE_TIME,
        NumericCodec.buildSecondQualifier(560, (short) 0),
        net.opentsdb.utils.Bytes.fromLong(24),
        pool,
        42,
        SET);
    assertEquals(4, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + 560, array[2]); // no flags here
    assertEquals(24, array[3]);
    assertEquals(0, array[4]); // no terminal
    assertFalse(pts.needs_repair);
    
    pts.addColumn((byte) 0, 
        BASE_TIME,
        NumericCodec.buildSecondQualifier(620, (short) 7),
        net.opentsdb.utils.Bytes.fromInt(1),
        pool,
        42,
        SET);
    assertEquals(6, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + 620, array[4]); // no flags here
    assertEquals(1, array[5]);
    assertEquals(0, array[6]); // no terminal
    assertFalse(pts.needs_repair);

    pts.data();
    pts.close(); // reset!
    pts.addColumn((byte) 0, 
        BASE_TIME,
        NumericCodec.buildSecondQualifier(500, (short) (7 | NumericCodec.FLAG_FLOAT)),
        net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F)),
        pool,
        42,
        SET);
    assertEquals(2, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + 500 | NumericLongArrayType.FLOAT_FLAG, array[0]); // float
    assertEquals(Double.doubleToRawLongBits(42.5), array[1]);
    //assertEquals(0, array[2]); // no terminal BUT because in this UT we're resetting and re-using we haven't cleaned out the array
    assertFalse(pts.needs_repair);

    // old style incorrect length floating point value
    pts.addColumn((byte) 0, 
        BASE_TIME,
        NumericCodec.buildSecondQualifier(560, (short) (3 | NumericCodec.FLAG_FLOAT)),
        com.google.common.primitives.Bytes.concat(new byte[4], 
            net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(24.5F))),
        pool,
        42,
        SET);
    assertEquals(4, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + 560 | NumericLongArrayType.FLOAT_FLAG, array[2]); // float
    assertEquals(Double.doubleToRawLongBits(24.5), array[3]);
    //assertEquals(0, array[4]); // no terminal BUT because in this UT we're resetting and re-using we haven't cleaned out the array
    assertFalse(pts.needs_repair);
  }

  @Test
  public void dedupeSortedSeconds() throws Exception {
    NumericPartialTimeSeries pts = new NumericPartialTimeSeries();
    
    pts.addColumn(Schema.APPENDS_PREFIX, 
        BASE_TIME,
        APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42),
        pool,
        42,
        SET);
    pts.addColumn(Schema.APPENDS_PREFIX, 
        BASE_TIME,
        APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
        pool,
        42,
        SET);
    pts.addColumn(Schema.APPENDS_PREFIX, 
        BASE_TIME,
        APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, 1),
        pool,
        42,
        SET);
    assertEquals(42, pts.idHash());
    assertSame(SET, pts.set());
    assertEquals(6, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + 60, array[0]); // no flags here
    assertEquals(42, array[1]);
    assertEquals(BASE_TIME.epoch() + 120, array[2]); // no flags here
    assertEquals(24, array[3]);
    assertEquals(BASE_TIME.epoch() + 180, array[4]); // no flags here
    assertEquals(1, array[5]);
    assertEquals(0, array[6]); // no terminal
    assertFalse(pts.needs_repair);

    pts.dedupe(false, false);
    assertEquals(6, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + 60, array[0]); // no flags here
    assertEquals(42, array[1]);
    assertEquals(BASE_TIME.epoch() + 120, array[2]); // no flags here
    assertEquals(24, array[3]);
    assertEquals(BASE_TIME.epoch() + 180, array[4]); // no flags here
    assertEquals(1, array[5]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[6]); // no terminal
    assertFalse(pts.needs_repair);
  }
  
  @Test
  public void dedupeSeconds() throws Exception {
    NumericPartialTimeSeries pts = new NumericPartialTimeSeries();
    
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2),
        pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(8, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + 30, array[0]); // no flags here
    assertEquals(2, array[1]);
    assertEquals(BASE_TIME.epoch() + 60, array[2]); // no flags here
    assertEquals(42, array[3]);
    assertEquals(BASE_TIME.epoch() + 120, array[4]); // no flags here
    assertEquals(24, array[5]);
    assertEquals(BASE_TIME.epoch() + 180, array[6]); // no flags here
    assertEquals(1, array[7]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[8]);
    assertFalse(pts.needs_repair);

    // reset to reverse in the tree
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2),
        pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, true);
    assertEquals(8, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + 180, array[0]); // no flags here
    assertEquals(1, array[1]);
    assertEquals(BASE_TIME.epoch() + 120, array[2]); // no flags here
    assertEquals(24, array[3]);
    assertEquals(BASE_TIME.epoch() + 60, array[4]); // no flags here
    assertEquals(42, array[5]);
    assertEquals(BASE_TIME.epoch() + 30, array[6]); // no flags here
    assertEquals(2, array[7]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[8]);
    assertFalse(pts.needs_repair);
    
    // consecutive dupe
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2),
        pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(true, false);
    assertEquals(6, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + 30, array[0]); // no flags here
    assertEquals(2, array[1]);
    assertEquals(BASE_TIME.epoch() + 60, array[2]); // no flags here
    assertEquals(42, array[3]);
    assertEquals(BASE_TIME.epoch() + 120, array[4]); // no flags here
    assertEquals(24, array[5]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[6]);
    assertFalse(pts.needs_repair);
    
    // keep first
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2),
        pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(6, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + 30, array[0]); // no flags here
    assertEquals(2, array[1]);
    assertEquals(BASE_TIME.epoch() + 60, array[2]); // no flags here
    assertEquals(1, array[3]);
    assertEquals(BASE_TIME.epoch() + 120, array[4]); // no flags here
    assertEquals(24, array[5]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[6]);
    assertFalse(pts.needs_repair);
    
    // non-consecutive dupe
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2),
        pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(true, false);
    assertEquals(6, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + 30, array[0]); // no flags here
    assertEquals(2, array[1]);
    assertEquals(BASE_TIME.epoch() + 60, array[2]); // no flags here
    assertEquals(42, array[3]);
    assertEquals(BASE_TIME.epoch() + 120, array[4]); // no flags here
    assertEquals(24, array[5]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[6]);
    assertFalse(pts.needs_repair);
    
    // keep first
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2),
        pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(6, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + 30, array[0]); // no flags here
    assertEquals(2, array[1]);
    assertEquals(BASE_TIME.epoch() + 60, array[2]); // no flags here
    assertEquals(42, array[3]);
    assertEquals(BASE_TIME.epoch() + 120, array[4]); // no flags here
    assertEquals(1, array[5]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[6]);
    assertFalse(pts.needs_repair);
  }

  @Test
  public void dedupeMilliSeconds() throws Exception {
    NumericPartialTimeSeries pts = new NumericPartialTimeSeries();
    
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 1000, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 2),
        pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(8, pts.write_idx);
    assertEquals(BASE_TIME.msEpoch() + 250 + NumericLongArrayType.MILLISECOND_FLAG, array[0]);
    assertEquals(2, array[1]);
    assertEquals(BASE_TIME.msEpoch() + 500 + NumericLongArrayType.MILLISECOND_FLAG, array[2]);
    assertEquals(42, array[3]);
    assertEquals(BASE_TIME.msEpoch() + 750 + NumericLongArrayType.MILLISECOND_FLAG, array[4]);
    assertEquals(24, array[5]);
    assertEquals(BASE_TIME.msEpoch() + 1000 + NumericLongArrayType.MILLISECOND_FLAG, array[6]);
    assertEquals(1, array[7]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[8]);
    assertFalse(pts.needs_repair);

    // reset to reverse in the tree
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 1000, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 2),
        pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, true);
    assertEquals(8, pts.write_idx);
    assertEquals(BASE_TIME.msEpoch() + 1000 + NumericLongArrayType.MILLISECOND_FLAG, array[0]);
    assertEquals(1, array[1]);
    assertEquals(BASE_TIME.msEpoch() + 750 + NumericLongArrayType.MILLISECOND_FLAG, array[2]);
    assertEquals(24, array[3]);
    assertEquals(BASE_TIME.msEpoch() + 500 + NumericLongArrayType.MILLISECOND_FLAG, array[4]);
    assertEquals(42, array[5]);
    assertEquals(BASE_TIME.msEpoch() + 250 + NumericLongArrayType.MILLISECOND_FLAG, array[6]);
    assertEquals(2, array[7]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[8]);
    assertFalse(pts.needs_repair);

    // consecutive dupe
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 2),
        pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(6, pts.write_idx);
    assertEquals(BASE_TIME.msEpoch() + 250 + NumericLongArrayType.MILLISECOND_FLAG, array[0]);
    assertEquals(2, array[1]);
    assertEquals(BASE_TIME.msEpoch() + 500 + NumericLongArrayType.MILLISECOND_FLAG, array[2]);
    assertEquals(1, array[3]);
    assertEquals(BASE_TIME.msEpoch() + 750 + NumericLongArrayType.MILLISECOND_FLAG, array[4]);
    assertEquals(24, array[5]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[6]);
    assertFalse(pts.needs_repair);

    // keep first
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42),
            pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 2),
        pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(true, false);
    assertEquals(6, pts.write_idx);
    assertEquals(BASE_TIME.msEpoch() + 250 + NumericLongArrayType.MILLISECOND_FLAG, array[0]);
    assertEquals(2, array[1]);
    assertEquals(BASE_TIME.msEpoch() + 500 + NumericLongArrayType.MILLISECOND_FLAG, array[2]);
    assertEquals(42, array[3]);
    assertEquals(BASE_TIME.msEpoch() + 750 + NumericLongArrayType.MILLISECOND_FLAG, array[4]);
    assertEquals(24, array[5]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[6]);
    assertFalse(pts.needs_repair);
    
    // non-consecutive dupe
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 2),
        pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(6, pts.write_idx);
    assertEquals(BASE_TIME.msEpoch() + 250 + NumericLongArrayType.MILLISECOND_FLAG, array[0]);
    assertEquals(2, array[1]);
    assertEquals(BASE_TIME.msEpoch() + 500 + NumericLongArrayType.MILLISECOND_FLAG, array[2]);
    assertEquals(42, array[3]);
    assertEquals(BASE_TIME.msEpoch() + 750 + NumericLongArrayType.MILLISECOND_FLAG, array[4]);
    assertEquals(1, array[5]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[6]);
    assertFalse(pts.needs_repair);

    // keep first
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 2),
        pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(true, false);
    assertEquals(6, pts.write_idx);
    assertEquals(BASE_TIME.msEpoch() + 250 + NumericLongArrayType.MILLISECOND_FLAG, array[0]);
    assertEquals(2, array[1]);
    assertEquals(BASE_TIME.msEpoch() + 500 + NumericLongArrayType.MILLISECOND_FLAG, array[2]);
    assertEquals(42, array[3]);
    assertEquals(BASE_TIME.msEpoch() + 750 + NumericLongArrayType.MILLISECOND_FLAG, array[4]);
    assertEquals(24, array[5]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[6]);
    assertFalse(pts.needs_repair);
  }
  
  @Test
  public void dedupeNanoSeconds() throws Exception {
    NumericPartialTimeSeries pts = new NumericPartialTimeSeries();
    
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 2),
        pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(12, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[0]);
    assertEquals(250, array[1]);
    assertEquals(2, array[2]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[3]);
    assertEquals(500, array[4]);
    assertEquals(42, array[5]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[6]);
    assertEquals(750, array[7]);
    assertEquals(24, array[8]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[9]);
    assertEquals(1000, array[10]);
    assertEquals(1, array[11]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[12]);
    assertFalse(pts.needs_repair);

    // reset to reverse in the tree
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 2),
        pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, true);
    assertEquals(12, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[0]);
    assertEquals(1000, array[1]);
    assertEquals(1, array[2]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[3]);
    assertEquals(750, array[4]);
    assertEquals(24, array[5]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[6]);
    assertEquals(500, array[7]);
    assertEquals(42, array[8]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[9]);
    assertEquals(250, array[10]);
    assertEquals(2, array[11]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[12]);
    assertFalse(pts.needs_repair);
    
    // consecutive dupe
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 2),
        pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(9, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[0]);
    assertEquals(250, array[1]);
    assertEquals(2, array[2]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[3]);
    assertEquals(500, array[4]);
    assertEquals(1, array[5]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[6]);
    assertEquals(750, array[7]);
    assertEquals(24, array[8]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[9]);
    assertFalse(pts.needs_repair);
    
    // keep first
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 2),
            pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(true, false);
    assertEquals(9, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[0]);
    assertEquals(250, array[1]);
    assertEquals(2, array[2]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[3]);
    assertEquals(500, array[4]);
    assertEquals(42, array[5]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[6]);
    assertEquals(750, array[7]);
    assertEquals(24, array[8]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[9]);
    assertFalse(pts.needs_repair);
    
    // non-consecutive dupe
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 2),
        pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(9, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[0]);
    assertEquals(250, array[1]);
    assertEquals(2, array[2]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[3]);
    assertEquals(500, array[4]);
    assertEquals(42, array[5]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[6]);
    assertEquals(750, array[7]);
    assertEquals(1, array[8]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[9]);
    assertFalse(pts.needs_repair);
    
    // keep first
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 2),
        pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(true, false);
    assertEquals(9, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[0]);
    assertEquals(250, array[1]);
    assertEquals(2, array[2]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[3]);
    assertEquals(500, array[4]);
    assertEquals(42, array[5]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[6]);
    assertEquals(750, array[7]);
    assertEquals(24, array[8]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[9]);
    assertFalse(pts.needs_repair);
  }
  
  @Test
  public void dedupeSortedMilliSeconds() throws Exception {
    NumericPartialTimeSeries pts = new NumericPartialTimeSeries();
    
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 1),
        pool, 42, SET);
    assertFalse(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(6, pts.write_idx);
    assertEquals(BASE_TIME.msEpoch() + 250 + NumericLongArrayType.MILLISECOND_FLAG, array[0]);
    assertEquals(42, array[1]);
    assertEquals(BASE_TIME.msEpoch() + 500 + NumericLongArrayType.MILLISECOND_FLAG, array[2]);
    assertEquals(24, array[3]);
    assertEquals(BASE_TIME.msEpoch() + 750 + NumericLongArrayType.MILLISECOND_FLAG, array[4]);
    assertEquals(1, array[5]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[6]);
    assertFalse(pts.needs_repair);
  }
  
  @Test
  public void dedupeSortedNanoSeconds() throws Exception {
    NumericPartialTimeSeries pts = new NumericPartialTimeSeries();
    
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, -42),
        pool, 42, SET);
    assertFalse(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(9, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[0]);
    assertEquals(250, array[1]);
    assertEquals(42, array[2]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[3]);
    assertEquals(500, array[4]);
    assertEquals(24, array[5]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[6]);
    assertEquals(750, array[7]);
    assertEquals(-42, array[8]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[9]);
    assertFalse(pts.needs_repair);
  }
  
  @Test
  public void dedupeMixed() throws Exception {
    NumericPartialTimeSeries pts = new NumericPartialTimeSeries();
    
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, -24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 120000, -42),
        pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(10, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[0]);
    assertEquals(250, array[1]);
    assertEquals(42, array[2]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[3]);
    assertEquals(1000, array[4]);
    assertEquals(-24, array[5]);
    assertEquals((BASE_TIME.msEpoch() + 500) | NumericLongArrayType.MILLISECOND_FLAG, array[6]);
    assertEquals(1, array[7]);
    assertEquals((BASE_TIME.msEpoch() + 120_000L) | NumericLongArrayType.MILLISECOND_FLAG, array[8]);
    assertEquals(-42, array[9]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[10]);
    assertFalse(pts.needs_repair);
    
    // keep first
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, -24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 120000, -42),
        pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(true, false);
    assertEquals(10, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[0]);
    assertEquals(250, array[1]);
    assertEquals(42, array[2]);
    assertEquals(BASE_TIME.epoch() + NumericLongArrayType.NANOSECOND_FLAG, array[3]);
    assertEquals(1000, array[4]);
    assertEquals(-24, array[5]);
    assertEquals((BASE_TIME.msEpoch() + 500) | NumericLongArrayType.MILLISECOND_FLAG, array[6]);
    assertEquals(1, array[7]);
    assertEquals(BASE_TIME.epoch() + 120, array[8]);
    assertEquals(24, array[9]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[10]);
    assertFalse(pts.needs_repair);
    
    // all the same
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 120000000000L, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 120000L, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 120000000000L, -24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 120000, -42),
        pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(false, false);
    assertEquals(2, pts.write_idx);
    assertEquals((BASE_TIME.msEpoch() + 120_000) | NumericLongArrayType.MILLISECOND_FLAG, array[0]);
    assertEquals(-42, array[1]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[2]);
    assertFalse(pts.needs_repair);

    // keep earliest
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 120000000000L, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 120000L, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 120000000000L, -24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 120000, -42),
        pool, 42, SET);
    assertTrue(pts.needs_repair);
    
    pts.dedupe(true, false);
    assertEquals(3, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + 120 | NumericLongArrayType.NANOSECOND_FLAG, array[0]);
    assertEquals(0, array[1]);
    assertEquals(42, array[2]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[3]);
    assertFalse(pts.needs_repair);
  }

  @Test
  public void reverseOneCell() throws Exception {
    NumericPartialTimeSeries pts = new NumericPartialTimeSeries();
    assertFalse(pts.needs_repair);
    
    // no-op
    pts.dedupe(false, true);
    assertEquals(0, pts.write_idx);
    
    // seconds
    pts.addColumn((byte) 0, BASE_TIME,
        NumericCodec.buildSecondQualifier(0, (short) 0), new byte[] { 42 },
        pool, 42, SET);
    pts.dedupe(false, true);
    assertEquals(2, pts.write_idx);
    assertEquals(BASE_TIME.epoch(), array[0]);
    assertEquals(42, array[1]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[2]);
    
    // millis
    pts.data();
    pts.release();
    pts.addColumn((byte) 0, BASE_TIME, NumericCodec.buildMsQualifier(500, (short) 0), 
        new byte[] { 1 }, pool, 42, SET);
    pts.dedupe(false, true);
    assertEquals(2, pts.write_idx);
    assertEquals(BASE_TIME.msEpoch() + 500 | NumericLongArrayType.MILLISECOND_FLAG, array[0]);
    assertEquals(1, array[1]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[2]);
    
    // nanos
    pts.data();
    pts.release();
    pts.addColumn((byte) 0, BASE_TIME, NumericCodec.buildNanoQualifier(25000, (short) 0), 
        new byte[] { -1 }, pool, 42, SET);
    pts.dedupe(false, true);
    assertEquals(3, pts.write_idx);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, array[0]);
    assertEquals(25000, array[1]);
    assertEquals(-1, array[2]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[3]);
  }
  
  @Test
  public void reverseSeconds() throws Exception {
    NumericPartialTimeSeries pts = new NumericPartialTimeSeries();
    
    // two
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
        pool, 42, SET);
    pts.dedupe(false, true);
    assertEquals(4, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + 120, array[0]);
    assertEquals(24, array[1]);
    assertEquals(BASE_TIME.epoch() + 60, array[2]);
    assertEquals(42, array[3]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[4]);
    
    // three
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, -42),
        pool, 42, SET);
    pts.dedupe(false, true);
    assertEquals(6, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + 180, array[0]);
    assertEquals(-42, array[1]);
    assertEquals(BASE_TIME.epoch() + 120, array[2]);
    assertEquals(24, array[3]);
    assertEquals(BASE_TIME.epoch() + 60, array[4]);
    assertEquals(42, array[5]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[6]);
    
    // four
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, -42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 240, -24),
        pool, 42, SET);
    pts.dedupe(false, true);
    assertEquals(8, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + 240, array[0]);
    assertEquals(-24, array[1]);
    assertEquals(BASE_TIME.epoch() + 180, array[2]);
    assertEquals(-42, array[3]);
    assertEquals(BASE_TIME.epoch() + 120, array[4]);
    assertEquals(24, array[5]);
    assertEquals(BASE_TIME.epoch() + 60, array[6]);
    assertEquals(42, array[7]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[8]);
  }
  
  @Test
  public void reverseMilliSeconds() throws Exception {
    NumericPartialTimeSeries pts = new NumericPartialTimeSeries();
    
    // two
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 24),
        pool, 42, SET);
    pts.dedupe(false, true);
    assertEquals(4, pts.write_idx);
    assertEquals(BASE_TIME.msEpoch() + 500 | NumericLongArrayType.MILLISECOND_FLAG, array[0]);
    assertEquals(24, array[1]);
    assertEquals(BASE_TIME.msEpoch() + 250 | NumericLongArrayType.MILLISECOND_FLAG, array[2]);
    assertEquals(42, array[3]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[4]);
    
    // three
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, -42),
        pool, 42, SET);
    pts.dedupe(false, true);
    assertEquals(6, pts.write_idx);
    assertEquals(BASE_TIME.msEpoch() + 750 | NumericLongArrayType.MILLISECOND_FLAG, array[0]);
    assertEquals(-42, array[1]);
    assertEquals(BASE_TIME.msEpoch() + 500 | NumericLongArrayType.MILLISECOND_FLAG, array[2]);
    assertEquals(24, array[3]);
    assertEquals(BASE_TIME.msEpoch() + 250 | NumericLongArrayType.MILLISECOND_FLAG, array[4]);
    assertEquals(42, array[5]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[6]);
    
    // four
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, -42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 1000, -24),
        pool, 42, SET);
    pts.dedupe(false, true);
    assertEquals(8, pts.write_idx);
    assertEquals(BASE_TIME.msEpoch() + 1000 | NumericLongArrayType.MILLISECOND_FLAG, array[0]);
    assertEquals(-24, array[1]);
    assertEquals(BASE_TIME.msEpoch() + 750 | NumericLongArrayType.MILLISECOND_FLAG, array[2]);
    assertEquals(-42, array[3]);
    assertEquals(BASE_TIME.msEpoch() + 500 | NumericLongArrayType.MILLISECOND_FLAG, array[4]);
    assertEquals(24, array[5]);
    assertEquals(BASE_TIME.msEpoch() + 250 | NumericLongArrayType.MILLISECOND_FLAG, array[6]);
    assertEquals(42, array[7]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[8]);
  }
  
  @Test
  public void reverseNanoSeconds() throws Exception {
    NumericPartialTimeSeries pts = new NumericPartialTimeSeries();
    
    // two
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 24),
        pool, 42, SET);
    pts.dedupe(false, true);
    assertEquals(6, pts.write_idx);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, array[0]);
    assertEquals(500, array[1]);
    assertEquals(24, array[2]);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, array[3]);
    assertEquals(250, array[4]);
    assertEquals(42, array[5]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[6]);
    
    // three
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, -42),
        pool, 42, SET);
    pts.dedupe(false, true);
    assertEquals(9, pts.write_idx);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, array[0]);
    assertEquals(750, array[1]);
    assertEquals(-42, array[2]);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, array[3]);
    assertEquals(500, array[4]);
    assertEquals(24, array[5]);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, array[6]);
    assertEquals(250, array[7]);
    assertEquals(42, array[8]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[9]);
    
    // four
    pts.data();
    pts.release();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, -42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, -24),
        pool, 42, SET);
    pts.dedupe(false, true);
    assertEquals(12, pts.write_idx);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, array[0]);
    assertEquals(1000, array[1]);
    assertEquals(-24, array[2]);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, array[3]);
    assertEquals(750, array[4]);
    assertEquals(-42, array[5]);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, array[6]);
    assertEquals(500, array[7]);
    assertEquals(24, array[8]);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, array[9]);
    assertEquals(250, array[10]);
    assertEquals(42, array[11]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[12]);
  }

  @Test
  public void reverseMixed() throws Exception {
    NumericPartialTimeSeries pts = new NumericPartialTimeSeries();
    
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, -24),
        pool, 42, SET);
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, -42),
        pool, 42, SET);
    pts.dedupe(false, true);
    assertEquals(12, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + 180, array[0]);
    assertEquals(-42, array[1]);
    assertEquals(BASE_TIME.epoch() + 120, array[2]);
    assertEquals(24, array[3]);
    assertEquals(BASE_TIME.msEpoch() + 500 | NumericLongArrayType.MILLISECOND_FLAG, array[4]);
    assertEquals(1, array[5]);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, array[6]);
    assertEquals(1000, array[7]);
    assertEquals(-24, array[8]);
    assertEquals(BASE_TIME.epoch() | NumericLongArrayType.NANOSECOND_FLAG, array[9]);
    assertEquals(250, array[10]);
    assertEquals(42, array[11]);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, array[12]);
  }

  @Test
  public void growOnAdd() throws Exception {
    array = new long[3];
    when(object.object()).thenReturn(array);
    
    NumericPartialTimeSeries pts = new NumericPartialTimeSeries();
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42),
        pool, 42, SET);
    PooledObject array = pts.pooled_array;
    verify(object, never()).release();
    assertEquals(3, ((long[]) array.object()).length);
    
    pts.addColumn(Schema.APPENDS_PREFIX, BASE_TIME, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
        pool, 42, SET);
    verify(object, times(1)).release();
    assertNotSame(array, pts.pooled_array);
    
    assertEquals(4, pts.write_idx);
    assertEquals(BASE_TIME.epoch() + 60, ((long[]) pts.pooled_array.object())[0]);
    assertEquals(42, ((long[]) pts.pooled_array.object())[1]);
    assertEquals(BASE_TIME.epoch() + 120, ((long[]) pts.pooled_array.object())[2]);
    assertEquals(24, ((long[]) pts.pooled_array.object())[3]);
    assertEquals(0, ((long[]) pts.pooled_array.object())[4]); // no terminal
    assertEquals(18, ((long[])pts.pooled_array.object()).length);
    
    pts.dedupe(false, false);
    assertEquals(NumericLongArrayType.TERIMNAL_FLAG, 
        ((long[]) pts.pooled_array.object())[4]); // terminated
    assertEquals(18, ((long[])pts.pooled_array.object()).length);
  }
  
  @Test
  public void setEmpty() throws Exception {
    NumericPartialTimeSeries pts = new NumericPartialTimeSeries();
    pts.setEmpty(42,  SET);
    assertNull(pts.data());
    assertEquals(42, pts.idHash());
    assertSame(SET, pts.set());
  }
}
