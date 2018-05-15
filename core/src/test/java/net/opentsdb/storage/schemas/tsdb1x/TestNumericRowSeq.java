// This file is part of OpenTSDB.
// Copyright (C) 2010-2018  The OpenTSDB Authors.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.time.temporal.ChronoUnit;

import org.junit.Test;

import com.google.common.primitives.Bytes;

import net.opentsdb.storage.schemas.tsdb1x.NumericCodec;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.NumericRowSeq;
import net.opentsdb.storage.schemas.tsdb1x.NumericCodec.OffsetResolution;

public class TestNumericRowSeq {
  private static final long BASE_TIME = 1514764800;
  private static final byte[] APPEND_Q = 
      new byte[] { Schema.APPENDS_PREFIX, 0, 0 };
  
  @Test
  public void addColumnPuts() throws Exception {
    NumericRowSeq seq = new NumericRowSeq(BASE_TIME);
    assertEquals(BASE_TIME, seq.base_timestamp);
    assertNull(seq.data);
    assertEquals(NumericRowSeq.HEADER_SIZE, seq.size());
    assertEquals(0, seq.dataPoints());
    
    seq.addColumn((byte) 0, 
        NumericCodec.buildSecondQualifier(0, (short) 0), new byte[] { 42 });
    assertArrayEquals(Bytes.concat(
        NumericCodec.buildSecondQualifier(0, (short) 0), new byte[] { 42 }),
           seq.data);
    
    seq.addColumn((byte) 0, NumericCodec.buildSecondQualifier(60, (short) 0), 
        new byte[] { 24 });
    assertArrayEquals(Bytes.concat(
        NumericCodec.buildSecondQualifier(0, (short) 0), new byte[] { 42 },
        NumericCodec.buildSecondQualifier(60, (short) 0), new byte[] { 24 }),
           seq.data);
    
    seq.addColumn((byte) 0, NumericCodec.buildSecondQualifier(30, (short) 0), 
        new byte[] { 24 });
    assertArrayEquals(Bytes.concat(
        NumericCodec.buildSecondQualifier(0, (short) 0), new byte[] { 42 },
        NumericCodec.buildSecondQualifier(60, (short) 0), new byte[] { 24 },
        NumericCodec.buildSecondQualifier(30, (short) 0), new byte[] { 24 }),
           seq.data);
    
    seq.addColumn((byte) 0, NumericCodec.buildMsQualifier(500, (short) 0), 
        new byte[] { 1 });
    assertArrayEquals(Bytes.concat(
        NumericCodec.buildSecondQualifier(0, (short) 0), new byte[] { 42 },
        NumericCodec.buildSecondQualifier(60, (short) 0), new byte[] { 24 },
        NumericCodec.buildSecondQualifier(30, (short) 0), new byte[] { 24 },
        NumericCodec.buildMsQualifier(500, (short) 0), new byte[] { 1 }),
           seq.data);
    
    seq.addColumn((byte) 0, NumericCodec.buildNanoQualifier(25000, (short) 0), 
        new byte[] { -1 });
    assertArrayEquals(Bytes.concat(
        NumericCodec.buildSecondQualifier(0, (short) 0), new byte[] { 42 },
        NumericCodec.buildSecondQualifier(60, (short) 0), new byte[] { 24 },
        NumericCodec.buildSecondQualifier(30, (short) 0), new byte[] { 24 },
        NumericCodec.buildMsQualifier(500, (short) 0), new byte[] { 1 },
        NumericCodec.buildNanoQualifier(25000, (short) 0), new byte[] { -1 }),
           seq.data);
    
    assertEquals(NumericRowSeq.HEADER_SIZE + 23, seq.size());
    assertEquals(0, seq.dataPoints());
    // not incremented as we don't call dedupe.
  }
  
  @Test
  public void addColumnAppends() throws Exception {
    NumericRowSeq seq = new NumericRowSeq(BASE_TIME);
    assertEquals(BASE_TIME, seq.base_timestamp);
    assertNull(seq.data);
    
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 42));
    assertArrayEquals(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 0, 42), seq.data);
    
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 24));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 0, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 24)), 
          seq.data);
    
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 0, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 24),
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1)), 
          seq.data);
    
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 25000, -1));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 0, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 24),
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1),
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 25000, -1)), 
          seq.data);
  }
  
  @Test
  public void addColumnMixed() throws Exception {
    NumericRowSeq seq = new NumericRowSeq(BASE_TIME);
    assertEquals(BASE_TIME, seq.base_timestamp);
    assertNull(seq.data);
    
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 42));
    assertArrayEquals(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 0, 42), seq.data);
    
    seq.addColumn((byte) 0, NumericCodec.buildSecondQualifier(60, (short) 0), 
        new byte[] { 24 });
    assertArrayEquals(Bytes.concat(
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 42),
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 24)),
           seq.data);
    
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 0, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 24),
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1)), 
          seq.data);
    
    seq.addColumn((byte) 0, NumericCodec.buildNanoQualifier(25000, (short) 0), 
        new byte[] { -1 });
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 0, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 24),
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1),
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 25000, -1)), 
          seq.data);
  }
  
  @Test
  public void addColumnExceptionsAndBadData() throws Exception {
    NumericRowSeq seq = new NumericRowSeq(BASE_TIME);
    try {
      seq.addColumn((byte) 0, null, new byte[] { 42 });
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
    
    try {
      seq.addColumn((byte) 0, NumericCodec.buildSecondQualifier(0, (short) 0), null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
    
    // WATCH OUT!!
    seq.addColumn((byte) 0, new byte[0], new byte[] { 42 });
    assertArrayEquals(new byte[0], seq.data);
    
    // thinks it's a millisecond offset
    try {
      seq.addColumn((byte) 0, new byte[] { (byte) 0xF0 }, new byte[] { 42 });
      fail("Expected ArrayIndexOutOfBoundsException");
    } catch (ArrayIndexOutOfBoundsException e) { }
    
    // WATCH OUT!!
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn((byte) 0, 
        NumericCodec.buildSecondQualifier(0, (short) 0), new byte[0]);
    assertArrayEquals(new byte[] { 0, 0 }, seq.data);
  }
  
  @Test
  public void addColumnCompactedSeconds() throws Exception {
    byte[] compacted_value = Bytes.concat(
        NumericCodec.vleEncodeLong(42), 
        NumericCodec.vleEncodeLong(24),
        NumericCodec.vleEncodeLong(1),
        NumericCodec.vleEncodeLong(-1),
        new byte[1]
    );
    byte[] compacted_qualifier = Bytes.concat(
        NumericCodec.buildSecondQualifier(0, (short) 0),
        NumericCodec.buildSecondQualifier(60, (short) 0),
        NumericCodec.buildSecondQualifier(120, (short) 0),
        NumericCodec.buildSecondQualifier(180, (short) 0)
    );
    
    NumericRowSeq seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn((byte) 0, compacted_qualifier, compacted_value);
    
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 0, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 24),
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 1),
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, -1)), 
          seq.data);
    
    seq.dedupe(false, false);
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 0, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 24),
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 1),
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, -1)), 
          seq.data);
    
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 42));
    seq.addColumn((byte) 0, compacted_qualifier, compacted_value);
    
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 0, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 42),
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 24),
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 1),
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, -1)), 
          seq.data);
  }
  
  @Test
  public void addColumnCompactedMixed() throws Exception {
    byte[] compacted_value = Bytes.concat(
        NumericCodec.vleEncodeLong(42), 
        NumericCodec.vleEncodeLong(24),
        NumericCodec.vleEncodeLong(1),
        NumericCodec.vleEncodeLong(-1),
        new byte[] { NumericCodec.MS_MIXED_COMPACT }
    );
    byte[] compacted_qualifier = Bytes.concat(
        NumericCodec.buildSecondQualifier(0, (short) 0),
        NumericCodec.buildMsQualifier(60000, (short) 0),
        NumericCodec.buildSecondQualifier(120, (short) 0),
        NumericCodec.buildMsQualifier(180000, (short) 0)
    );
    
    NumericRowSeq seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn((byte) 0, compacted_qualifier, compacted_value);
    
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 0, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 60000, 24),
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 1),
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 180000, -1)), 
          seq.data);
    
    seq.dedupe(false, false);
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 0, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 60000, 24),
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 1),
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 180000, -1)), 
          seq.data);
    
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 42));
    seq.addColumn((byte) 0, compacted_qualifier, compacted_value);
    
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 0, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 42),
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 60000, 24),
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 1),
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 180000, -1)), 
          seq.data);
  }
  
  @Test
  public void addColumnFixOldTSDIssues() throws Exception {
    NumericRowSeq seq = new NumericRowSeq(BASE_TIME);
    
    seq.addColumn((byte) 0, NumericCodec.buildSecondQualifier(500, (short) 1),
        net.opentsdb.utils.Bytes.fromLong(42));
    assertArrayEquals(Bytes.concat(
        NumericCodec.buildSecondQualifier(500, (short) 7), 
        net.opentsdb.utils.Bytes.fromLong(42)),
          seq.data);
    
    seq.addColumn((byte) 0, NumericCodec.buildSecondQualifier(560, (short) 0),
        net.opentsdb.utils.Bytes.fromLong(24));
    assertArrayEquals(Bytes.concat(
        NumericCodec.buildSecondQualifier(500, (short) 7), 
        net.opentsdb.utils.Bytes.fromLong(42),
        NumericCodec.buildSecondQualifier(560, (short) 7), 
        net.opentsdb.utils.Bytes.fromLong(24)),
          seq.data);
    
    seq.addColumn((byte) 0, NumericCodec.buildSecondQualifier(620, (short) 7),
        net.opentsdb.utils.Bytes.fromInt(1));
    assertArrayEquals(Bytes.concat(
        NumericCodec.buildSecondQualifier(500, (short) 7), 
        net.opentsdb.utils.Bytes.fromLong(42),
        NumericCodec.buildSecondQualifier(560, (short) 7), 
        net.opentsdb.utils.Bytes.fromLong(24),
        NumericCodec.buildSecondQualifier(620, (short) 3), 
        net.opentsdb.utils.Bytes.fromInt(1)),
          seq.data);
    
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn((byte) 0, NumericCodec.buildSecondQualifier(500, (short) (7 | NumericCodec.FLAG_FLOAT)),
        net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F)));
    assertArrayEquals(Bytes.concat(
        NumericCodec.buildSecondQualifier(500, (short) (3 | NumericCodec.FLAG_FLOAT)), 
        net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F))),
          seq.data);
    
    // old style incorrect length floating point value
    seq.addColumn((byte) 0, NumericCodec.buildSecondQualifier(560, (short) (3 | NumericCodec.FLAG_FLOAT)),
        Bytes.concat(new byte[4], 
            net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(24.5F))));
    assertArrayEquals(Bytes.concat(
        NumericCodec.buildSecondQualifier(500, (short) (3 | NumericCodec.FLAG_FLOAT)), 
        net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F)),
        NumericCodec.buildSecondQualifier(560, (short) (3 | NumericCodec.FLAG_FLOAT)), 
        net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(24.5F))),
          seq.data);
  }
  
  @Test
  public void dedupeSortedSeconds() throws Exception {
    NumericRowSeq seq = new NumericRowSeq(BASE_TIME);
    
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, 1));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
          OffsetResolution.SECONDS, 60, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, 1)), 
          seq.data);
    
    assertEquals(ChronoUnit.SECONDS, seq.dedupe(false, false));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 60, 42), 
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, 1)), 
        seq.data);
    
    assertEquals(ChronoUnit.SECONDS, seq.dedupe(false, true));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 180, 1), 
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42)), 
        seq.data);
    assertEquals(3, seq.dataPoints());
  }
  
  @Test
  public void dedupeSeconds() throws Exception {
    NumericRowSeq seq = new NumericRowSeq(BASE_TIME);
    
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
          OffsetResolution.SECONDS, 120, 24), 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42),
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, 1),
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2)), 
          seq.data);
    
    assertEquals(ChronoUnit.SECONDS, seq.dedupe(false, false));
    
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 30, 2),
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42), 
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, 1)), 
        seq.data);
    assertEquals(4, seq.dataPoints());
    
    // reset to reverse in the tree
    seq = new NumericRowSeq(BASE_TIME);    
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2));
    
    assertEquals(ChronoUnit.SECONDS, seq.dedupe(false, true));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 180, 1), 
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42),
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2)), 
        seq.data);
    assertEquals(4, seq.dataPoints());
    
    // consecutive dupe
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2));
    
    assertEquals(ChronoUnit.SECONDS, seq.dedupe(false, false));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 30, 2),
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 1), 
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24)), 
        seq.data);
    assertEquals(3, seq.dataPoints());
    
    // keep first
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2));
    
    assertEquals(ChronoUnit.SECONDS, seq.dedupe(true, false));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 30, 2),
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42), 
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24)), 
        seq.data);
    assertEquals(3, seq.dataPoints());
    
    // non-consecutive dupe
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2));
    
    assertEquals(ChronoUnit.SECONDS, seq.dedupe(false, false));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 30, 2),
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42), 
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 1)), 
        seq.data);
    assertEquals(3, seq.dataPoints());
    
    // keep first
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2));
    
    assertEquals(ChronoUnit.SECONDS, seq.dedupe(true, false));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 30, 2),
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42), 
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24)), 
        seq.data);
    assertEquals(3, seq.dataPoints());
  }
  
  @Test
  public void dedupeMilliSeconds() throws Exception {
    NumericRowSeq seq = new NumericRowSeq(BASE_TIME);
    
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 1000, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 2));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
          OffsetResolution.MILLIS, 750, 24), 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42),
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 1000, 1),
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 2)), 
          seq.data);
    
    assertEquals(ChronoUnit.MILLIS, seq.dedupe(false, false));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.MILLIS, 250, 2),
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42), 
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24),
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 1000, 1)), 
        seq.data);
    assertEquals(4, seq.dataPoints());
    
    // reset to reverse in the tree
    seq = new NumericRowSeq(BASE_TIME);    
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 1000, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 2));
    
    assertEquals(ChronoUnit.MILLIS, seq.dedupe(false, true));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.MILLIS, 1000, 1), 
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24),
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42),
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 2)), 
        seq.data);
    assertEquals(4, seq.dataPoints());
    
    // consecutive dupe
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 2));
    
    assertEquals(ChronoUnit.MILLIS, seq.dedupe(false, false));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.MILLIS, 250, 2),
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1), 
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24)), 
        seq.data);
    assertEquals(3, seq.dataPoints());
    
    // keep first
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 2));
    
    assertEquals(ChronoUnit.MILLIS, seq.dedupe(true, false));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.MILLIS, 250, 2),
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42), 
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24)), 
        seq.data);
    assertEquals(3, seq.dataPoints());
    
    // non-consecutive dupe
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 2));
    
    assertEquals(ChronoUnit.MILLIS, seq.dedupe(false, false));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.MILLIS, 250, 2),
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42), 
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 1)), 
        seq.data);
    assertEquals(3, seq.dataPoints());
    
    // keep first
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 2));
    
    assertEquals(ChronoUnit.MILLIS, seq.dedupe(true, false));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.MILLIS, 250, 2),
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 42), 
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 24)), 
        seq.data);
    assertEquals(3, seq.dataPoints());
  }
  
  @Test
  public void dedupeNanoSeconds() throws Exception {
    NumericRowSeq seq = new NumericRowSeq(BASE_TIME);
    
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 2));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
          OffsetResolution.NANOS, 750, 24), 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42),
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, 1),
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 2)), 
          seq.data);
    
    assertEquals(ChronoUnit.NANOS, seq.dedupe(false, false));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.NANOS, 250, 2),
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42), 
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24),
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, 1)), 
        seq.data);
    assertEquals(4, seq.dataPoints());
    
    // reset to reverse in the tree
    seq = new NumericRowSeq(BASE_TIME);    
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 2));
    
    assertEquals(ChronoUnit.NANOS, seq.dedupe(false, true));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.NANOS, 1000, 1), 
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24),
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42),
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 2)), 
        seq.data);
    assertEquals(4, seq.dataPoints());
    
    // consecutive dupe
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 2));
    
    assertEquals(ChronoUnit.NANOS, seq.dedupe(false, false));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.NANOS, 250, 2),
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 1), 
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24)), 
        seq.data);
    assertEquals(3, seq.dataPoints());
    
    // keep first
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 2));
    
    assertEquals(ChronoUnit.NANOS, seq.dedupe(true, false));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.NANOS, 250, 2),
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42), 
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24)), 
        seq.data);
    assertEquals(3, seq.dataPoints());
    
    // non-consecutive dupe
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 2));
    
    assertEquals(ChronoUnit.NANOS, seq.dedupe(false, false));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.NANOS, 250, 2),
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42), 
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 1)), 
        seq.data);
    assertEquals(3, seq.dataPoints());
    
    // keep first
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 2));
    
    assertEquals(ChronoUnit.NANOS, seq.dedupe(true, false));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.NANOS, 250, 2),
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 42), 
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, 24)), 
        seq.data);
    assertEquals(3, seq.dataPoints());
  }
  
  @Test
  public void dedupeSortedMilliSeconds() throws Exception {
    NumericRowSeq seq = new NumericRowSeq(BASE_TIME);
    
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 1));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
          OffsetResolution.MILLIS, 250, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 24),
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 1)), 
          seq.data);
    
    assertEquals(ChronoUnit.MILLIS, seq.dedupe(false, false));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.MILLIS, 250, 42), 
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 24),
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, 1)), 
        seq.data);
    
    assertEquals(ChronoUnit.MILLIS, seq.dedupe(false, true));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.MILLIS, 750, 1), 
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 24),
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 42)), 
        seq.data);
  }
  
  @Test
  public void dedupeSortedNanoSeconds() throws Exception {
    NumericRowSeq seq = new NumericRowSeq(BASE_TIME);
    
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, -42));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
          OffsetResolution.NANOS, 250, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 24),
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, -42)), 
          seq.data);
    
    assertEquals(ChronoUnit.NANOS, seq.dedupe(false, false));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.NANOS, 250, 42), 
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 24),
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, -42)), 
        seq.data);
    
    assertEquals(ChronoUnit.NANOS, seq.dedupe(false, true));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.NANOS, 750, -42), 
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 24),
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42)), 
        seq.data);
  }
  
  @Test
  public void dedupeMixed() throws Exception {
    NumericRowSeq seq = new NumericRowSeq(BASE_TIME);
    
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, -24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 120000, -42));
    
    assertEquals(ChronoUnit.NANOS, seq.dedupe(false, false));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.NANOS, 250, 42), 
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, -24),
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1),
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 120000, -42)), 
        seq.data);
    
    // keep first
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, -24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 120000, -42));
    
    assertEquals(ChronoUnit.NANOS, seq.dedupe(true, false));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.NANOS, 250, 42), 
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, -24),
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1),
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24)), 
        seq.data);
    
    // all the same
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 120000000000L, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 120000L, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 120000000000L, -24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 120000, -42));
    
    assertEquals(ChronoUnit.NANOS, seq.dedupe(false, false));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.MILLIS, 120000, -42)), 
        seq.data);
    
    // keep earliest
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 120000000000L, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 120000L, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 120000000000L, -24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 120000, -42));
    
    assertEquals(ChronoUnit.NANOS, seq.dedupe(true, false));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.NANOS, 120000000000L, 42)), 
        seq.data);
  }
  
  @Test
  public void reverseOneCell() throws Exception {
    NumericRowSeq seq = new NumericRowSeq(BASE_TIME);
    assertNull(seq.data);
    
    // no-op
    seq.reverse();
    assertNull(seq.data);
    
    // seconds
    seq.addColumn((byte) 0, 
        NumericCodec.buildSecondQualifier(0, (short) 0), new byte[] { 42 });
    byte[] data = seq.data;
    seq.reverse();
    assertNotSame(data, seq.data);
    assertArrayEquals(NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 42), 
        seq.data);
    
    // millis
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn((byte) 0, NumericCodec.buildMsQualifier(500, (short) 0), 
        new byte[] { 1 });
    data = seq.data;
    seq.reverse();
    assertNotSame(data, seq.data);
    assertArrayEquals(NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1), 
        seq.data);
    
    // nanos
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn((byte) 0, NumericCodec.buildNanoQualifier(25000, (short) 0), 
        new byte[] { -1 });
    data = seq.data;
    seq.reverse();
    assertNotSame(data, seq.data);
    assertArrayEquals(NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 25000, -1), 
        seq.data);
  }
  
  @Test
  public void reverseSeconds() throws Exception {
    NumericRowSeq seq = new NumericRowSeq(BASE_TIME);
    
    // two
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
          OffsetResolution.SECONDS, 60, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24)), 
          seq.data);
    
    seq.reverse();
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 120, 24), 
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42)), 
        seq.data);
    
    // three
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, -42));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
          OffsetResolution.SECONDS, 60, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, -42)), 
          seq.data);
    
    seq.reverse();
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 180, -42),
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24), 
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42)), 
        seq.data);
    
    // four
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, -42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 240, -24));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
          OffsetResolution.SECONDS, 60, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, -42),
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 240, -24)), 
          seq.data);
    
    seq.reverse();
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 240, -24),
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, -42),
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24), 
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42)), 
        seq.data);
  }
  
  @Test
  public void reverseMilliSeconds() throws Exception {
    NumericRowSeq seq = new NumericRowSeq(BASE_TIME);
    
    // two
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 24));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
          OffsetResolution.MILLIS, 250, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 24)), 
          seq.data);
    
    seq.reverse();
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.MILLIS, 500, 24), 
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 42)), 
        seq.data);
    
    // three
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, -42));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
          OffsetResolution.MILLIS, 250, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 24),
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, -42)), 
          seq.data);
    
    seq.reverse();
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.MILLIS, 750, -42),
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 24), 
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 42)), 
        seq.data);
    
    // four
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, -42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 1000, -24));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
          OffsetResolution.MILLIS, 250, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 24),
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, -42),
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 1000, -24)), 
          seq.data);
    
    seq.reverse();
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.MILLIS, 1000, -24),
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 750, -42),
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 24), 
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 250, 42)), 
        seq.data);
  }
  
  @Test
  public void reverseNanoSeconds() throws Exception {
    NumericRowSeq seq = new NumericRowSeq(BASE_TIME);
    
    // two
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 24));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
          OffsetResolution.NANOS, 250, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 24)), 
          seq.data);
    
    seq.reverse();
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.NANOS, 500, 24), 
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42)), 
        seq.data);
    
    // three
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, -42));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
          OffsetResolution.NANOS, 250, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 24),
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, -42)), 
          seq.data);
    
    seq.reverse();
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.NANOS, 750, -42),
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 24), 
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42)), 
        seq.data);
    
    // four
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, -42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, -24));
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
          OffsetResolution.NANOS, 250, 42), 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 24),
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, -42),
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, -24)), 
          seq.data);
    
    seq.reverse();
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.NANOS, 1000, -24),
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 750, -42),
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 500, 24), 
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42)), 
        seq.data);
  }

  @Test
  public void reverseMixed() throws Exception {
    NumericRowSeq seq = new NumericRowSeq(BASE_TIME);
    
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, -24));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, -42));
    
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.NANOS, 250, 42), 
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1),
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, -24),
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, -42)), 
        seq.data);
    
    seq.reverse();
    assertArrayEquals(Bytes.concat(NumericCodec.encodeAppendValue(
        OffsetResolution.SECONDS, 180, -42), 
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1000, -24),
      NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 1),
      NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 24),
      NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 250, 42)), 
        seq.data);
  }

}
