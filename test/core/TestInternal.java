// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;

import net.opentsdb.core.Internal.Cell;
import net.opentsdb.storage.MockBase;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Internal.class, Const.class })
public final class TestInternal {
  private static final byte[] KEY = 
    { 0, 0, 1, 0x50, (byte)0xE2, 0x27, 0, 0, 0, 1, 0, 0, 2 };
  private static final byte[] FAMILY = { 't' };
  private static final byte[] ZERO = { 0 };

  @Test
  public void extractDataPointsFixQualifierFlags() {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromInt(5);
    final byte[] qual3 = { 0x00, 0x43 };
    final byte[] val3 = Bytes.fromLong(6L);
    
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(3);
    row.add(makekv(qual1, val1));
    row.add(makekv(qual2, val2));
    row.add(makekv(qual3, val3));
    
    final ArrayList<Cell> cells = Internal.extractDataPoints(row, 3);
    assertEquals(3, cells.size());
    assertArrayEquals(new byte[] { 0x00, 0x07 }, cells.get(0).qualifier);
    assertArrayEquals(Bytes.fromLong(4L), cells.get(0).value);
    assertArrayEquals(new byte[] { 0x00, 0x23 }, cells.get(1).qualifier);
    assertArrayEquals(Bytes.fromInt(5), cells.get(1).value);
    assertArrayEquals(new byte[] { 0x00, 0x47 }, cells.get(2).qualifier);
    assertArrayEquals(Bytes.fromLong(6L), cells.get(2).value);
  }
  
  @Test
  public void extractDataPointsFixFloatingPointValue() {
    final byte[] qual1 = { 0x00, 0x0F };
    final byte[] val1 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 };
    final byte[] qual2 = { 0x00, 0x2B };
    final byte[] val2 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 };
    final byte[] qual3 = { 0x00, 0x4B };
    final byte[] val3 = new byte[] { 0, 0, 0, 1 };
    
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(3);
    row.add(makekv(qual1, val1));
    row.add(makekv(qual2, val2));
    row.add(makekv(qual3, val3));
    
    final ArrayList<Cell> cells = Internal.extractDataPoints(row, 3);
    assertEquals(3, cells.size());
    assertArrayEquals(new byte[] { 0x00, 0x0F }, cells.get(0).qualifier);
    assertArrayEquals(new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 }, cells.get(0).value);
    assertArrayEquals(new byte[] { 0x00, 0x2B }, cells.get(1).qualifier);
    assertArrayEquals(new byte[] { 0, 0, 0, 1 }, cells.get(1).value);
    assertArrayEquals(new byte[] { 0x00, 0x4B }, cells.get(2).qualifier);
    assertArrayEquals(new byte[] { 0, 0, 0, 1 }, cells.get(2).value);
  }
  
  @Test (expected = IllegalDataException.class)
  public void extractDataPointsFixFloatingPointValueCorrupt() {
    final byte[] qual1 = { 0x00, 0x0F };
    final byte[] val1 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 };
    final byte[] qual2 = { 0x00, 0x2B };
    final byte[] val2 = new byte[] { 0, 2, 0, 0, 0, 0, 0, 1 };
    final byte[] qual3 = { 0x00, 0x4B };
    final byte[] val3 = new byte[] { 0, 0, 0, 1 };
    
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(3);
    row.add(makekv(qual1, val1));
    row.add(makekv(qual2, val2));
    row.add(makekv(qual3, val3));
    
    Internal.extractDataPoints(row, 3);
  }
  
  @Test
  public void extractDataPointsMixSecondsMs() {
    final byte[] qual1 = { 0x00, 0x27 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x01, 0x00, 0x02 };
    final byte[] val2 = "Annotation".getBytes(MockBase.ASCII());
    final byte[] qual3 = { 0x00, 0x47 };
    final byte[] val3 = Bytes.fromLong(6L);
    
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(3);
    row.add(makekv(qual1, val1));
    row.add(makekv(qual2, val2));
    row.add(makekv(qual3, val3));
    
    final ArrayList<Cell> cells = Internal.extractDataPoints(row, 3);
    assertEquals(2, cells.size());
    assertArrayEquals(new byte[] { 0x00, 0x27 }, cells.get(0).qualifier);
    assertArrayEquals(new byte[] { 0x00, 0x47 }, cells.get(1).qualifier);
  }
  
  @Test
  public void extractDataPointsWithNonDataColumns() {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x00, 0x47 };
    final byte[] val3 = Bytes.fromLong(6L);
    
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(3);
    row.add(makekv(qual1, val1));
    row.add(makekv(qual2, val2));
    row.add(makekv(qual3, val3));
    
    final ArrayList<Cell> cells = Internal.extractDataPoints(row, 3);
    assertEquals(3, cells.size());
    assertArrayEquals(new byte[] { 0x00, 0x07 }, cells.get(0).qualifier);
    assertArrayEquals(new byte[] { (byte) 0xF0, 0x00, 0x02, 0x07 }, 
        cells.get(1).qualifier);
    assertArrayEquals(new byte[] { 0x00, 0x47 }, cells.get(2).qualifier);
  }

  @Test
  public void extractDataPointsWithNonDataColumnsSort() {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x00, 0x47 };
    final byte[] val3 = Bytes.fromLong(6L);
    
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(3);
    row.add(makekv(qual3, val3));
    row.add(makekv(qual2, val2));
    row.add(makekv(qual1, val1));
    
    final ArrayList<Cell> cells = Internal.extractDataPoints(row, 3);
    assertEquals(3, cells.size());
    assertArrayEquals(new byte[] { 0x00, 0x07 }, cells.get(0).qualifier);
    assertArrayEquals(new byte[] { (byte) 0xF0, 0x00, 0x02, 0x07 }, 
        cells.get(1).qualifier);
    assertArrayEquals(new byte[] { 0x00, 0x47 }, cells.get(2).qualifier);
  }
  
  @Test
  public void extractDataPointsCompactSeconds() {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x00, 0x47 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual123 = MockBase.concatByteArrays(qual1, qual2, qual3);
    final byte[] val123 = MockBase.concatByteArrays(val1, val2, val3, ZERO);
    
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    row.add(makekv(qual123, val123));
    
    final ArrayList<Cell> cells = Internal.extractDataPoints(row, 1);
    assertEquals(3, cells.size());
    assertArrayEquals(new byte[] { 0x00, 0x07 }, cells.get(0).qualifier);
    assertArrayEquals(Bytes.fromLong(4L), cells.get(0).value);
    assertArrayEquals(new byte[] { 0x00, 0x27 }, cells.get(1).qualifier);
    assertArrayEquals(Bytes.fromLong(5L), cells.get(1).value);
    assertArrayEquals(new byte[] { 0x00, 0x47 }, cells.get(2).qualifier);
    assertArrayEquals(Bytes.fromLong(6L), cells.get(2).value);
  }
  
  @Test
  public void extractDataPointsCompactSecondsSorting() {
    final byte[] qual1 = { 0x00, 0x47 };
    final byte[] val1 = Bytes.fromLong(6L);
    final byte[] qual2 = { 0x00, 0x07 };
    final byte[] val2 = Bytes.fromLong(4L);
    final byte[] qual3 = { 0x00, 0x27 };
    final byte[] val3 = Bytes.fromLong(5L);
    final byte[] qual123 = MockBase.concatByteArrays(qual1, qual2, qual3);
    final byte[] val123 = MockBase.concatByteArrays(val1, val2, val3, ZERO);
    
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    row.add(makekv(qual123, val123));
    
    final ArrayList<Cell> cells = Internal.extractDataPoints(row, 1);
    assertEquals(3, cells.size());
    assertArrayEquals(new byte[] { 0x00, 0x07 }, cells.get(0).qualifier);
    assertArrayEquals(Bytes.fromLong(4L), cells.get(0).value);
    assertArrayEquals(new byte[] { 0x00, 0x27 }, cells.get(1).qualifier);
    assertArrayEquals(Bytes.fromLong(5L), cells.get(1).value);
    assertArrayEquals(new byte[] { 0x00, 0x47 }, cells.get(2).qualifier);
    assertArrayEquals(Bytes.fromLong(6L), cells.get(2).value);
  }
  
  @Test
  public void extractDataPointsCompactMs() {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x07, 0x07 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual123 = MockBase.concatByteArrays(qual1, qual2, qual3);
    final byte[] val123 = MockBase.concatByteArrays(val1, val2, val3, ZERO);
    
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    row.add(makekv(qual123, val123));
    
    final ArrayList<Cell> cells = Internal.extractDataPoints(row, 1);
    assertEquals(3, cells.size());
    assertArrayEquals(new byte[] { (byte) 0xF0, 0x00, 0x00, 0x07 }, 
        cells.get(0).qualifier);
    assertArrayEquals(Bytes.fromLong(4L), cells.get(0).value);
    assertArrayEquals(new byte[] { (byte) 0xF0, 0x00, 0x02, 0x07 }, 
        cells.get(1).qualifier);
    assertArrayEquals(Bytes.fromLong(5L), cells.get(1).value);
    assertArrayEquals(new byte[] { (byte) 0xF0, 0x00, 0x07, 0x07 }, 
        cells.get(2).qualifier);
    assertArrayEquals(Bytes.fromLong(6L), cells.get(2).value);
  }
  
  @Test
  public void extractDataPointsCompactSecAndMs() {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x00, 0x47 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual123 = MockBase.concatByteArrays(qual1, qual2, qual3);
    final byte[] val123 = MockBase.concatByteArrays(val1, val2, val3, ZERO);
    
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    row.add(makekv(qual123, val123));
    
    final ArrayList<Cell> cells = Internal.extractDataPoints(row, 1);
    assertEquals(3, cells.size());
    assertArrayEquals(new byte[] { 0x00, 0x07 }, cells.get(0).qualifier);
    assertArrayEquals(Bytes.fromLong(4L), cells.get(0).value);
    assertArrayEquals(new byte[] { (byte) 0xF0, 0x00, 0x02, 0x07 }, 
        cells.get(1).qualifier);
    assertArrayEquals(Bytes.fromLong(5L), cells.get(1).value);
    assertArrayEquals(new byte[] { 0x00, 0x47 }, cells.get(2).qualifier);
    assertArrayEquals(Bytes.fromLong(6L), cells.get(2).value);
  }
  
  @Test (expected = IllegalDataException.class)
  public void extractDataPointsCompactCorrupt() {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x00, 0x41 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual123 = MockBase.concatByteArrays(qual1, qual2, qual3);
    final byte[] val123 = MockBase.concatByteArrays(val1, val2, val3, ZERO);
    
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    row.add(makekv(qual123, val123));
    
    Internal.extractDataPoints(row, 1);
  }
  
  @Test
  public void compareQualifiersLTSecInt() {
    assertEquals(-1, Internal.compareQualifiers(new byte[] {0x00, 0x27}, 0,
        new byte[] {0x00, 0x37}, 0));
  }
  
  @Test
  public void compareQualifiersGTSecInt() {
    assertEquals(1, Internal.compareQualifiers(new byte[] {0x00, 0x37}, 0,
        new byte[] {0x00, 0x27}, 0));
  }
  
  @Test
  public void compareQualifiersEQSecInt() {
    assertEquals(0, Internal.compareQualifiers(new byte[] {0x00, 0x27}, 0,
        new byte[] {0x00, 0x27}, 0));
  }

  @Test
  public void compareQualifiersLTSecIntAndFloat() {
    assertEquals(-1, Internal.compareQualifiers(new byte[] {0x00, 0x27}, 0,
        new byte[] {0x00, 0x3B}, 0));
  }
  
  @Test
  public void compareQualifiersGTSecIntAndFloat() {
    assertEquals(1, Internal.compareQualifiers(new byte[] {0x00, 0x37}, 0,
        new byte[] {0x00, 0x2B}, 0));
  }
  
  @Test
  public void compareQualifiersEQSecIntAndFloat() {
    assertEquals(0, Internal.compareQualifiers(new byte[] {0x00, 0x27}, 0,
        new byte[] {0x00, 0x2B}, 0));
  }

  public void compareQualifiersLTMsInt() {
    assertEquals(-1, Internal.compareQualifiers(
        new byte[] { (byte) 0xF0, 0x00, 0x02, 0x07 }, 0, 
        new byte[] { (byte) 0xF0, 0x00, 0x07, 0x07 }, 0));
  }
  
  @Test
  public void compareQualifiersGTMsInt() {
    assertEquals(1, Internal.compareQualifiers(
        new byte[] { (byte) 0xF0, 0x00, 0x07, 0x07 }, 0,
        new byte[] { (byte) 0xF0, 0x00, 0x02, 0x07 }, 0));
  }
  
  @Test
  public void compareQualifiersEQMsInt() {
    assertEquals(0, Internal.compareQualifiers(
        new byte[] { (byte) 0xF0, 0x00, 0x07, 0x07 }, 0, 
        new byte[] { (byte) 0xF0, 0x00, 0x07, 0x07 }, 0));
  }
  
  public void compareQualifiersLTMsIntAndFloat() {
    assertEquals(-1, Internal.compareQualifiers(
        new byte[] { (byte) 0xF0, 0x00, 0x02, 0x07 }, 0,
        new byte[] { (byte) 0xF0, 0x00, 0x07, 0x0B }, 0));
  }
  
  @Test
  public void compareQualifiersGTMsIntAndFloat() {
    assertEquals(1, Internal.compareQualifiers(
        new byte[] { (byte) 0xF0, 0x00, 0x07, 0x07 }, 0,
        new byte[] { (byte) 0xF0, 0x00, 0x02, 0x0B }, 0));
  }
  
  @Test
  public void compareQualifiersEQMsIntAndFloat() {
    assertEquals(0, Internal.compareQualifiers(
        new byte[] { (byte) 0xF0, 0x00, 0x07, 0x07 }, 0, 
        new byte[] { (byte) 0xF0, 0x00, 0x07, 0x0B }, 0));
  }
  
  @Test
  public void compareQualifiersLTMsAndSecond() {
    assertEquals(-1, Internal.compareQualifiers(
        new byte[] { (byte) 0xF0, 0x00, 0x07, 0x0B }, 0, 
        new byte[] { 0x00, 0x27}, 0));
  }
  
  @Test
  public void compareQualifiersGTMsAndSecond() {
    assertEquals(1, Internal.compareQualifiers(new byte[] { 0x00, 0x27}, 0, 
        new byte[] { (byte) 0xF0, 0x00, 0x07, 0x0B }, 0));
  }
  
  @Test
  public void compareQualifiersEQMsAndSecond() {
    assertEquals(0, Internal.compareQualifiers(new byte[] { 0x00, 0x27}, 0, 
        new byte[] { (byte) 0xF0, 0x01, (byte) 0xF4, 0x0B }, 0));
  }
  
  @Test
  public void fixQualifierFlags() {
    assertEquals(0x0B, Internal.fixQualifierFlags((byte) 0x0F, 4));
  }
  
  @Test
  public void floatingPointValueToFix() {
    assertTrue(Internal.floatingPointValueToFix((byte) 0x0B, 
        new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 }));
  }
  
  @Test
  public void floatingPointValueToFixNot() {
    assertFalse(Internal.floatingPointValueToFix((byte) 0x0B, 
        new byte[] { 0, 0, 0, 1 }));
  }
  
  @Test
  public void fixFloatingPointValue() {
    assertArrayEquals(new byte[] { 0, 0, 0, 1 }, 
        Internal.fixFloatingPointValue((byte) 0x0B, 
            new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 }));
  }
  
  @Test
  public void fixFloatingPointValueNot() {
    assertArrayEquals(new byte[] { 0, 0, 0, 1 }, 
        Internal.fixFloatingPointValue((byte) 0x0B, 
            new byte[] { 0, 0, 0, 1 }));
  }
  
  @Test
  public void fixFloatingPointValueWasInt() {
    assertArrayEquals(new byte[] { 0, 0, 0, 1 }, 
        Internal.fixFloatingPointValue((byte) 0x03, 
            new byte[] { 0, 0, 0, 1 }));
  }
  
  @Test (expected = IllegalDataException.class)
  public void fixFloatingPointValueCorrupt() {
    Internal.fixFloatingPointValue((byte) 0x0B, 
            new byte[] { 0, 2, 0, 0, 0, 0, 0, 1 });
  }
  
  @Test
  public void inMilliseconds() {
    assertTrue(Internal.inMilliseconds((byte)0xFF));
  }
  
  @Test
  public void inMillisecondsNot() {
    assertFalse(Internal.inMilliseconds((byte)0xEF));
  }
  
  @Test
  public void getValueLengthFromQualifierInt8() {
    assertEquals(8, Internal.getValueLengthFromQualifier(new byte[] { 0, 7 }));
  }
  
  @Test
  public void getValueLengthFromQualifierInt8also() {
    assertEquals(8, Internal.getValueLengthFromQualifier(new byte[] { 0, 0x0F }));
  }
  
  @Test
  public void getValueLengthFromQualifierInt1() {
    assertEquals(1, Internal.getValueLengthFromQualifier(new byte[] { 0, 0 }));
  }
  
  @Test
  public void getValueLengthFromQualifierInt4() {
    assertEquals(4, Internal.getValueLengthFromQualifier(new byte[] { 0, 0x4B }));
  }
  
  @Test
  public void getValueLengthFromQualifierFloat4() {
    assertEquals(4, Internal.getValueLengthFromQualifier(new byte[] { 0, 11 }));
  }
  
  @Test
  public void getValueLengthFromQualifierFloat4also() {
    assertEquals(4, Internal.getValueLengthFromQualifier(new byte[] { 0, 0x1B }));
  }
  
  @Test
  public void getValueLengthFromQualifierFloat8() {
    assertEquals(8, Internal.getValueLengthFromQualifier(new byte[] { 0, 0x1F }));
  }
  
  // since all the qualifier methods share the validateQualifier() method, we
  // can test them once
  @Test (expected = NullPointerException.class)
  public void getValueLengthFromQualifierNull() {
    Internal.getValueLengthFromQualifier(null);
  }
  
  @Test (expected = IllegalDataException.class)
  public void getValueLengthFromQualifierEmpty() {
    Internal.getValueLengthFromQualifier(new byte[0]);
  }
  
  @Test (expected = IllegalDataException.class)
  public void getValueLengthFromQualifierNegativeOffset() {
    Internal.getValueLengthFromQualifier(new byte[] { 0, 0x4B }, -42);
  }
  
  @Test (expected = IllegalDataException.class)
  public void getValueLengthFromQualifierBadOffset() {
    Internal.getValueLengthFromQualifier(new byte[] { 0, 0x4B }, 42);
  }

  @Test
  public void getQualifierLengthSeconds() {
    assertEquals(2, Internal.getQualifierLength(new byte[] { 0, 0x0F }));
  }
  
  @Test
  public void getQualifierLengthMilliSeconds() {
    assertEquals(4, Internal.getQualifierLength(
        new byte[] { (byte) 0xF0, 0x00, 0x00, 0x07 }));
  }

  @Test (expected = IllegalDataException.class)
  public void getQualifierLengthSecondsTooShort() {
    Internal.getQualifierLength(new byte[] { 0x0F });
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getQualifierLengthMilliSecondsTooShort() {
    Internal.getQualifierLength(new byte[] { (byte) 0xF0, 0x00, 0x00,  });
  }

  @Test
  public void getTimestampFromQualifier() {
    final long ts = Internal.getTimestampFromQualifier(
        new byte[] { 0x00, 0x37 }, 1356998400);
    assertEquals(1356998403000L, ts);
  }
  
  @Test
  public void getTimestampFromQualifierMs() {
    final long ts = Internal.getTimestampFromQualifier(
        new byte[] { (byte) 0xF0, 0x00, 0x02, 0x07 }, 1356998400);
    assertEquals(1356998400008L, ts);
  }
  
  @Test
  public void getTimestampFromQualifierMsLarge() {
    long ts = 1356998400500L;
    // mimicks having 64K data points in a row
    final int limit = 64000;
    final byte[] qualifier = new byte[4 * limit];
    for (int i = 0; i < limit; i++) {
      System.arraycopy(Internal.buildQualifier(ts, (short) 7), 0, 
          qualifier, i * 4, 4);
      ts += 50;
    }
    assertEquals(1356998400550L, 
        Internal.getTimestampFromQualifier(qualifier, 1356998400, 4));
    assertEquals(1357001600450L, 
        Internal.getTimestampFromQualifier(qualifier, 1356998400, (limit - 1) * 4));
  }

  @Test
  public void getOffsetFromQualifier() {
    assertEquals(3000, Internal.getOffsetFromQualifier(
        new byte[] { 0x00, 0x37 }));
  }
  
  @Test
  public void getOffsetFromQualifierMs1ms() {
    assertEquals(1, Internal.getOffsetFromQualifier(
        new byte[] { (byte) 0xF0, 0x00, 0x00, 0x47 }));
  }

  @Test
  public void getOffsetFromQualifierMs() {
    assertEquals(8, Internal.getOffsetFromQualifier(
        new byte[] { (byte) 0xF0, 0x00, 0x02, 0x07 }));
  }
  
  @Test
  public void getOffsetFromQualifierMs2() {
    assertEquals(12, Internal.getOffsetFromQualifier(
        new byte[] { (byte) 0xF0, 0x00, 0x02, 0x07, 
            (byte) 0xF0, 0x00, 0x03, 0x07 }, 4));
  }
  
  @Test
  public void getOffsetFromQualifierMsLarge() {
    long ts = 1356998400500L;
    // mimicks having 64K data points in a row
    final int limit = 64000;
    final byte[] qualifier = new byte[4 * limit];
    for (int i = 0; i < limit; i++) {
      System.arraycopy(Internal.buildQualifier(ts, (short) 7), 0, 
          qualifier, i * 4, 4);
      ts += 50;
    }
    assertEquals(500, Internal.getOffsetFromQualifier(qualifier, 0));
    assertEquals(3200450, 
        Internal.getOffsetFromQualifier(qualifier, (limit - 1) * 4));
  }
  
  @Test
  public void getOffsetFromQualifierOffset() {
    final byte[] qual = { 0x00, 0x37, 0x00, 0x47 };
    assertEquals(4000, Internal.getOffsetFromQualifier(qual, 2));
  }
  
  @Test (expected = IllegalDataException.class)
  public void getOffsetFromQualifierBadOffset() {
    final byte[] qual = { 0x00, 0x37, 0x00, 0x47 };
    assertEquals(4000, Internal.getOffsetFromQualifier(qual, 3));
  }
  
  @Test
  public void getOffsetFromQualifierOffsetMixed() {
    final byte[] qual = { 0x00, 0x37, (byte) 0xF0, 0x00, 0x02, 0x07, 0x00, 
        0x47 };
    assertEquals(8, Internal.getOffsetFromQualifier(qual, 2));
  }

  @Test
  public void getFlagsFromQualifierInt() {
    assertEquals(7, Internal.getFlagsFromQualifier(new byte[] { 0x00, 0x37 }));
  }
  
  @Test
  public void getFlagsFromQualifierFloat() {
    assertEquals(11, Internal.getFlagsFromQualifier(new byte[] { 0x00, 0x1B }));
  }
    
  @Test
  public void buildQualifierSecond8ByteLong() {
    final byte[] q = Internal.buildQualifier(1356998403, (short) 7);
    assertArrayEquals(new byte[] { 0x00, 0x37 }, q);
  }
  
  @Test
  public void buildQualifierSecond8ByteLongEOH() {
    final byte[] q = Internal.buildQualifier(1357001999, (short) 7);
    assertArrayEquals(new byte[] { (byte) 0xE0, (byte) 0xF7 }, q);
  }

  @Test
  public void buildQualifierSecond6ByteLong() {
    final byte[] q = Internal.buildQualifier(1356998403, (short) 5);
    assertArrayEquals(new byte[] { 0x00, 0x35 }, q);
  }
  
  @Test
  public void buildQualifierSecond6ByteLongEOH() {
    final byte[] q = Internal.buildQualifier(1357001999, (short) 5);
    assertArrayEquals(new byte[] { (byte) 0xE0, (byte) 0xF5 }, q);
  }
  
  @Test
  public void buildQualifierSecond4ByteLong() {
    final byte[] q = Internal.buildQualifier(1356998403, (short) 3);
    assertArrayEquals(new byte[] { 0x00, 0x33 }, q);
  }
  
  @Test
  public void buildQualifierSecond4ByteLongEOH() {
    final byte[] q = Internal.buildQualifier(1357001999, (short) 3);
    assertArrayEquals(new byte[] { (byte) 0xE0, (byte) 0xF3 }, q);
  }
  
  @Test
  public void buildQualifierSecond2ByteLong() {
    final byte[] q = Internal.buildQualifier(1356998403, (short) 1);
    assertArrayEquals(new byte[] { 0x00, 0x31 }, q);
  }
  
  @Test
  public void buildQualifierSecond2ByteLongEOH() {
    final byte[] q = Internal.buildQualifier(1357001999, (short) 1);
    assertArrayEquals(new byte[] { (byte) 0xE0, (byte) 0xF1 }, q);
  }
  
  @Test
  public void buildQualifierSecond1ByteLong() {
    final byte[] q = Internal.buildQualifier(1356998403, (short) 0);
    assertArrayEquals(new byte[] { 0x00, 0x30 }, q);
  }
  
  @Test
  public void buildQualifierSecond1ByteLongEOH() {
    final byte[] q = Internal.buildQualifier(1357001999, (short) 0);
    assertArrayEquals(new byte[] { (byte) 0xE0, (byte) 0xF0 }, q);
  }
  
  @Test
  public void buildQualifierSecond8ByteFloat() {
    final byte[] q = Internal.buildQualifier(1356998403, 
        (short) ( 7 | Const.FLAG_FLOAT));
    assertArrayEquals(new byte[] { 0x00, 0x3F }, q);
  }
  
  @Test
  public void buildQualifierSecond8ByteFloatEOH() {
    final byte[] q = Internal.buildQualifier(1357001999, 
        (short) ( 7 | Const.FLAG_FLOAT));
    assertArrayEquals(new byte[] { (byte) 0xE0, (byte) 0xFF }, q);
  }
  
  @Test
  public void buildQualifierSecond4ByteFloat() {
    final byte[] q = Internal.buildQualifier(1356998403, 
        (short) ( 3 | Const.FLAG_FLOAT));
    assertArrayEquals(new byte[] { 0x00, 0x3B }, q);
  }
  
  @Test
  public void buildQualifierSecond4ByteFloatEOH() {
    final byte[] q = Internal.buildQualifier(1357001999, 
        (short) ( 3 | Const.FLAG_FLOAT));
    assertArrayEquals(new byte[] { (byte) 0xE0, (byte) 0xFB }, q);
  }
  
  @Test
  public void buildQualifierMilliSecond8ByteLong() {
    final byte[] q = Internal.buildQualifier(1356998400008L, (short) 7);
    assertArrayEquals(new byte[] {(byte) 0xF0, 0x00, 0x02, 0x07 }, q);
  }
  
  @Test
  public void buildQualifierMilliSecond8ByteLongEOH() {
    final byte[] q = Internal.buildQualifier(1357001999999L, (short) 7);
    assertArrayEquals(new byte[] {
        (byte) 0xFD, (byte) 0xBB, (byte) 0x9F, (byte) 0xC7 }, q);
  }
  
  @Test
  public void buildQualifierMilliSecond6ByteLong() {
    final byte[] q = Internal.buildQualifier(1356998400008L, (short) 5);
    assertArrayEquals(new byte[] {(byte) 0xF0, 0x00, 0x02, 0x05 }, q);
  }
  
  @Test
  public void buildQualifierMilliSecond6ByteLongEOH() {
    final byte[] q = Internal.buildQualifier(1357001999999L, (short) 5);
    assertArrayEquals(new byte[] {
        (byte) 0xFD, (byte) 0xBB, (byte) 0x9F, (byte) 0xC5 }, q);
  }
  
  @Test
  public void buildQualifierMilliSecond4ByteLong() {
    final byte[] q = Internal.buildQualifier(1356998400008L, (short) 3);
    assertArrayEquals(new byte[] {(byte) 0xF0, 0x00, 0x02, 0x03 }, q);
  }
  
  @Test
  public void buildQualifierMilliSecond4ByteLongEOH() {
    final byte[] q = Internal.buildQualifier(1357001999999L, (short) 3);
    assertArrayEquals(new byte[] {
        (byte) 0xFD, (byte) 0xBB, (byte) 0x9F, (byte) 0xC3 }, q);
  }
  
  @Test
  public void buildQualifierMilliSecond2ByteLong() {
    final byte[] q = Internal.buildQualifier(1356998400008L, (short) 1);
    assertArrayEquals(new byte[] {(byte) 0xF0, 0x00, 0x02, 0x01 }, q);
  }
  
  @Test
  public void buildQualifierMilliSecond2ByteLongEOH() {
    final byte[] q = Internal.buildQualifier(1357001999999L, (short) 1);
    assertArrayEquals(new byte[] {
        (byte) 0xFD, (byte) 0xBB, (byte) 0x9F, (byte) 0xC1 }, q);
  }
  
  @Test
  public void buildQualifierMilliSecond1ByteLong() {
    final byte[] q = Internal.buildQualifier(1356998400008L, (short) 0);
    assertArrayEquals(new byte[] {(byte) 0xF0, 0x00, 0x02, 0x00 }, q);
  }
  
  @Test
  public void buildQualifierMilliSecond0ByteLongEOH() {
    final byte[] q = Internal.buildQualifier(1357001999999L, (short) 0);
    assertArrayEquals(new byte[] {
        (byte) 0xFD, (byte) 0xBB, (byte) 0x9F, (byte) 0xC0 }, q);
  }
  
  @Test
  public void buildQualifierMilliSecond8ByteFloat() {
    final byte[] q = Internal.buildQualifier(1356998400008L, 
        (short) ( 7 | Const.FLAG_FLOAT));
    assertArrayEquals(new byte[] {(byte) 0xF0, 0x00, 0x02, 0x0F }, q);
  }
  
  @Test
  public void buildQualifierMilliSecond8ByteFloatEOH() {
    final byte[] q = Internal.buildQualifier(1357001999999L, 
        (short) ( 7 | Const.FLAG_FLOAT));
    assertArrayEquals(new byte[] {
        (byte) 0xFD, (byte) 0xBB, (byte) 0x9F, (byte) 0xCF }, q);
  }
  
  @Test
  public void buildQualifierMilliSecond4ByteFloat() {
    final byte[] q = Internal.buildQualifier(1356998400008L, 
        (short) ( 3 | Const.FLAG_FLOAT));
    assertArrayEquals(new byte[] {(byte) 0xF0, 0x00, 0x02, 0x0B }, q);
  }

  @Test
  public void buildQualifierMilliSecond4ByteFloatEOH() {
    final byte[] q = Internal.buildQualifier(1357001999999L, 
        (short) ( 3 | Const.FLAG_FLOAT));
    assertArrayEquals(new byte[] {
        (byte) 0xFD, (byte) 0xBB, (byte) 0x9F, (byte) 0xCB }, q);
  }
  
  @Test
  public void extractQualifierSeconds() {
    final byte[] qual = { 0x00, 0x37, (byte) 0xF0, 0x00, 0x02, 0x07, 0x00, 
        0x47 };
    assertArrayEquals(new byte[] { 0, 0x47 }, 
        Internal.extractQualifier(qual, 6));
  }
  
  @Test
  public void extractQualifierMilliSeconds() {
    final byte[] qual = { 0x00, 0x37, (byte) 0xF0, 0x00, 0x02, 0x07, 0x00, 
        0x47 };
    assertArrayEquals(new byte[] { (byte) 0xF0, 0x00, 0x02, 0x07 }, 
        Internal.extractQualifier(qual, 2));
  }
 
  @Test
  public void getMaxUnsignedValueOnBytes() throws Exception {
    assertEquals(0, Internal.getMaxUnsignedValueOnBytes(0));
    assertEquals(255, Internal.getMaxUnsignedValueOnBytes(1));
    assertEquals(65535, Internal.getMaxUnsignedValueOnBytes(2));
    assertEquals(16777215, Internal.getMaxUnsignedValueOnBytes(3));
    assertEquals(4294967295L, Internal.getMaxUnsignedValueOnBytes(4));
    assertEquals(1099511627775L, Internal.getMaxUnsignedValueOnBytes(5));
    assertEquals(281474976710655L, Internal.getMaxUnsignedValueOnBytes(6));
    assertEquals(72057594037927935L, Internal.getMaxUnsignedValueOnBytes(7));
    assertEquals(Long.MAX_VALUE, Internal.getMaxUnsignedValueOnBytes(8));
    
    try {
      Internal.getMaxUnsignedValueOnBytes(9);
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertNotNull(e);
    }
    
    try {
      Internal.getMaxUnsignedValueOnBytes(-1);
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertNotNull(e);
    }
  }
  
  /** Shorthand to create a {@link KeyValue}.  */
  private static KeyValue makekv(final byte[] qualifier, final byte[] value) {
    return new KeyValue(KEY, FAMILY, qualifier, value);
  }
}
