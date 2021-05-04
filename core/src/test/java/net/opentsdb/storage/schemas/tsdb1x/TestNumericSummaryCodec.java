// This file is part of OpenTSDB.
// Copyright (C) 2018 The OpenTSDB Authors.
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.rollup.DefaultRollupInterval;
import net.opentsdb.storage.WriteStatus;
import org.junit.Test;

import net.opentsdb.data.types.numeric.NumericSummaryType;

public class TestNumericSummaryCodec {

  @Test
  public void base() throws Exception {
    NumericSummaryCodec codec = new NumericSummaryCodec();
    assertEquals(NumericSummaryType.TYPE, codec.type());
    Span<?> span = codec.newSequences(false);
    assertTrue(span instanceof NumericSummarySpan);
    try {
      codec.newRowSeq(1L);
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }

  @Test
  public void encode() throws Exception {
    DefaultRollupInterval interval = DefaultRollupInterval.builder()
            .setInterval("1h")
            .setRowSpan("1d")
            .setTable("table")
            .setPreAggregationTable("table2")
            .build();
    Codec codec = new NumericSummaryCodec();
    MutableNumericSummaryValue value = new MutableNumericSummaryValue();
    value.resetTimestamp(new SecondTimeStamp(1262304000));
    value.resetValue(0, 42);
    value.resetValue(1, 60);
    assertEquals(WriteStatus.OK,
            codec.encode(value, false, 1262304000, interval));
    assertArrayEquals(new byte[] { 0, 0, 0 }, codec.qualifiers()[0]);
    assertArrayEquals(new byte[] { 42 }, codec.values()[0]);
    assertEquals(3, codec.qualifierLengths()[0]);
    assertEquals(1, codec.valueLengths()[0]);
    assertArrayEquals(new byte[] { 1, 0, 0 }, codec.qualifiers()[1]);
    assertArrayEquals(new byte[] { 60 }, codec.values()[1]);
    assertEquals(3, codec.qualifierLengths()[1]);
    assertEquals(1, codec.valueLengths()[1]);
    assertEquals(2, codec.encodedValues());

    // offset and VLE lengths
    value.resetTimestamp(new SecondTimeStamp(1262318400));
    value.resetValue(0, 256);
    value.resetValue(1, 59);
    assertEquals(WriteStatus.OK,
            codec.encode(value, false, 1262304000, interval));
    assertArrayEquals(new byte[] { 0, 0, 65 }, codec.qualifiers()[0]);
    assertArrayEquals(new byte[] { 1, 0 }, codec.values()[0]);
    assertEquals(3, codec.qualifierLengths()[0]);
    assertEquals(2, codec.valueLengths()[0]);
    assertArrayEquals(new byte[] { 1, 0, 64 }, codec.qualifiers()[1]);
    assertArrayEquals(new byte[] { 59 }, codec.values()[1]);
    assertEquals(3, codec.qualifierLengths()[1]);
    assertEquals(1, codec.valueLengths()[1]);
    assertEquals(2, codec.encodedValues());

    value.resetTimestamp(new SecondTimeStamp(1262347200));
    value.resetValue(0, 65536);
    value.resetValue(1, 256);
    assertEquals(WriteStatus.OK,
            codec.encode(value, false, 1262304000, interval));
    assertArrayEquals(new byte[] { 0, 0, -61 }, codec.qualifiers()[0]);
    assertArrayEquals(new byte[] { 0, 1, 0, 0 }, codec.values()[0]);
    assertEquals(3, codec.qualifierLengths()[0]);
    assertEquals(4, codec.valueLengths()[0]);
    assertArrayEquals(new byte[] { 1, 0, -63 }, codec.qualifiers()[1]);
    assertArrayEquals(new byte[] { 1, 0 }, codec.values()[1]);
    assertEquals(3, codec.qualifierLengths()[1]);
    assertEquals(2, codec.valueLengths()[1]);
    assertEquals(2, codec.encodedValues());

    value.resetValue(0, 4294967295L);
    value.resetValue(1, 1);
    assertEquals(WriteStatus.OK,
            codec.encode(value, false, 1262304000, interval));
    assertArrayEquals(new byte[] { 0, 0, -57 }, codec.qualifiers()[0]);
    assertArrayEquals(new byte[] { 0, 0, 0, 0, -1, -1, -1, -1 }, codec.values()[0]);
    assertEquals(3, codec.qualifierLengths()[0]);
    assertEquals(8, codec.valueLengths()[0]);
    assertArrayEquals(new byte[] { 1, 0, -64 }, codec.qualifiers()[1]);
    assertArrayEquals(new byte[] { 1 }, codec.values()[1]);
    assertEquals(3, codec.qualifierLengths()[1]);
    assertEquals(1, codec.valueLengths()[1]);
    assertEquals(2, codec.encodedValues());

    // Float / Double
    value.resetValue(0, 42.5D);
    value.resetValue(1, 60);
    assertEquals(WriteStatus.OK,
            codec.encode(value, false, 1262304000, interval));
    assertArrayEquals(new byte[] { 0, 0, -53 }, codec.qualifiers()[0]);
    assertArrayEquals(new byte[] { 66, 42, 0, 0 }, codec.values()[0]);
    assertEquals(3, codec.qualifierLengths()[0]);
    assertEquals(4, codec.valueLengths()[0]);
    assertArrayEquals(new byte[] { 1, 0, -64 }, codec.qualifiers()[1]);
    assertArrayEquals(new byte[] { 60 }, codec.values()[1]);
    assertEquals(3, codec.qualifierLengths()[1]);
    assertEquals(1, codec.valueLengths()[1]);
    assertEquals(2, codec.encodedValues());

    value.resetValue(0, 1.234556789123456);
    value.resetValue(1, 60);
    assertEquals(WriteStatus.OK,
            codec.encode(value, false, 1262304000, interval));
    assertArrayEquals(new byte[] { 0, 0, -49 }, codec.qualifiers()[0]);
    assertArrayEquals(new byte[] { 63, -13, -64, -66, -98, -91, 112, -80 }, codec.values()[0]);
    assertEquals(3, codec.qualifierLengths()[0]);
    assertEquals(8, codec.valueLengths()[0]);
    assertArrayEquals(new byte[] { 1, 0, -64 }, codec.qualifiers()[1]);
    assertArrayEquals(new byte[] { 60 }, codec.values()[1]);
    assertEquals(3, codec.qualifierLengths()[1]);
    assertEquals(1, codec.valueLengths()[1]);
    assertEquals(2, codec.encodedValues());

    // appends
    value.resetTimestamp(new SecondTimeStamp(1262304000));
    value.resetValue(0, 42);
    value.resetValue(1, 60);
    assertEquals(WriteStatus.OK,
            codec.encode(value, true, 1262304000, interval));
    assertEquals(0, codec.qualifiers()[0][0]);
    assertArrayEquals(new byte[] { 0, 0, 42 }, codec.values()[0]);
    assertEquals(1, codec.qualifierLengths()[0]);
    assertEquals(3, codec.valueLengths()[0]);
    assertEquals(1, codec.qualifiers()[1][0]);
    assertArrayEquals(new byte[] { 0, 0, 60 }, codec.values()[1]);
    assertEquals(1, codec.qualifierLengths()[1]);
    assertEquals(3, codec.valueLengths()[1]);
    assertEquals(2, codec.encodedValues());

    value.resetValue(0, 42.5D);
    value.resetValue(1, 60);
    assertEquals(WriteStatus.OK,
            codec.encode(value, true, 1262304000, interval));
    assertEquals(0, codec.qualifiers()[0][0]);
    assertArrayEquals(new byte[] { 0, 11, 66, 42, 0, 0 }, codec.values()[0]);
    assertEquals(1, codec.qualifierLengths()[0]);
    assertEquals(6, codec.valueLengths()[0]);
    assertEquals(1, codec.qualifiers()[1][0]);
    assertArrayEquals(new byte[] { 0, 0, 60 }, codec.values()[1]);
    assertEquals(1, codec.qualifierLengths()[1]);
    assertEquals(3, codec.valueLengths()[1]);
    assertEquals(2, codec.encodedValues());

    // grow
    value.resetValue(2, 6);
    value.resetValue(3, 0);
    value.resetValue(4, 5);
    assertEquals(WriteStatus.OK,
            codec.encode(value, true, 1262304000, interval));
    assertEquals(0, codec.qualifiers()[0][0]);
    assertArrayEquals(new byte[] { 0, 11, 66, 42, 0, 0 }, codec.values()[0]);
    assertEquals(1, codec.qualifierLengths()[0]);
    assertEquals(6, codec.valueLengths()[0]);

    assertEquals(1, codec.qualifiers()[1][0]);
    assertArrayEquals(new byte[] { 0, 0, 60 }, codec.values()[1]);
    assertEquals(1, codec.qualifierLengths()[1]);
    assertEquals(3, codec.valueLengths()[1]);

    assertEquals(2, codec.qualifiers()[2][0]);
    assertArrayEquals(new byte[] { 0, 0, 6 }, codec.values()[2]);
    assertEquals(1, codec.qualifierLengths()[2]);
    assertEquals(3, codec.valueLengths()[2]);

    assertEquals(3, codec.qualifiers()[3][0]);
    assertArrayEquals(new byte[] { 0, 0, 0 }, codec.values()[3]);
    assertEquals(1, codec.qualifierLengths()[3]);
    assertEquals(3, codec.valueLengths()[3]);

    assertEquals(4, codec.qualifiers()[4][0]);
    assertArrayEquals(new byte[] { 0, 0, 5 }, codec.values()[4]);
    assertEquals(1, codec.qualifierLengths()[4]);
    assertEquals(3, codec.valueLengths()[4]);

    assertEquals(5, codec.encodedValues());
  }
}
