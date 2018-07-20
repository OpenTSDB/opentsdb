// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;

import org.junit.Test;

import net.opentsdb.data.pbuf.NumericDatumPB.NumericDatum;

public class TestPBufNumericDatum {

  @Test
  public void toAndFro() throws Exception {
    // small int
    NumericDatum datum = NumericDatum.newBuilder()
      .setIsInteger(true)
      .setValue(42)
      .build();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    datum.writeTo(baos);
    
    PBufNumericDatum pbn = new PBufNumericDatum(
        NumericDatum.parseFrom(baos.toByteArray()));
    assertTrue(pbn.isInteger());
    assertEquals(42, pbn.longValue());
    try {
      pbn.doubleValue();
      fail("Expected ClassCastException");
    } catch (ClassCastException e) { }
    
    // smallest int
    datum = NumericDatum.newBuilder()
        .setIsInteger(true)
        .setValue(Long.MIN_VALUE)
        .build();
    baos = new ByteArrayOutputStream();
    datum.writeTo(baos);
    pbn = new PBufNumericDatum(
        NumericDatum.parseFrom(baos.toByteArray()));
    assertTrue(pbn.isInteger());
    assertEquals(Long.MIN_VALUE, pbn.longValue());
    
    // biggest int
    datum = NumericDatum.newBuilder()
        .setIsInteger(true)
        .setValue(Long.MAX_VALUE)
        .build();
    baos = new ByteArrayOutputStream();
    datum.writeTo(baos);
    pbn = new PBufNumericDatum(
        NumericDatum.parseFrom(baos.toByteArray()));
    assertTrue(pbn.isInteger());
    assertEquals(Long.MAX_VALUE, pbn.longValue());
    
    // double
    datum = NumericDatum.newBuilder()
        .setIsInteger(false)
        .setValue(Double.doubleToRawLongBits(42.75))
        .build();
    baos = new ByteArrayOutputStream();
    datum.writeTo(baos);
    pbn = new PBufNumericDatum(
        NumericDatum.parseFrom(baos.toByteArray()));
    assertFalse(pbn.isInteger());
    assertEquals(42.75, pbn.doubleValue(), 0.001);
    
    // smallest double
    datum = NumericDatum.newBuilder()
        .setIsInteger(false)
        .setValue(Double.doubleToRawLongBits(Double.MIN_VALUE))
        .build();
    baos = new ByteArrayOutputStream();
    datum.writeTo(baos);
    pbn = new PBufNumericDatum(
        NumericDatum.parseFrom(baos.toByteArray()));
    assertFalse(pbn.isInteger());
    assertEquals(Double.MIN_VALUE, pbn.doubleValue(), 0.001);
    
    // largest double
    datum = NumericDatum.newBuilder()
        .setIsInteger(false)
        .setValue(Double.doubleToRawLongBits(Double.MAX_VALUE))
        .build();
    baos = new ByteArrayOutputStream();
    datum.writeTo(baos);
    pbn = new PBufNumericDatum(
        NumericDatum.parseFrom(baos.toByteArray()));
    assertFalse(pbn.isInteger());
    assertEquals(Double.MAX_VALUE, pbn.doubleValue(), 0.001);
    
    // NaN
    datum = NumericDatum.newBuilder()
        .setIsInteger(false)
        .setValue(Double.doubleToRawLongBits(Double.NaN))
        .build();
    baos = new ByteArrayOutputStream();
    datum.writeTo(baos);
    pbn = new PBufNumericDatum(
        NumericDatum.parseFrom(baos.toByteArray()));
    assertFalse(pbn.isInteger());
    assertEquals(Double.NaN, pbn.doubleValue(), 0.001);
  }
}
