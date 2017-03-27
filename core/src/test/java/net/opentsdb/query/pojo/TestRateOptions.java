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
package net.opentsdb.query.pojo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import net.opentsdb.utils.JSON;

public class TestRateOptions {

  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenIntervalIsInvalid() throws Exception {
    RateOptions.newBuilder()
      .setInterval(0)
    .build()
    .validate();
  }
  
  @Test
  public void serdes() throws Exception {
    RateOptions options = RateOptions.newBuilder()
        .build();
    String json = JSON.serializeToString(options);
    // defaults are empty
    assertEquals("{}", json);
    
    options = RateOptions.newBuilder()
        .setCounter(true)
        .setInterval(30000)
        .setCounterMax(Integer.MAX_VALUE)
        .build();
    json = JSON.serializeToString(options);
    assertTrue(json.contains("\"counter\":true"));
    assertTrue(json.contains("\"interval\":30000"));
    assertTrue(json.contains("\"counterMax\":" + Integer.MAX_VALUE));
    
    json = "{\"counter\":true,\"interval\":15000,\"counterMax\":16,"
        + "\"resetValue\":-1,\"dropResets\":true,\"someOtherField\":\"Boo!\"}";
    options = JSON.parseToObject(json, RateOptions.class);
    assertTrue(options.isCounter());
    assertTrue(options.getDropResets());
    assertEquals(15000, options.getInterval());
    assertEquals(-1, options.getResetValue());
    assertEquals(16, options.getCounterMax());
  }
  
  @Test
  public void build() throws Exception {
    final RateOptions options = RateOptions.newBuilder()
        .setCounter(true)
        .setInterval(30000)
        .setCounterMax(Integer.MAX_VALUE)
        .build();
    final RateOptions clone = RateOptions.newBuilder(options).build();
    assertTrue(clone.isCounter());
    assertFalse(clone.getDropResets());
    assertEquals(30000, clone.getInterval());
    assertEquals(0, clone.getResetValue());
    assertEquals(Integer.MAX_VALUE, clone.getCounterMax());
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final RateOptions r1 = RateOptions.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        .setInterval(30000)
        .setCounterMax(Integer.MAX_VALUE)
        .setResetValue(-1)
        .build();
    
    RateOptions r2 = RateOptions.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        .setInterval(30000)
        .setCounterMax(Integer.MAX_VALUE)
        .setResetValue(-1)
        .build();
    assertEquals(r1.hashCode(), r2.hashCode());
    assertEquals(r1, r2);
    assertEquals(0, r1.compareTo(r2));
    
    r2 = RateOptions.newBuilder()
        //.setCounter(true) // <-- Diff
        .setDropResets(true)
        .setInterval(30000)
        .setCounterMax(Integer.MAX_VALUE)
        .setResetValue(-1)
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(-1, r1.compareTo(r2));
    
    r2 = RateOptions.newBuilder()
        .setCounter(true)
        //.setDropResets(true) // <-- Diff
        .setInterval(30000)
        .setCounterMax(Integer.MAX_VALUE)
        .setResetValue(-1)
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(-1, r1.compareTo(r2));
    
    r2 = RateOptions.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        .setInterval(15000) // <-- Diff
        .setCounterMax(Integer.MAX_VALUE)
        .setResetValue(-1)
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(1, r1.compareTo(r2));
    
    r2 = RateOptions.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        //.setInterval(30000) // <-- Diff
        .setCounterMax(Integer.MAX_VALUE)
        .setResetValue(-1)
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(-1, r1.compareTo(r2));
    
    r2 = RateOptions.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        .setInterval(30000)
        .setCounterMax(Short.MAX_VALUE) // <-- Diff
        .setResetValue(-1)
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(1, r1.compareTo(r2));
    
    r2 = RateOptions.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        .setInterval(30000)
        //.setCounterMax(Integer.MAX_VALUE) // <-- Diff
        .setResetValue(-1)
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(-1, r1.compareTo(r2));
    
    r2 = RateOptions.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        .setInterval(30000)
        .setCounterMax(Integer.MAX_VALUE)
        .setResetValue(100) // <-- Diff
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(-1, r1.compareTo(r2));
    
    r2 = RateOptions.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        .setInterval(30000)
        .setCounterMax(Integer.MAX_VALUE)
        //.setResetValue(-1) // <-- Diff
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(-1, r1.compareTo(r2));
  }
  
}
