// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.query.pojo;

import org.junit.Test;

import net.opentsdb.core.Aggregator;
import net.opentsdb.data.types.numeric.Aggregators;
import net.opentsdb.data.types.numeric.NumericAggregator;
import net.opentsdb.utils.DateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.TimeZone;

public class TestDownsamplingSpecification {
  final long interval = 60000L;
  final FillPolicy fill_policy = FillPolicy.ZERO;
  final NumericAggregator function = Aggregators.SUM;
  final TimeZone timezone = DateTime.timezones.get(DateTime.UTC_ID);
  
  @Test
  public void testCtor() {
    final long interval = 1234567L;
    final NumericAggregator function = Aggregators.SUM;
    final FillPolicy fill_policy = FillPolicy.ZERO;

    final DownsamplingSpecification ds = new DownsamplingSpecification(
      interval, function, fill_policy);

    assertEquals(interval, ds.getInterval());
    assertEquals(function, ds.getFunction());
    assertEquals(fill_policy, ds.getFillPolicy());
  }

  @Test
  public void testStringCtor() {
    final DownsamplingSpecification ds = new DownsamplingSpecification(
      "15m-avg-nan");

    assertEquals(900000L, ds.getInterval());
    assertEquals(Aggregators.AVG, ds.getFunction());
    assertEquals(FillPolicy.NOT_A_NUMBER, ds.getFillPolicy());
  }

  @Test(expected = RuntimeException.class)
  public void testBadInterval() {
    new DownsamplingSpecification("blah-avg-lerp");
  }

  @Test(expected = RuntimeException.class)
  public void testBadFunction() {
    new DownsamplingSpecification("1m-hurp-lerp");
  }

  @Test(expected = RuntimeException.class)
  public void testBadFillPolicy() {
    new DownsamplingSpecification("10m-avg-max");
  }

  @Test (expected = IllegalArgumentException.class)
  public void testNoneAgg() {
    new DownsamplingSpecification("1m-none-lerp");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void testCtorNegativeInterval() {
    new DownsamplingSpecification(-1, function, fill_policy);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void testCtorZeroInterval() {
    new DownsamplingSpecification(DownsamplingSpecification.NO_INTERVAL, 
        function, fill_policy);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void testCtorNullFunction() {
    new DownsamplingSpecification(interval, null, fill_policy);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testStringCtorNull() {
    new DownsamplingSpecification(null);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testStringCtorEmpty() {
    new DownsamplingSpecification("");
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testStringCtorNoIntervalString() {
    new DownsamplingSpecification("blah-avg-lerp");
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testStringCtorZeroInterval() {
    new DownsamplingSpecification("0m-avg-lerp");
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testStringCtorNegativeInterval() {
    new DownsamplingSpecification("-60m-avg-lerp");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testStringCtorUnknownUnits() {
    new DownsamplingSpecification("1j-avg-lerp");
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testStringCtorMissingUnits() {
    new DownsamplingSpecification("1-avg-lerp");
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testStringCtorBadFunction() {
    new DownsamplingSpecification("1m-hurp-lerp");
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testStringCtorMissingFunction() {
    new DownsamplingSpecification("1m");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testStringCtorBadFillPolicy() {
    new DownsamplingSpecification("10m-avg-max");
  }
  
  @Test
  public void testSetCalendar() {
    DownsamplingSpecification ds = new DownsamplingSpecification("15m-avg");
    assertFalse(ds.useCalendar());
    
    ds.setUseCalendar(true);
    assertTrue(ds.useCalendar());
    
    ds.setUseCalendar(false);
    assertFalse(ds.useCalendar());
  }
  
  @Test
  public void setTimezone() {
    DownsamplingSpecification ds = new DownsamplingSpecification("15m-avg");
    assertEquals(timezone, ds.getTimezone());
    
    final TimeZone tz = DateTime.timezones.get("America/Denver"); 
    ds.setTimezone(tz);
    assertEquals(tz, ds.getTimezone());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setTimezoneNull() {
    DownsamplingSpecification ds = new DownsamplingSpecification("15m-avg");
    ds.setTimezone(null);
  }
  
  @Test
  public void testToString() {
    final String string = new DownsamplingSpecification(
        4532019L,
        Aggregators.ZIMSUM,
        FillPolicy.NOT_A_NUMBER).toString();
    
    assertTrue(string.contains("interval=4532019"));
    assertTrue(string.contains("function=zimsum"));
    assertTrue(string.contains("fillPolicy=NOT_A_NUMBER"));
    assertTrue(string.contains("useCalendar=false"));
    assertTrue(string.contains("timeZone=UTC"));
  }

}

