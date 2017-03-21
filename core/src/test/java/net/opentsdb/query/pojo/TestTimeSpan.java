// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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

import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestTimeSpan {
  @Test(expected = IllegalArgumentException.class)
  public void startIsNull() {
    String json = "{\"start\":null,\"end\":\"2015/05/05\","
        + "\"timezone\":\"UTC\",\"downsample\":\"15m-avg-nan\","
        + ",\"aggregator\":\"sum\"}";
    Timespan timespan = JSON.parseToObject(json, Timespan.class);
    timespan.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void startIsEmpty() {
    String json = "{\"start\":\"\",\"end\":\"2015/05/05\","
        + "\"timezone\":\"UTC\",\"downsample\":\"15m-avg-nan\""
        + ",\"aggregator\":\"sum\"}";
    Timespan timespan = JSON.parseToObject(json, Timespan.class);
    timespan.validate();
  }

  @Test 
  public void endIsNull() {
    String json = "{\"start\":\"2015/05/05\",\"end\":null,"
        + "\"timezone\":\"UTC\",\"downsample\":\"15m-avg-nan\""
        + ",\"aggregator\":\"sum\"}";
    Timespan timespan = JSON.parseToObject(json, Timespan.class);
    timespan.validate();
  }

  @Test
  public void endIsEmpty() {
    String json = "{\"start\":\"1h-ago\",\"end\":\"\","
        + "\"timezone\":\"UTC\",\"downsample\":\"15m-avg-nan\""
        + ",\"aggregator\":\"sum\"}";
    Timespan timespan = JSON.parseToObject(json, Timespan.class);
    timespan.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void aggregatorIsNull() {
    String json = "{\"start\":\"1h-ago\",\"end\":\"2015/05/05\","
        + "\"timezone\":\"UTC\",\"downsample\":\"15m-avg-nan\","
        + "}";
    Timespan timespan = JSON.parseToObject(json, Timespan.class);
    timespan.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void aggregatorIsEmpty() {
    String json = "{\"start\":\"1h-ago\",\"end\":\"2015/05/05\","
        + "\"timezone\":\"UTC\",\"downsample\":\"15m-avg-nan\""
        + ",\"aggregator\":\"\"}";
    Timespan timespan = JSON.parseToObject(json, Timespan.class);
    timespan.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void idIsNull() {
    String json = "{\"start\":\"-1h\",\"end\":\"2015/05/05\","
        + "\"timezone\":\"UTC\",\"downsample\":\"15m-avg-nan\""
        + ",\"interpolation\":\"LERP\"}";
    Timespan timespan = JSON.parseToObject(json, Timespan.class);
    timespan.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void idIsEmpty() {
    String json = "{\"start\":\"-1h\",\"end\":\"2015/05/05\","
        + "\"timezone\":\"UTC\",\"downsample\":\"15m-avg-nan\""
        + ",\"interpolation\":\"LERP\"}";
    Timespan timespan = JSON.parseToObject(json, Timespan.class);
    timespan.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidDownsample() {
    String json = "{\"start\":\"1h-ago\",\"end\":\"2015/05/05\",\"timezone\":\"UTC\","
        + "\"downsampler\":\"xxx\"}";
    Timespan timespan = JSON.parseToObject(json, Timespan.class);
    timespan.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidSliceConfig() {
    String json = "{\"start\":\"1h-ago\",\"end\":\"2015/05/05\",\"timezone\":\"UTC\","
        + "\"downsampler\":{\"interval\":\"15m\",\"aggregator\":\"avg\","
        + "\"fillPolicy\":{\"policy\":\"nan\"}},\"aggregator\":\"sum\","
        + "\"sliceConfig\":\"1\"}";
    Timespan timespan = JSON.parseToObject(json, Timespan.class);
    timespan.validate();
  }
  
  @Test
  public void startTime() {
    Timespan timespan = Timespan.newBuilder()
        .setStart("1h-ago")
        .build();
    assertTrue(timespan.startTime().msEpoch() > 0 && 
        timespan.startTime().msEpoch() < DateTime.currentTimeMillis());
    
    timespan = Timespan.newBuilder()
        .setStart("2017/02/02 06:30:01")
        .setTimezone("UTC")
        .build();
    assertEquals(1486017001000L, timespan.startTime().msEpoch());
    
    timespan = Timespan.newBuilder()
        .setStart("not a real time")
        .setTimezone("UTC")
        .build();
    try {
      timespan.startTime();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    timespan = Timespan.newBuilder()
        .setStart("2017/02/02 06:30:01")
        .setTimezone("unknown tz")
        .build();
    try {
      timespan.startTime();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void endTime() {
    Timespan timespan = Timespan.newBuilder()
        .setEnd("1h-ago")
        .build();
    assertTrue(timespan.endTime().msEpoch() > 0 && 
        timespan.endTime().msEpoch() < DateTime.currentTimeMillis());
    
    timespan = Timespan.newBuilder()
        .setEnd("2017/02/02 06:30:01")
        .setTimezone("UTC")
        .build();
    assertEquals(1486017001000L, timespan.endTime().msEpoch());
    
    timespan = Timespan.newBuilder()
        .setEnd("not a real time")
        .setTimezone("UTC")
        .build();
    try {
      timespan.endTime();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    timespan = Timespan.newBuilder()
        .setEnd("2017/02/02 06:30:01")
        .setTimezone("unknown tz")
        .build();
    try {
      timespan.endTime();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void deserialize() {
    String json = "{\"start\":\"1h-ago\",\"end\":\"2015/05/05\",\"timezone\":\"UTC\","
        + "\"downsampler\":{\"interval\":\"15m\",\"aggregator\":\"avg\","
        + "\"fillPolicy\":{\"policy\":\"nan\"}},\"aggregator\":\"sum\","
        + "\"unknownfield\":\"boo\"}";
    Timespan timespan = JSON.parseToObject(json, Timespan.class);
    Timespan expected = Timespan.newBuilder().setStart("1h-ago")
        .setEnd("2015/05/05").setTimezone("UTC").setAggregator("sum")
        .setDownsampler(
            Downsampler.newBuilder().setInterval("15m").setAggregator("avg")
            .setFillPolicy(new NumericFillPolicy(FillPolicy.NOT_A_NUMBER)).build())
        .build();
    timespan.validate();
    assertEquals(expected, timespan);
  }

  @Test
  public void serialize() {
    Timespan timespan = Timespan.newBuilder().setStart("1h-ago")
        .setEnd("2015/05/05").setTimezone("UTC").setAggregator("sum").setDownsampler(
            Downsampler.newBuilder().setInterval("15m").setAggregator("avg")
            .setFillPolicy(new NumericFillPolicy(FillPolicy.NOT_A_NUMBER)).build())
        .build();
    String actual = JSON.serializeToString(timespan);
    assertTrue(actual.contains("\"start\":\"1h-ago\""));
    assertTrue(actual.contains("\"end\":\"2015/05/05\""));
    assertTrue(actual.contains("\"aggregator\":\"sum\""));
    assertTrue(actual.contains("\"timezone\":\"UTC\""));
    assertTrue(actual.contains("\"downsampler\":{"));
    assertTrue(actual.contains("\"interval\":\"15m\""));
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final Timespan t1 = new Timespan.Builder()
        .setStart("1h-ago")
        .setEnd("1m-ago")
        .setAggregator("sum")
        .setTimezone("UTC")
        .setDownsampler(new Downsampler.Builder()
            .setAggregator("sum")
            .setInterval("1h")
            .build())
        .setRate(false)
        .setSliceConfig("50%")
        .build();
    
    Timespan t2 = new Timespan.Builder()
        .setStart("1h-ago")
        .setEnd("1m-ago")
        .setAggregator("sum")
        .setTimezone("UTC")
        .setDownsampler(new Downsampler.Builder()
            .setAggregator("sum")
            .setInterval("1h")
            .build())
        .setRate(false)
        .setSliceConfig("50%")
        .build();
    assertEquals(t1.hashCode(), t2.hashCode());
    assertEquals(t1, t2);
    assertEquals(0, t1.compareTo(t2));
    
    t2 = new Timespan.Builder()
        .setStart("1d-ago") // <-- diff
        .setEnd("1m-ago")
        .setAggregator("sum")
        .setTimezone("UTC")
        .setDownsampler(new Downsampler.Builder()
            .setAggregator("sum")
            .setInterval("1h")
            .build())
        .setRate(false)
        .setSliceConfig("50%")
        .build();
    assertNotEquals(t1.hashCode(), t2.hashCode());
    assertNotEquals(t1, t2);
    assertEquals(1, t1.compareTo(t2));
    
    t2 = new Timespan.Builder()
        .setStart("1h-ago")
        .setEnd("10m-ago")  // <-- diff
        .setAggregator("sum")
        .setTimezone("UTC")
        .setDownsampler(new Downsampler.Builder()
            .setAggregator("sum")
            .setInterval("1h")
            .build())
        .setRate(false)
        .setSliceConfig("50%")
        .build();
    assertNotEquals(t1.hashCode(), t2.hashCode());
    assertNotEquals(t1, t2);
    assertEquals(1, t1.compareTo(t2));
    
    t2 = new Timespan.Builder()
        .setStart("1h-ago")
        //.setEnd("1m-ago")  // <-- diff
        .setAggregator("sum")
        .setTimezone("UTC")
        .setDownsampler(new Downsampler.Builder()
            .setAggregator("sum")
            .setInterval("1h")
            .build())
        .setRate(false)
        .setSliceConfig("50%")
        .build();
    assertNotEquals(t1.hashCode(), t2.hashCode());
    assertNotEquals(t1, t2);
    assertEquals(1, t1.compareTo(t2));
    
    t2 = new Timespan.Builder()
        .setStart("1h-ago")
        .setEnd("1m-ago")
        .setAggregator("max")  // <-- diff
        .setTimezone("UTC")
        .setDownsampler(new Downsampler.Builder()
            .setAggregator("sum")
            .setInterval("1h")
            .build())
        .setRate(false)
        .setSliceConfig("50%")
        .build();
    assertNotEquals(t1.hashCode(), t2.hashCode());
    assertNotEquals(t1, t2);
    assertEquals(1, t1.compareTo(t2));
    
    t2 = new Timespan.Builder()
        .setStart("1h-ago")
        .setEnd("1m-ago")
        .setAggregator("sum")
        .setTimezone("PST")  // <-- diff
        .setDownsampler(new Downsampler.Builder()
            .setAggregator("sum")
            .setInterval("1h")
            .build())
        .setRate(false)
        .setSliceConfig("50%")
        .build();
    assertNotEquals(t1.hashCode(), t2.hashCode());
    assertNotEquals(t1, t2);
    assertEquals(1, t1.compareTo(t2));
    
    t2 = new Timespan.Builder()
        .setStart("1h-ago")
        .setEnd("1m-ago")
        .setAggregator("sum")
        //.setTimezone("UTC")  // <-- diff
        .setDownsampler(new Downsampler.Builder()
            .setAggregator("sum")
            .setInterval("1h")
            .build())
        .setRate(false)
        .setSliceConfig("50%")
        .build();
    assertNotEquals(t1.hashCode(), t2.hashCode());
    assertNotEquals(t1, t2);
    assertEquals(1, t1.compareTo(t2));
    
    t2 = new Timespan.Builder()
        .setStart("1h-ago")
        .setEnd("1m-ago")
        .setAggregator("sum")
        .setTimezone("UTC")
        .setDownsampler(new Downsampler.Builder()
            .setAggregator("max")  // <-- diff
            .setInterval("1h")
            .build())
        .setRate(false)
        .setSliceConfig("50%")
        .build();
    assertNotEquals(t1.hashCode(), t2.hashCode());
    assertNotEquals(t1, t2);
    assertEquals(1, t1.compareTo(t2));
    
    t2 = new Timespan.Builder()
        .setStart("1h-ago")
        .setEnd("1m-ago")
        .setAggregator("sum")
        .setTimezone("UTC")
        //.setDownsampler(new Downsampler.Builder()  // <-- diff
        //    .setAggregator("sum")  
        //    .setInterval("1h")
        //    .build())
        .setRate(false)
        .setSliceConfig("50%")
        .build();
    assertNotEquals(t1.hashCode(), t2.hashCode());
    assertNotEquals(t1, t2);
    assertEquals(1, t1.compareTo(t2));
    
    t2 = new Timespan.Builder()
        .setStart("1h-ago")
        .setEnd("1m-ago")
        .setAggregator("sum")
        .setTimezone("UTC")
        .setDownsampler(new Downsampler.Builder()
            .setAggregator("sum")
            .setInterval("1h")
            .build())
        .setRate(true)  // <-- diff
        .setSliceConfig("50%")
        .build();
    assertNotEquals(t1.hashCode(), t2.hashCode());
    assertNotEquals(t1, t2);
    assertEquals(1, t1.compareTo(t2));
    
    t2 = new Timespan.Builder()
        .setStart("1h-ago")
        .setEnd("1m-ago")
        .setAggregator("sum")
        .setTimezone("UTC")
        .setDownsampler(new Downsampler.Builder()
            .setAggregator("sum")
            .setInterval("1h")
            .build())
        .setRate(false)
        .setSliceConfig("75%")   // <-- diff
        .build();
    assertNotEquals(t1.hashCode(), t2.hashCode());
    assertNotEquals(t1, t2);
    assertEquals(-1, t1.compareTo(t2));
    
    t2 = new Timespan.Builder()
        .setStart("1h-ago")
        .setEnd("1m-ago")
        .setAggregator("sum")
        .setTimezone("UTC")
        .setDownsampler(new Downsampler.Builder()
            .setAggregator("sum")
            .setInterval("1h")
            .build())
        .setRate(false)
        //.setSliceConfig("50%")   // <-- diff
        .build();
    assertNotEquals(t1.hashCode(), t2.hashCode());
    assertNotEquals(t1, t2);
    assertEquals(1, t1.compareTo(t2));
  }
}
