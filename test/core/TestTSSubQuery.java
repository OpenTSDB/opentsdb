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

import static net.opentsdb.core.TSSubQuery.DEFAULT_INTERPOLTION_WINDOW_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Test;

public final class TestTSSubQuery {

  @Test
  public void constructor() {
    assertNotNull(new TSSubQuery());
  }

  @Test
  public void validate() {
    TSSubQuery sub = getMetricForValidate();
    // Forces the default interpolation window.
    sub.setInterpolationWindowOption(null);
    sub.validateAndSetQuery();
    assertEquals("sys.cpu.0", sub.getMetric());
    assertEquals("*", sub.getTags().get("host"));
    assertEquals("lga", sub.getTags().get("dc"));
    assertEquals(Aggregators.SUM, sub.aggregator());
    DownsampleOptions predownsample_options = sub.getPredownsampleOptions();
    assertEquals(Aggregators.AVG, predownsample_options.getDownsampler());
    assertEquals(60000, predownsample_options.getIntervalMs());
    DownsampleOptions postdownsample_options = sub.getDownsampleOptions();
    assertEquals(Aggregators.AVG, postdownsample_options.getDownsampler());
    assertEquals(300000, postdownsample_options.getIntervalMs());
    assertEquals(DEFAULT_INTERPOLTION_WINDOW_MS,
        sub.interpolationWindowMillis());
  }

  @Test
  public void validateTS() {
    TSSubQuery sub = getMetricForValidate();
    // Forces the default pre downsampling.
    sub.setPredownsample(null);
    // Forces the default interpolation window.
    sub.setInterpolationWindowOption(null);
    // TSUID query should not have any metric query param.
    sub.setMetric(null);
    ArrayList<String> tsuids = new ArrayList<String>(1);
    tsuids.add("ABCD");
    sub.setTsuids(tsuids);
    sub.validateAndSetQuery();
    assertNotNull(sub.getTsuids());
    assertEquals("*", sub.getTags().get("host"));
    assertEquals("lga", sub.getTags().get("dc"));
    assertEquals(Aggregators.SUM, sub.aggregator());
    assertNull(sub.getPredownsampleOptions());
    DownsampleOptions postdownsample_options = sub.getDownsampleOptions();
    assertEquals(Aggregators.AVG, postdownsample_options.getDownsampler());
    assertEquals(300000, postdownsample_options.getIntervalMs());
    assertEquals(DEFAULT_INTERPOLTION_WINDOW_MS,
        sub.interpolationWindowMillis());
  }

  @Test
  public void validateNoDS() {
    TSSubQuery sub = getMetricForValidate();
    sub.setPredownsample(null);
    sub.setDownsample(null);
    sub.validateAndSetQuery();
    assertEquals("sys.cpu.0", sub.getMetric());
    assertEquals("*", sub.getTags().get("host"));
    assertEquals("lga", sub.getTags().get("dc"));
    assertEquals(Aggregators.SUM, sub.aggregator());
    assertNull(sub.getPredownsampleOptions());
    assertNull(sub.getDownsampleOptions());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateNullAgg() {
    TSSubQuery sub = getMetricForValidate();
    sub.setAggregator(null);
    sub.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateEmptyAgg() {
    TSSubQuery sub = getMetricForValidate();
    sub.setAggregator("");
    sub.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateBadAgg() {
    TSSubQuery sub = getMetricForValidate();
    sub.setAggregator("Notanagg");
    sub.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateNoMetricOrTsuids() {
    TSSubQuery sub = getMetricForValidate();
    sub.setMetric(null);
    sub.setTsuids(null);
    sub.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateNoMetricOrEmptyTsuids() {
    TSSubQuery sub = getMetricForValidate();
    sub.setMetric(null);
    sub.setTsuids(new ArrayList<String>());
    sub.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateBadDS() {
    TSSubQuery sub = getMetricForValidate();
    sub.setDownsample("bad");
    sub.validateAndSetQuery();
  }

  @Test
  public void validateMetricSubQuery_predownsampling() {
    TSSubQuery sub = getMetricForValidate();
    sub.setPredownsample("4m-max-pre");
    assertEquals("4m-max-pre", sub.getPredownsample());
    sub.validateAndSetQuery();
    assertEquals("sys.cpu.0", sub.getMetric());
    assertEquals("*", sub.getTags().get("host"));
    assertEquals("lga", sub.getTags().get("dc"));
    assertEquals(Aggregators.SUM, sub.aggregator());
    DownsampleOptions predownsample_options = sub.getPredownsampleOptions();
    assertEquals(Aggregators.MAX, predownsample_options.getDownsampler());
    assertEquals(240000, predownsample_options.getIntervalMs());
    DownsampleOptions postdownsample_options = sub.getDownsampleOptions();
    assertEquals(Aggregators.AVG, postdownsample_options.getDownsampler());
    assertEquals(300000, postdownsample_options.getIntervalMs());
  }

  @Test
  public void validateTsuid_predownsampling() {
    TSSubQuery sub = getMetricForValidate();
    sub.setMetric(null);
    sub.setPredownsample("4m-max-pre");
    assertEquals("4m-max-pre", sub.getPredownsample());
    ArrayList<String> tsuids = new ArrayList<String>(1);
    tsuids.add("ABCD");
    sub.setTsuids(tsuids);
    sub.validateAndSetQuery();
    assertNotNull(sub.getTsuids());
    assertEquals("*", sub.getTags().get("host"));
    assertEquals("lga", sub.getTags().get("dc"));
    assertEquals(Aggregators.SUM, sub.aggregator());
    DownsampleOptions predownsample_options = sub.getPredownsampleOptions();
    assertEquals(Aggregators.MAX, predownsample_options.getDownsampler());
    assertEquals(240000, predownsample_options.getIntervalMs());
    DownsampleOptions postdownsample_options = sub.getDownsampleOptions();
    assertEquals(Aggregators.AVG, postdownsample_options.getDownsampler());
    assertEquals(300000, postdownsample_options.getIntervalMs());
  }

  @Test
  public void validateMetricSubQuery_interpolationWindow() {
    TSSubQuery sub = getMetricForValidate();
    sub.setPredownsample("4m-max-pre");
    assertEquals("4m-max-pre", sub.getPredownsample());
    sub.setInterpolationWindowOption("iw-7m");
    assertEquals("iw-7m", sub.getInterpolationWindowOption());
    sub.validateAndSetQuery();
    assertEquals("sys.cpu.0", sub.getMetric());
    assertEquals("*", sub.getTags().get("host"));
    assertEquals("lga", sub.getTags().get("dc"));
    assertEquals(Aggregators.SUM, sub.aggregator());
    DownsampleOptions predownsample_options = sub.getPredownsampleOptions();
    assertEquals(Aggregators.MAX, predownsample_options.getDownsampler());
    assertEquals(240000, predownsample_options.getIntervalMs());
    DownsampleOptions postdownsample_options = sub.getDownsampleOptions();
    assertEquals(Aggregators.AVG, postdownsample_options.getDownsampler());
    assertEquals(300000, postdownsample_options.getIntervalMs());
    assertEquals(420000, sub.interpolationWindowMillis());
  }

  @Test
  public void validateTsuid_interpolationWindow() {
    TSSubQuery sub = getMetricForValidate();
    sub.setMetric(null);
    sub.setPredownsample("4m-max-pre");
    assertEquals("4m-max-pre", sub.getPredownsample());
    sub.setInterpolationWindowOption("iw-7m");
    assertEquals("iw-7m", sub.getInterpolationWindowOption());
    ArrayList<String> tsuids = new ArrayList<String>(1);
    tsuids.add("ABCD");
    sub.setTsuids(tsuids);
    sub.validateAndSetQuery();
    assertNotNull(sub.getTsuids());
    assertEquals("*", sub.getTags().get("host"));
    assertEquals("lga", sub.getTags().get("dc"));
    assertEquals(Aggregators.SUM, sub.aggregator());
    DownsampleOptions predownsample_options = sub.getPredownsampleOptions();
    assertEquals(Aggregators.MAX, predownsample_options.getDownsampler());
    assertEquals(240000, predownsample_options.getIntervalMs());
    DownsampleOptions postdownsample_options = sub.getDownsampleOptions();
    assertEquals(Aggregators.AVG, postdownsample_options.getDownsampler());
    assertEquals(300000, postdownsample_options.getIntervalMs());
    assertEquals(420000, sub.interpolationWindowMillis());
  }

  /**
   * Sets up an object with good, common values for testing the validation
   * function with an "m" type query (no tsuids). Each test can "set" the 
   * method it wants to fool with and call .validateAndSetQuery()
   * <b>Warning:</b> This method is also shared by {@link TestTSQuery} so be
   * careful if you change any values
   * @return A sub query object
   */
  public static TSSubQuery getMetricForValidate() {
    final TSSubQuery sub = new TSSubQuery();
    sub.setAggregator("sum");
    sub.setPredownsample("1m-avg-pre");
    sub.setDownsample("5m-avg");
    sub.setInterpolationWindowOption("iw-6m");
    sub.setMetric("sys.cpu.0");
    sub.setRate(false);
    final HashMap<String, String> tags = new HashMap<String, String>();
    tags.put("host", "*");
    tags.put("dc", "lga");
    sub.setTags(tags);
    return sub;
  }
}
