// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.query.expression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.MutableDataPoint;
import net.opentsdb.core.SeekableView;
import net.opentsdb.meta.Annotation;

import org.hbase.async.Bytes.ByteMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({ Annotation.class })
public class TestPostAggregatedDataPoints {
  private static int NUM_POINTS = 5;
  private static String METRIC_NAME = "sys.cpu";
  private static long BASE_TIME = 1356998400000L;
  private static int TIME_INTERVAL = 60000;
  
  private DataPoints base_data_points;
  private DataPoint[] points;
  private Map<String, String> tags;
  private List<String> agg_tags;
  private List<String> tsuids;
  private List<Annotation> annotations;
  private ByteMap<byte[]> tag_uids;
  
  @Before
  public void before() throws Exception {
    base_data_points = PowerMockito.mock(DataPoints.class);
    points = new MutableDataPoint[NUM_POINTS];
    
    long ts = BASE_TIME;
    for (int i = 0; i < NUM_POINTS; i++) {
      MutableDataPoint mdp = new MutableDataPoint();
      mdp.reset(ts, i);
      points[i] = mdp;
      ts += TIME_INTERVAL;
    }
    
    tags = new HashMap<String, String>(1);
    tags.put("colo", "lga");
    agg_tags = new ArrayList<String>(1);
    agg_tags.add("host");
    tsuids = new ArrayList<String>(1);
    tsuids.add("0101010202"); // just 1 byte UIDs for kicks
    annotations = new ArrayList<Annotation>(1);
    annotations.add(PowerMockito.mock(Annotation.class));
    tag_uids = new ByteMap<byte[]>();
    tag_uids.put(new byte[] { 1 }, new byte[] { 1 });
    
    when(base_data_points.metricName()).thenReturn(METRIC_NAME);
    when(base_data_points.metricNameAsync()).thenReturn(
        Deferred.fromResult(METRIC_NAME));
    when(base_data_points.getTags()).thenReturn(tags);
    when(base_data_points.getTagsAsync()).thenReturn(Deferred.fromResult(tags));
    when(base_data_points.getAggregatedTags()).thenReturn(agg_tags);
    when(base_data_points.getAggregatedTagsAsync()).thenReturn(
        Deferred.fromResult(agg_tags));
    when(base_data_points.getTSUIDs()).thenReturn(tsuids);
    when(base_data_points.getAnnotations()).thenReturn(annotations);
    when(base_data_points.getTagUids()).thenReturn(tag_uids);
    when(base_data_points.getQueryIndex()).thenReturn(42);
  }
  
  @Test
  public void ctorDefaults() throws Exception {
    final PostAggregatedDataPoints dps = new PostAggregatedDataPoints(
        base_data_points, points);
    assertEquals(METRIC_NAME, dps.metricName());
    assertEquals(METRIC_NAME, dps.metricNameAsync().join());
    assertSame(tags, dps.getTags());
    assertSame(tags, dps.getTagsAsync().join());
    assertSame(agg_tags, dps.getAggregatedTags());
    assertSame(agg_tags, dps.getAggregatedTagsAsync().join());
    assertSame(tsuids, dps.getTSUIDs());
    assertSame(annotations, dps.getAnnotations());
    assertSame(tag_uids, dps.getTagUids());
    assertEquals(42, dps.getQueryIndex());
    assertEquals(5, dps.size());
    assertEquals(5, dps.aggregatedSize());
    
    // values
    final SeekableView iterator = dps.iterator();
    int values = 0;
    long value = 0;
    long ts = BASE_TIME;
    while(iterator.hasNext()) {
      final DataPoint dp = iterator.next();
      assertEquals(value++, dp.longValue());
      assertEquals(ts, dp.timestamp());
      ts += TIME_INTERVAL;
      values++;
    }
    assertEquals(5, values);
    assertFalse(iterator.hasNext());
    try {
      iterator.next();
      fail("Expected a NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    assertEquals(BASE_TIME + (4 * TIME_INTERVAL), dps.timestamp(4));
    assertEquals(4, dps.longValue(4));
    assertTrue(dps.isInteger(4));
    try {
      assertEquals(4, dps.doubleValue(4), 0.00);
      fail("Expected a ClassCastException");
    } catch (ClassCastException e) { }
    
    try {
      dps.timestamp(5);
      fail("Expected a ArrayIndexOutOfBoundsException");
    } catch (ArrayIndexOutOfBoundsException e) { }
    try {
      dps.isInteger(5);
      fail("Expected a ArrayIndexOutOfBoundsException");
    } catch (ArrayIndexOutOfBoundsException e) { }
    try {
      dps.longValue(5);
      fail("Expected a ArrayIndexOutOfBoundsException");
    } catch (ArrayIndexOutOfBoundsException e) { }
    try {
      dps.doubleValue(5);
      fail("Expected a ArrayIndexOutOfBoundsException");
    } catch (ArrayIndexOutOfBoundsException e) { }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullBase() throws Exception {
    new PostAggregatedDataPoints(null, points);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullPoints() throws Exception {
    new PostAggregatedDataPoints(base_data_points, null);
  }
  
  @Test
  public void alias() throws Exception {
    final PostAggregatedDataPoints dps = new PostAggregatedDataPoints(
        base_data_points, points);
    final String alias = "ein";
    dps.setAlias(alias);
    assertEquals(alias, dps.metricName());
    assertEquals(alias, dps.metricNameAsync().join());
    assertTrue(dps.getTags().isEmpty());
    assertTrue(dps.getTagsAsync().join().isEmpty());
    assertTrue(dps.getAggregatedTags().isEmpty());
    assertTrue(dps.getAggregatedTagsAsync().join().isEmpty());
    assertSame(tsuids, dps.getTSUIDs());
    assertSame(annotations, dps.getAnnotations());
    assertSame(tag_uids, dps.getTagUids());
    assertEquals(42, dps.getQueryIndex());
    assertEquals(5, dps.size());
    assertEquals(5, dps.aggregatedSize());
  }
  
  @Test
  public void emptyPoints() throws Exception {
    points = new MutableDataPoint[0];
    final PostAggregatedDataPoints dps = new PostAggregatedDataPoints(
        base_data_points, points);
    assertEquals(METRIC_NAME, dps.metricName());
    assertEquals(METRIC_NAME, dps.metricNameAsync().join());
    assertSame(tags, dps.getTags());
    assertSame(tags, dps.getTagsAsync().join());
    assertSame(agg_tags, dps.getAggregatedTags());
    assertSame(agg_tags, dps.getAggregatedTagsAsync().join());
    assertSame(tsuids, dps.getTSUIDs());
    assertSame(annotations, dps.getAnnotations());
    assertSame(tag_uids, dps.getTagUids());
    assertEquals(42, dps.getQueryIndex());
    assertEquals(0, dps.size());
    assertEquals(0, dps.aggregatedSize());
    
    // values
    final SeekableView iterator = dps.iterator();
    assertFalse(iterator.hasNext());
    try {
      iterator.next();
      fail("Expected a NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }
}
