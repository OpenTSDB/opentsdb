// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.hbase.async.HBaseClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, Config.class, UniqueId.class, HBaseClient.class,
    IncomingDataPoints.class })
public class TestBatchedDataPoints {
  private static Config config;
  private static TSDB tsdb = null;
  private HBaseClient client = mock(HBaseClient.class);
  private UniqueId metrics = mock(UniqueId.class);
  private UniqueId tag_names = mock(UniqueId.class);
  private UniqueId tag_values = mock(UniqueId.class);
  private BatchedDataPoints bdp = null;

  @SuppressWarnings("unchecked")
  @Before
  public void before() throws Exception {
    config = new Config(false);
    tsdb = new TSDB(client, config);

    // replace the "real" field objects with mocks
    Field met = tsdb.getClass().getDeclaredField("metrics");
    met.setAccessible(true);
    met.set(tsdb, metrics);

    Field tagk = tsdb.getClass().getDeclaredField("tag_names");
    tagk.setAccessible(true);
    tagk.set(tsdb, tag_names);

    Field tagv = tsdb.getClass().getDeclaredField("tag_values");
    tagv.setAccessible(true);
    tagv.set(tsdb, tag_values);

    when(metrics.width()).thenReturn((short) 3);
    when(tag_names.width()).thenReturn((short) 3);
    when(tag_values.width()).thenReturn((short) 3);

    when(metrics.getId("foo")).thenReturn(new byte[] { 0, 0, 1 });
    when(metrics.getNameAsync(new byte[] { 0, 0, 1 })).thenReturn(
        Deferred.fromResult("foo"));

    PowerMockito.mockStatic(IncomingDataPoints.class);
    final byte[] row = new byte[] { 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1 };
    PowerMockito.doAnswer(new Answer<byte[]>() {
      public byte[] answer(final InvocationOnMock unused) throws Exception {
        return row;
      }
    }).when(IncomingDataPoints.class, "rowKeyTemplate", (TSDB) any(),
        anyString(), (Map<String, String>) any());

    Map<String, String> tags = new HashMap<String, String>();
    tags.put("host", "web01");
    bdp = new BatchedDataPoints(tsdb, "foo", tags);
  }

  @Test
  public void timestamp() {
    bdp.addPoint(1388534400L, 1);
    bdp.addPoint(1388534400500L, 2);
    bdp.addPoint(1388534400750L, 2);

    assertEquals(1388534400000L, bdp.timestamp(0));
    assertEquals(1388534400500L, bdp.timestamp(1));
    assertEquals(1388534400750L, bdp.timestamp(2));
  }

  @Test
  public void isInteger() {
    bdp.addPoint(1388534400L, 1);
    bdp.addPoint(1388534400500L, Short.MIN_VALUE);
    bdp.addPoint(1388534401L, Integer.MIN_VALUE);
    bdp.addPoint(1388534401750L, 2.0f);

    assertTrue(bdp.isInteger(0));
    assertTrue(bdp.isInteger(1));
    assertTrue(bdp.isInteger(2));
    assertFalse(bdp.isInteger(3));
  }

  @Test
  public void longValue() {
    bdp.addPoint(1388534400L, 1);
    bdp.addPoint(1388534400500L, Short.MIN_VALUE);
    bdp.addPoint(1388534401L, Integer.MIN_VALUE);
    bdp.addPoint(1388534401750L, 2.0f);

    assertEquals(1, bdp.longValue(0));
    assertEquals(Short.MIN_VALUE, bdp.longValue(1));
    assertEquals(Integer.MIN_VALUE, bdp.longValue(2));
  }

  @Test
  public void doubleValue() {
    bdp.addPoint(1388534400L, 1);
    bdp.addPoint(1388534400500L, Short.MIN_VALUE);
    bdp.addPoint(1388534401L, Integer.MIN_VALUE);
    bdp.addPoint(1388534401750L, 2.0f);

    Assert.assertEquals(2.0f, bdp.doubleValue(3), 0.00001f);
  }

  @Test
  public void inBounds() {
    long timestamp = 1388534400L;
    long base_time = (timestamp - (timestamp % Const.MAX_TIMESPAN));

    // test that the limit values are fine
    bdp.addPoint(base_time, 2);
    bdp.addPoint(base_time + Const.MAX_TIMESPAN - 1, 3);
  }

  @Test(expected = IllegalArgumentException.class)
  public void addPrevTimestamp() {
    bdp.addPoint(1388534400L, 1);
    bdp.addPoint(1388534350L, 2);
  }

  @Test(expected = IllegalDataException.class)
  public void outOfBounds() {
    long timestamp = 1388534400L;
    long base_time = (timestamp - (timestamp % Const.MAX_TIMESPAN));

    bdp.addPoint(timestamp, 1); // this set's the batch's baseTime

    // outside the limits addPoint throws an IllegalArgumentException
    bdp.addPoint(base_time + Const.MAX_TIMESPAN, 2);
  }

  @Test
  public void fullLoad() {
    long timestamp = 1388534400L;
    long base_time = (timestamp - (timestamp % Const.MAX_TIMESPAN));

    for (int i = 0; i < Const.MAX_TIMESPAN; i++) {
      bdp.addPoint(base_time + i, i);
    }
  }

  @Test
  public void fullLoadMs() {
    long timestamp = 1388534400L;
    long base_time = (timestamp - (timestamp % Const.MAX_TIMESPAN));

    for (long i = 0; i < Const.MAX_TIMESPAN * 1000; i++) {
      bdp.addPoint(base_time * 1000 + i, i);
    }
  }
}
