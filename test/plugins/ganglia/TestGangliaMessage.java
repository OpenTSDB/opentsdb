// This file is part of OpenTSDB.
// Copyright (C) 2014 The OpenTSDB Authors.
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
package net.opentsdb.plugins.ganglia;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


/** Tests {@link GangliaMessage}. */
public class TestGangliaMessage {

  private static final long NOW_MILLIS = 1397000152;

  @Mock private InetAddress mock_inet_addr;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testCreateHostnameTag() {
    GangliaMessage.Tag tag = GangliaMessage.createHostnameTag("foo");
    assertEquals("host", tag.key);
    assertEquals("foo", tag.value);
  }

  @Test
  public void testCreateIpAddrTag() {
    GangliaMessage.Tag tag = GangliaMessage.createIpAddrTag("foo.ip");
    assertEquals("ipaddr", tag.key);
    assertEquals("foo.ip", tag.value);
  }

  @Test
  public void testEmptyMessage() {
    GangliaMessage msg = GangliaMessage.emptyMessage();
    assertTrue(msg.hasError());
    assertFalse(msg.isMetadata());
    assertNull(msg.getMetricIfNumeric());
    assertFalse(msg.getTags(mock_inet_addr).iterator().hasNext());
  }

  @Test
  public void testEnsureNotNull() {
    String foo = "foo";
    assertSame(foo, GangliaMessage.ensureNotNull(foo, "foo is null"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEnsureNotNull_exception() {
    String foo = null;
    GangliaMessage.ensureNotNull(foo, "foo is null");
  }

  @Test
  public void testEnsureNotEmpty() {
    String foo = "foo";
    assertSame(foo, GangliaMessage.ensureNotEmpty(foo, "foo is null"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEnsureNotEmpty_exception() {
    String foo = null;
    GangliaMessage.ensureNotEmpty(foo, "foo is null");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEnsureNotEmpty_emptyString() {
    String foo = "";
    GangliaMessage.ensureNotEmpty(foo, "foo is null");
  }

  @Test
  public void testMetric_integer() {
    int value = 17;
    GangliaMessage.Metric metric = new GangliaMessage.Metric(
        "foo_metric", NOW_MILLIS, value);
    assertTrue(metric.isInteger());
    assertEquals("foo_metric", metric.name());
    assertEquals(NOW_MILLIS, metric.timestamp());
    assertEquals(17, metric.longValue());
  }

  @Test
  public void testMetric_float() {
    float value = 17;
    GangliaMessage.Metric metric = new GangliaMessage.Metric(
        "foo_metric", NOW_MILLIS, value);
    assertFalse(metric.isInteger());
    assertEquals("foo_metric", metric.name());
    assertEquals(NOW_MILLIS, metric.timestamp());
    assertEquals(17, metric.floatValue(), 0.0001);
  }
}
