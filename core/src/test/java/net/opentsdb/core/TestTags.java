// This file is part of OpenTSDB.
// Copyright (C) 2011-2012  The OpenTSDB Authors.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.opentsdb.utils.Pair;

import org.junit.Test;

import static org.junit.Assert.*;

public final class TestTags {
  @Test
  public void parseWithMetricWTag() {
    final HashMap<String, String> tags = new HashMap<>(1);
    final String metric = Tags.parseWithMetric("sys.cpu.user{host=web01}", tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(1, tags.size());
    assertEquals("web01", tags.get("host"));
  }
  
  @Test
  public void parseWithMetricWTags() {
    final HashMap<String, String> tags = new HashMap<>(2);
    final String metric = Tags.parseWithMetric("sys.cpu.user{host=web01,dc=lga}", 
        tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(2, tags.size());
    assertEquals("web01", tags.get("host"));
    assertEquals("lga", tags.get("dc"));
  }
  
  @Test
  public void parseWithMetricMetricOnly() {
    final HashMap<String, String> tags = new HashMap<>(0);
    final String metric = Tags.parseWithMetric("sys.cpu.user", tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(0, tags.size());
  }
  
  @Test
  public void parseWithMetricMetricEmptyCurlies() {
    final HashMap<String, String> tags = new HashMap<>(0);
    final String metric = Tags.parseWithMetric("sys.cpu.user{}", tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(0, tags.size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricNullMetric() {
    final HashMap<String, String> tags = new HashMap<>(1);
    Tags.parseWithMetric("{host=}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricNullTagv() {
    final HashMap<String, String> tags = new HashMap<>(1);
    Tags.parseWithMetric("sys.cpu.user{host=}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricNullTagk() {
    final HashMap<String, String> tags = new HashMap<>(1);
    Tags.parseWithMetric("sys.cpu.user{=web01}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricNullTagv2() {
    final HashMap<String, String> tags = new HashMap<>(2);
    Tags.parseWithMetric("sys.cpu.user{host=web01,dc=}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricNullTagk2() {
    final HashMap<String, String> tags = new HashMap<>(2);
    Tags.parseWithMetric("sys.cpu.user{host=web01,=lga}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricNullTagv3() {
    final HashMap<String, String> tags = new HashMap<>(3);
    Tags.parseWithMetric("sys.cpu.user{host=web01,dc=,=root}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricNullTagk3() {
    final HashMap<String, String> tags = new HashMap<>(3);
    Tags.parseWithMetric("sys.cpu.user{host=web01,=lga,owner=}", tags);
  }
  
  @Test (expected = NullPointerException.class)
  public void parseWithMetricNull() {
    final HashMap<String, String> tags = new HashMap<>(0);
    Tags.parseWithMetric(null, tags);
  }
  
  @Test
  public void parseWithMetricEmpty() {
    final HashMap<String, String> tags = new HashMap<>(0);
    assertTrue(Tags.parseWithMetric("", tags).isEmpty());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricMissingClosingCurly() {
    final HashMap<String, String> tags = new HashMap<>(0);
    Tags.parseWithMetric("sys.cpu.user{host=web01", tags);
  }

  // Maybe this one should throw an exception. Usually this will be used before
  // a UID lookup so it will toss an exception then.
  @Test
  public void parseWithMetricMissingOpeningCurly() {
    final HashMap<String, String> tags = new HashMap<>(0);
    assertEquals("sys.cpu.user host=web01}",
        Tags.parseWithMetric("sys.cpu.user host=web01}", tags));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricMissingEquals() {
    final HashMap<String, String> tags = new HashMap<>(0);
    Tags.parseWithMetric("sys.cpu.user{hostweb01}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricMissingComma() {
    final HashMap<String, String> tags = new HashMap<>(0);
    Tags.parseWithMetric("sys.cpu.user{host=web01 dc=lga}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricTrailingComma() {
    final HashMap<String, String> tags = new HashMap<>(0);
    Tags.parseWithMetric("sys.cpu.user{host=web01,}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricForwardComma() {
    final HashMap<String, String> tags = new HashMap<>(0);
    Tags.parseWithMetric("sys.cpu.user{,host=web01}", tags);
  }

  @Test
  public void parseWithMetricListMetricOnly() {
    final List<Pair<String, String>> tags = new ArrayList<>(0);
    final String metric = Tags.parseWithMetric("sys.cpu.user", tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(0, tags.size());
  }
  
  @Test
  public void parseWithMetricListMetricEmptyCurlies() {
    final List<Pair<String, String>> tags = new ArrayList<>(0);
    final String metric = Tags.parseWithMetric("sys.cpu.user{}", tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(0, tags.size());
  }
  
  @Test
  public void parseWithMetricListNullMetric() {
    final List<Pair<String, String>> tags = new ArrayList<>(1);
    final String metric = Tags.parseWithMetric("{host=}", tags);
    assertNull(metric);
    assertEquals(1, tags.size());
    assertEquals("host", tags.get(0).getKey());
    assertNull(tags.get(0).getValue());
  }
  
  @Test
  public void parseWithMetricListNullTagv() {
    final List<Pair<String, String>> tags = new ArrayList<>(1);
    final String metric = Tags.parseWithMetric("sys.cpu.user{host=}", tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(1, tags.size());
    assertEquals("host", tags.get(0).getKey());
    assertNull(tags.get(0).getValue());
  }
  
  @Test
  public void parseWithMetricListNullTagk() {
    final List<Pair<String, String>> tags = new ArrayList<>(1);
    final String metric = Tags.parseWithMetric("sys.cpu.user{=web01}", tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(1, tags.size());
    assertNull(tags.get(0).getKey());
    assertEquals("web01", tags.get(0).getValue());
  }
  
  @Test
  public void parseWithMetricListWTag() {
    final List<Pair<String, String>> tags = new ArrayList<>(1);
    final String metric = Tags.parseWithMetric("sys.cpu.user{host=web01}", tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(1, tags.size());
    assertEquals("host", tags.get(0).getKey());
    assertEquals("web01", tags.get(0).getValue());
  }
  
  @Test
  public void parseWithMetricListNullTagv2() {
    final List<Pair<String, String>> tags = new ArrayList<>(2);
    final String metric = Tags.parseWithMetric("sys.cpu.user{host=web01,dc=}", 
        tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(2, tags.size());
    assertEquals("host", tags.get(0).getKey());
    assertEquals("web01", tags.get(0).getValue());
    assertEquals("dc", tags.get(1).getKey());
    assertNull(tags.get(1).getValue());
  }
  
  @Test
  public void parseWithMetricListNullTagk2() {
    final List<Pair<String, String>> tags = new ArrayList<>(2);
    final String metric = Tags.parseWithMetric("sys.cpu.user{host=web01,=lga}", 
        tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(2, tags.size());
    assertEquals("host", tags.get(0).getKey());
    assertEquals("web01", tags.get(0).getValue());
    assertNull(tags.get(1).getKey());
    assertEquals("lga", tags.get(1).getValue());
  }
  
  @Test
  public void parseWithMetricListNullTagv3() {
    final List<Pair<String, String>> tags = new ArrayList<>(3);
    final String metric = Tags.parseWithMetric("sys.cpu.user{host=web01,dc=,=root}", 
        tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(3, tags.size());
    assertEquals("host", tags.get(0).getKey());
    assertEquals("web01", tags.get(0).getValue());
    assertEquals("dc", tags.get(1).getKey());
    assertNull(tags.get(1).getValue());
    assertNull(tags.get(2).getKey());
    assertEquals("root", tags.get(2).getValue());
  }
  
  @Test
  public void parseWithMetricListNullTagk3() {
    final List<Pair<String, String>> tags = new ArrayList<>(3);
    final String metric = Tags.parseWithMetric("sys.cpu.user{host=web01,=lga,owner=}", 
        tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(3, tags.size());
    assertEquals("host", tags.get(0).getKey());
    assertEquals("web01", tags.get(0).getValue());
    assertNull(tags.get(1).getKey());
    assertEquals("lga", tags.get(1).getValue());
    assertEquals("owner", tags.get(2).getKey());
    assertNull(tags.get(2).getValue());
  }
  
  @Test (expected = NullPointerException.class)
  public void parseWithMetricListNull() {
    final List<Pair<String, String>> tags = new ArrayList<>(0);
    Tags.parseWithMetric(null, tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricListEmpty() {
    final List<Pair<String, String>> tags = 
        new ArrayList<>(0);
    Tags.parseWithMetric("", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricListMissingClosingCurly() {
    final List<Pair<String, String>> tags = new ArrayList<>(0);
    Tags.parseWithMetric("sys.cpu.user{host=web01", tags);
  }

  // Maybe this one should throw an exception. Usually this will be used before
  // a UID lookup so it will toss an exception then.
  @Test
  public void parseWithMetricListMissingOpeningCurly() {
    final List<Pair<String, String>> tags = new ArrayList<>(0);
    assertEquals("sys.cpu.user host=web01}",
        Tags.parseWithMetric("sys.cpu.user host=web01}", tags));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricListMissingEquals() {
    final List<Pair<String, String>> tags = new ArrayList<>(0);
    Tags.parseWithMetric("sys.cpu.user{hostweb01}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricListMissingComma() {
    final List<Pair<String, String>> tags = new ArrayList<>(0);
    Tags.parseWithMetric("sys.cpu.user{host=web01 dc=lga}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricListTrailingComma() {
    final List<Pair<String, String>> tags = 
        new ArrayList<>(0);
    Tags.parseWithMetric("sys.cpu.user{host=web01,}", tags);
  }

  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricListForwardComma() {
    final List<Pair<String, String>> tags = 
        new ArrayList<>(0);
    Tags.parseWithMetric("sys.cpu.user{,host=web01}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricOnlyEquals() {
    final HashMap<String, String> tags = new HashMap<>(0);
    Tags.parseWithMetric("{=}", tags);
  }
  
  @Test
  public void parseSuccessful() {
    final HashMap<String, String> tags = new HashMap<>(2);
    Tags.parse(tags, "foo=bar");
    assertEquals(1, tags.size());
    assertEquals("bar", tags.get("foo"));
    Tags.parse(tags, "qux=baz");
    assertEquals(2, tags.size());
    assertEquals("bar", tags.get("foo"));
    assertEquals("baz", tags.get("qux"));
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void parseNoEqualSign() {
    Tags.parse(new HashMap<String, String>(1), "foo");
  }

  @Test(expected=IllegalArgumentException.class)
  public void parseTooManyEqualSigns() {
    Tags.parse(new HashMap<String, String>(1), "foo=bar=qux");
  }

  @Test(expected=IllegalArgumentException.class)
  public void parseEmptyTagName() {
    Tags.parse(new HashMap<String, String>(1), "=bar");
  }

  @Test(expected=IllegalArgumentException.class)
  public void parseEmptyTagValue() {
    Tags.parse(new HashMap<String, String>(1), "foo=");
  }

  @Test(expected=IllegalArgumentException.class)
  public void parseDifferentValues() {
    final HashMap<String, String> tags = new HashMap<>(1);
    Tags.parse(tags, "foo=bar");
    assertEquals(1, tags.size());
    assertEquals("bar", tags.get("foo"));
    Tags.parse(tags, "foo=qux");
  }

  @Test
  public void parseSameValues() {
    final HashMap<String, String> tags = new HashMap<>(1);
    Tags.parse(tags, "foo=bar");
    assertEquals(1, tags.size());
    assertEquals("bar", tags.get("foo"));
    Tags.parse(tags, "foo=bar");
    assertEquals(1, tags.size());
    assertEquals("bar", tags.get("foo"));
  }
}
