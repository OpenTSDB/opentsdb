// This file is part of OpenTSDB.
// Copyright (C) 2020 The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package net.opentsdb.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.opentsdb.query.pojo.TagVFilter;
import net.opentsdb.query.pojo.TagVLiteralOrFilter;
import net.opentsdb.query.pojo.TagVRegexFilter;
import net.opentsdb.query.pojo.TagVWildcardFilter;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.Pair;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public final class TestTags {
  private TSDB tsdb;
  private Config config;

  @Test
  public void parseWithMetricWTag() {
    final HashMap<String, String> tags = new HashMap<String, String>(1);
    final String metric = Tags.parseWithMetric("sys.cpu.user{host=web01}", tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(1, tags.size());
    assertEquals("web01", tags.get("host"));
  }
  
  @Test
  public void parseWithMetricWTags() {
    final HashMap<String, String> tags = new HashMap<String, String>(2);
    final String metric = Tags.parseWithMetric("sys.cpu.user{host=web01,dc=lga}", 
        tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(2, tags.size());
    assertEquals("web01", tags.get("host"));
    assertEquals("lga", tags.get("dc"));
  }
  
  @Test
  public void parseWithMetricMetricOnly() {
    final HashMap<String, String> tags = new HashMap<String, String>(0);
    final String metric = Tags.parseWithMetric("sys.cpu.user", tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(0, tags.size());
  }
  
  @Test
  public void parseWithMetricMetricEmptyCurlies() {
    final HashMap<String, String> tags = new HashMap<String, String>(0);
    final String metric = Tags.parseWithMetric("sys.cpu.user{}", tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(0, tags.size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricNullMetric() {
    final HashMap<String, String> tags = new HashMap<String, String>(1);
    Tags.parseWithMetric("{host=}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricNullTagv() {
    final HashMap<String, String> tags = new HashMap<String, String>(1);
    Tags.parseWithMetric("sys.cpu.user{host=}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricNullTagk() {
    final HashMap<String, String> tags = new HashMap<String, String>(1);
    Tags.parseWithMetric("sys.cpu.user{=web01}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricNullTagv2() {
    final HashMap<String, String> tags = new HashMap<String, String>(2);
    Tags.parseWithMetric("sys.cpu.user{host=web01,dc=}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricNullTagk2() {
    final HashMap<String, String> tags = new HashMap<String, String>(2);
    Tags.parseWithMetric("sys.cpu.user{host=web01,=lga}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricNullTagv3() {
    final HashMap<String, String> tags = new HashMap<String, String>(3);
    Tags.parseWithMetric("sys.cpu.user{host=web01,dc=,=root}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricNullTagk3() {
    final HashMap<String, String> tags = new HashMap<String, String>(3);
    Tags.parseWithMetric("sys.cpu.user{host=web01,=lga,owner=}", tags);
  }
  
  @Test (expected = NullPointerException.class)
  public void parseWithMetricNull() {
    final HashMap<String, String> tags = new HashMap<String, String>(0);
    Tags.parseWithMetric(null, tags);
  }
  
  @Test
  public void parseWithMetricEmpty() {
    final HashMap<String, String> tags = new HashMap<String, String>(0);
    assertTrue(Tags.parseWithMetric("", tags).isEmpty());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricMissingClosingCurly() {
    final HashMap<String, String> tags = new HashMap<String, String>(0);
    Tags.parseWithMetric("sys.cpu.user{host=web01", tags);
  }

  // Maybe this one should throw an exception. Usually this will be used before
  // a UID lookup so it will toss an exception then.
  @Test
  public void parseWithMetricMissingOpeningCurly() {
    final HashMap<String, String> tags = new HashMap<String, String>(0);
    assertEquals("sys.cpu.user host=web01}",
        Tags.parseWithMetric("sys.cpu.user host=web01}", tags));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricMissingEquals() {
    final HashMap<String, String> tags = new HashMap<String, String>(0);
    Tags.parseWithMetric("sys.cpu.user{hostweb01}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricMissingComma() {
    final HashMap<String, String> tags = new HashMap<String, String>(0);
    Tags.parseWithMetric("sys.cpu.user{host=web01 dc=lga}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricTrailingComma() {
    final HashMap<String, String> tags = new HashMap<String, String>(0);
    Tags.parseWithMetric("sys.cpu.user{host=web01,}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricForwardComma() {
    final HashMap<String, String> tags = new HashMap<String, String>(0);
    Tags.parseWithMetric("sys.cpu.user{,host=web01}", tags);
  }

  @Test
  public void parseWithMetricListMetricOnly() {
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(0);
    final String metric = Tags.parseWithMetric("sys.cpu.user", tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(0, tags.size());
  }
  
  @Test
  public void parseWithMetricListMetricEmptyCurlies() {
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(0);
    final String metric = Tags.parseWithMetric("sys.cpu.user{}", tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(0, tags.size());
  }
  
  @Test
  public void parseWithMetricListNullMetric() {
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    final String metric = Tags.parseWithMetric("{host=}", tags);
    assertNull(metric);
    assertEquals(1, tags.size());
    assertEquals("host", tags.get(0).getKey());
    assertNull(tags.get(0).getValue());
  }
  
  @Test
  public void parseWithMetricListNullTagv() {
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    final String metric = Tags.parseWithMetric("sys.cpu.user{host=}", tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(1, tags.size());
    assertEquals("host", tags.get(0).getKey());
    assertNull(tags.get(0).getValue());
  }
  
  @Test
  public void parseWithMetricListNullTagk() {
    final List<Pair<String, String>> tags = new ArrayList<Pair<String, String>>(1);
    final String metric = Tags.parseWithMetric("sys.cpu.user{=web01}", tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(1, tags.size());
    assertNull(tags.get(0).getKey());
    assertEquals("web01", tags.get(0).getValue());
  }
  
  @Test
  public void parseWithMetricListWTag() {
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    final String metric = Tags.parseWithMetric("sys.cpu.user{host=web01}", tags);
    assertEquals("sys.cpu.user", metric);
    assertEquals(1, tags.size());
    assertEquals("host", tags.get(0).getKey());
    assertEquals("web01", tags.get(0).getValue());
  }
  
  @Test
  public void parseWithMetricListNullTagv2() {
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(2);
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
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(2);
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
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(3);
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
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(3);
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
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(0);
    Tags.parseWithMetric(null, tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricListEmpty() {
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(0);
    Tags.parseWithMetric("", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricListMissingClosingCurly() {
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(0);
    Tags.parseWithMetric("sys.cpu.user{host=web01", tags);
  }

  // Maybe this one should throw an exception. Usually this will be used before
  // a UID lookup so it will toss an exception then.
  @Test
  public void parseWithMetricListMissingOpeningCurly() {
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(0);
    assertEquals("sys.cpu.user host=web01}",
        Tags.parseWithMetric("sys.cpu.user host=web01}", tags));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricListMissingEquals() {
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(0);
    Tags.parseWithMetric("sys.cpu.user{hostweb01}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricListMissingComma() {
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(0);
    Tags.parseWithMetric("sys.cpu.user{host=web01 dc=lga}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricListTrailingComma() {
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(0);
    Tags.parseWithMetric("sys.cpu.user{host=web01,}", tags);
  }

  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricListForwardComma() {
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(0);
    Tags.parseWithMetric("sys.cpu.user{,host=web01}", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricOnlyEquals() {
    final HashMap<String, String> tags = new HashMap<String, String>(0);
    Tags.parseWithMetric("{=}", tags);
  }
  
  @Test
  public void parseWithMetricAndFilters() {
    final List<TagVFilter> filters = new ArrayList<TagVFilter>();
    String metric = Tags.parseWithMetricAndFilters("sys.cpu.user", filters);
    assertEquals("sys.cpu.user", metric);
    assertEquals(0, filters.size());
    
    filters.clear();
    metric = Tags.parseWithMetricAndFilters("sys.cpu.user{host=web01}", filters);
    assertEquals("sys.cpu.user", metric);
    assertEquals(1, filters.size());
    assertEquals("host", filters.get(0).getTagk());
    assertTrue(filters.get(0).isGroupBy());
    assertTrue(filters.get(0) instanceof TagVLiteralOrFilter);
    
    filters.clear();
    metric = Tags.parseWithMetricAndFilters("sys.cpu.user{host=*}", filters);
    assertEquals("sys.cpu.user", metric);
    assertEquals(1, filters.size());
    assertEquals("host", filters.get(0).getTagk());
    assertTrue(filters.get(0).isGroupBy());
    assertTrue(filters.get(0) instanceof TagVWildcardFilter);
    
    filters.clear();
    metric = Tags.parseWithMetricAndFilters("sys.cpu.user{host=web01}{}", filters);
    assertEquals("sys.cpu.user", metric);
    assertEquals(1, filters.size());
    assertEquals("host", filters.get(0).getTagk());
    assertTrue(filters.get(0).isGroupBy());
    assertTrue(filters.get(0) instanceof TagVLiteralOrFilter);
    
    filters.clear();
    metric = Tags.parseWithMetricAndFilters(
        "sys.cpu.user{host=*,owner=regexp(.*ob)}", filters);
    assertEquals("sys.cpu.user", metric);
    assertEquals(2, filters.size());
    for (final TagVFilter filter : filters) {
      if (filter instanceof TagVWildcardFilter) {
        assertEquals("host", filter.getTagk());
      } else if (filter instanceof TagVRegexFilter) {
        assertEquals("owner", filter.getTagk());
      }
      assertTrue(filter.isGroupBy());
    }
    
    filters.clear();
    metric = Tags.parseWithMetricAndFilters("sys.cpu.user{}{host=web01}", filters);
    assertEquals("sys.cpu.user", metric);
    assertEquals(1, filters.size());
    assertEquals("host", filters.get(0).getTagk());
    assertFalse(filters.get(0).isGroupBy());
    assertTrue(filters.get(0) instanceof TagVLiteralOrFilter);
    
    filters.clear();
    metric = Tags.parseWithMetricAndFilters(
        "sys.cpu.user{}{host=iliteral_or(web01|Web02)}", filters);
    assertEquals("sys.cpu.user", metric);
    assertEquals(1, filters.size());
    assertEquals("host", filters.get(0).getTagk());
    assertFalse(filters.get(0).isGroupBy());
    assertTrue(filters.get(0) instanceof TagVLiteralOrFilter.TagVILiteralOrFilter);
    
    filters.clear();
    metric = Tags.parseWithMetricAndFilters(
        "sys.cpu.user{}{host=iliteral_or(web01|Web02),owner=*}", filters);
    assertEquals("sys.cpu.user", metric);
    assertEquals(2, filters.size());
    for (final TagVFilter filter : filters) {
      if (filter instanceof TagVWildcardFilter) {
        assertEquals("owner", filter.getTagk());
      } else if (filter instanceof TagVLiteralOrFilter.TagVILiteralOrFilter) {
        assertEquals("host", filter.getTagk());
      }
      assertFalse(filter.isGroupBy());
    }
    
    filters.clear();
    metric = Tags.parseWithMetricAndFilters(
        "sys.cpu.user{host=iliteral_or(web01|Web02)}{owner=*}", filters);
    assertEquals("sys.cpu.user", metric);
    System.out.println(filters);
    assertEquals(2, filters.size());
    for (final TagVFilter filter : filters) {
      if (filter instanceof TagVWildcardFilter) {
        assertEquals("owner", filter.getTagk());
        assertFalse(filter.isGroupBy());
      } else if (filter instanceof TagVLiteralOrFilter.TagVILiteralOrFilter) {
        assertEquals("host", filter.getTagk());
        assertTrue(filter.isGroupBy());
      }
    }
  }

  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricAndFiltersMissingTrailingCurly() {
    final List<TagVFilter> filters = new ArrayList<TagVFilter>();
    Tags.parseWithMetricAndFilters("sys.cpu.user{}{host=web01", filters);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricAndFiltersNullString() {
    final List<TagVFilter> filters = new ArrayList<TagVFilter>();
    Tags.parseWithMetricAndFilters(null, filters);
  }

  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricAndFiltersEmptyString() {
    final List<TagVFilter> filters = new ArrayList<TagVFilter>();
    Tags.parseWithMetricAndFilters("", filters);
  }

  @Test (expected = IllegalArgumentException.class)
  public void parseWithMetricAndFiltersNullFilters() {
    Tags.parseWithMetricAndFilters("sys.cpu.user{}{host=web01}", null);
  }
  @Test
  public void parseSuccessful() {
    final HashMap<String, String> tags = new HashMap<String, String>(2);
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
    final HashMap<String, String> tags = new HashMap<String, String>(1);
    Tags.parse(tags, "foo=bar");
    assertEquals(1, tags.size());
    assertEquals("bar", tags.get("foo"));
    Tags.parse(tags, "foo=qux");
  }

  @Test
  public void parseSameValues() {
    final HashMap<String, String> tags = new HashMap<String, String>(1);
    Tags.parse(tags, "foo=bar");
    assertEquals(1, tags.size());
    assertEquals("bar", tags.get("foo"));
    Tags.parse(tags, "foo=bar");
    assertEquals(1, tags.size());
    assertEquals("bar", tags.get("foo"));
  }

  // parseLong

  @Test
  public void parseLongSimple() {
    assertEquals(0, Tags.parseLong("0"));
    assertEquals(0, Tags.parseLong("+0"));
    assertEquals(0, Tags.parseLong("-0"));
    assertEquals(1, Tags.parseLong("1"));
    assertEquals(1, Tags.parseLong("+1"));
    assertEquals(-1, Tags.parseLong("-1"));
    assertEquals(4242, Tags.parseLong("4242"));
    assertEquals(4242, Tags.parseLong("+4242"));
    assertEquals(-4242, Tags.parseLong("-4242"));
  }

  @Test
  public void parseLongMaxValue() {
    assertEquals(Long.MAX_VALUE, Tags.parseLong(Long.toString(Long.MAX_VALUE)));
  }

  @Test
  public void parseLongMinValue() {
    assertEquals(Long.MIN_VALUE, Tags.parseLong(Long.toString(Long.MIN_VALUE)));
  }

  @Test(expected=NumberFormatException.class)
  public void parseLongEmptyString() {
    Tags.parseLong("");
  }

  @Test(expected=NumberFormatException.class)
  public void parseLongMalformed() {
    Tags.parseLong("42a51");
  }

  @Test(expected=NumberFormatException.class)
  public void parseLongMalformedPlus() {
    Tags.parseLong("+");
  }

  @Test(expected=NumberFormatException.class)
  public void parseLongMalformedMinus() {
    Tags.parseLong("-");
  }

  @Test(expected=NumberFormatException.class)
  public void parseLongValueTooLarge() {
    Tags.parseLong("18446744073709551616");
  }

  @Test(expected=NumberFormatException.class)
  public void parseLongValueTooLargeSubtle() {
    Tags.parseLong("9223372036854775808"); // MAX_VALUE + 1
  }

  @Test(expected=NumberFormatException.class)
  public void parseLongValueTooSmallSubtle() {
    Tags.parseLong("-9223372036854775809"); // MIN_VALUE - 1
  }
}
