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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.filter.TagVLiteralOrFilter;
import net.opentsdb.query.filter.TagVLiteralOrFilter.TagVILiteralOrFilter;
import net.opentsdb.query.filter.TagVRegexFilter;
import net.opentsdb.query.filter.TagVWildcardFilter;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.FailedToAssignUniqueIdException;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.Pair;

import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Bytes.ByteMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class, 
  GetRequest.class, PutRequest.class, DeleteRequest.class, KeyValue.class, 
  Const.class})
public final class TestTags {
  private TSDB tsdb;
  private Config config;
  private HBaseClient client;
  private MockBase storage = null;
  private UniqueId metrics = mock(UniqueId.class);
  private UniqueId tag_names = mock(UniqueId.class);
  private UniqueId tag_values = mock(UniqueId.class);
  
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
    assertTrue(filters.get(0) instanceof TagVILiteralOrFilter);
    
    filters.clear();
    metric = Tags.parseWithMetricAndFilters(
        "sys.cpu.user{}{host=iliteral_or(web01|Web02),owner=*}", filters);
    assertEquals("sys.cpu.user", metric);
    assertEquals(2, filters.size());
    for (final TagVFilter filter : filters) {
      if (filter instanceof TagVWildcardFilter) {
        assertEquals("owner", filter.getTagk());
      } else if (filter instanceof TagVILiteralOrFilter) {
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
      } else if (filter instanceof TagVILiteralOrFilter) {
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

  @Test
  public void validateGoodString() {
    Tags.validateString("test", "omg-TSDB/42._foo_");
  }

  @Test(expected=IllegalArgumentException.class)
  public void validateNullString() {
    Tags.validateString("test", null);
  }

  @Test(expected=IllegalArgumentException.class)
  public void validateBadString() {
    Tags.validateString("test", "this is a test!");
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

  @Test
  public void resolveIdsAsync() throws Exception {
    setupStorage();
    setupResolveIds();
    
    final List<byte[]> ids = new ArrayList<byte[]>(1);
    ids.add(new byte[] { 0, 0, 1, 0, 0, 1 });
    final HashMap<String, String> tags = Tags.resolveIdsAsync(tsdb, ids)
      .joinUninterruptibly();
    assertEquals("web01", tags.get("host"));
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void resolveIdsAsyncNSUI() throws Exception {
    setupStorage();
    setupResolveIds();
    
    final List<byte[]> ids = new ArrayList<byte[]>(1);
    ids.add(new byte[] { 0, 0, 1, 0, 0, 2 });
    Tags.resolveIdsAsync(tsdb, ids).joinUninterruptibly();
  }
  
  @Test
  public void resolveIdsAsyncEmptyList() throws Exception {
    setupStorage();
    setupResolveIds();
    
    final List<byte[]> ids = new ArrayList<byte[]>(0);
    final HashMap<String, String> tags = Tags.resolveIdsAsync(tsdb, ids)
      .joinUninterruptibly();
    assertNotNull(tags);
    assertEquals(0, tags.size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void resolveIdsAsyncWrongLength() throws Exception {
    setupStorage();
    setupResolveIds();
    
    final List<byte[]> ids = new ArrayList<byte[]>(1);
    ids.add(new byte[] { 0, 0, 1, 0, 0, 0, 2 });
    Tags.resolveIdsAsync(tsdb, ids).joinUninterruptibly();
  }

  @Test
  public void resolveOrCreateAllCreate() throws Exception {
    setupStorage();
    setupResolveAll();
    
    final Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    final List<byte[]> uids = Tags.resolveOrCreateAll(tsdb, tags);
    assertEquals(1, uids.size());
    assertArrayEquals(new byte[] { 0, 0, 1, 0, 0, 1}, uids.get(0));
  }
  
  @Test
  public void resolveOrCreateTagkAllowed() throws Exception {
    setupStorage();
    setupResolveAll();
    
    final Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("doesnotexist", "web01");
    final List<byte[]> uids = Tags.resolveOrCreateAll(tsdb, tags);
    assertEquals(1, uids.size());
    assertArrayEquals(new byte[] { 0, 0, 3, 0, 0, 1}, uids.get(0));
  }
  
  @Test
  public void resolveOrCreateTagkNotAllowedGood() throws Exception {
    setupStorage();
    config.overrideConfig("tsd.core.auto_create_tagks", "false");
    setupResolveAll();
    
    final Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("pop", "web01");
    final List<byte[]> uids = Tags.resolveOrCreateAll(tsdb, tags);
    assertEquals(1, uids.size());
    assertArrayEquals(new byte[] { 0, 0, 2, 0, 0, 1}, uids.get(0));
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void resolveOrCreateTagkNotAllowedBlocked() throws Exception {
    setupStorage();
    config.overrideConfig("tsd.core.auto_create_tagks", "false");
    setupResolveAll();
    
    final Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("nonesuch", "web01");
    Tags.resolveOrCreateAll(tsdb, tags);
  }
  
  @Test
  public void resolveOrCreateTagvAllowed() throws Exception {
    setupStorage();
    setupResolveAll();
    
    final Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "nohost");
    final List<byte[]> uids = Tags.resolveOrCreateAll(tsdb, tags);
    assertEquals(1, uids.size());
    assertArrayEquals(new byte[] { 0, 0, 1, 0, 0, 3}, uids.get(0));
  }
  
  @Test
  public void resolveOrCreateTagvNotAllowedGood() throws Exception {
    setupStorage();
    config.overrideConfig("tsd.core.auto_create_tagvs", "false");
    setupResolveAll();
    
    final Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web02");
    final List<byte[]> uids = Tags.resolveOrCreateAll(tsdb, tags);
    assertEquals(1, uids.size());
    assertArrayEquals(new byte[] { 0, 0, 1, 0, 0, 2}, uids.get(0));
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void resolveOrCreateTagvNotAllowedBlocked() throws Exception {
    setupStorage();
    config.overrideConfig("tsd.core.auto_create_tagvs", "false");
    setupResolveAll();
    
    final Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "invalidhost");
    Tags.resolveOrCreateAll(tsdb, tags);
  }
  
  @Test
  public void resolveOrCreateAllAsync() throws Exception {
    setupStorage();
    setupResolveAll();
    
    final Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "nohost");
    final List<byte[]> uids = Tags.resolveOrCreateAllAsync(tsdb, "metric", tags).join();
    assertEquals(1, uids.size());
    assertArrayEquals(new byte[] { 0, 0, 1, 0, 0, 3}, uids.get(0));
  }
  
  @Test (expected = DeferredGroupException.class)
  public void resolveOrCreateAllAsyncFilterBlocked() throws Exception {
    setupStorage();
    setupResolveAll();
    when(tag_names.getOrCreateIdAsync(eq("host"), anyString(), 
        anyMapOf(String.class, String.class)))
      .thenReturn(Deferred.<byte[]>fromError(new FailedToAssignUniqueIdException(
          "tagk", "host", 0, "Blocked by UID filter.")));
    
    final Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "nohost");
    Tags.resolveOrCreateAllAsync(tsdb, "metric", tags).join();
  }
  
  // PRIVATE helpers to setup unit tests
  
  private void setupStorage() throws Exception {
    config = new Config(false);
    client = mock(HBaseClient.class);
    tsdb = new TSDB(config);
    storage = new MockBase(tsdb, client, true, true, true, true);

    // replace the "real" field objects with mocks
    Field cl = tsdb.getClass().getDeclaredField("client");
    cl.setAccessible(true);
    cl.set(tsdb, client);
    
    Field met = tsdb.getClass().getDeclaredField("metrics");
    met.setAccessible(true);
    met.set(tsdb, metrics);
    
    Field tagk = tsdb.getClass().getDeclaredField("tag_names");
    tagk.setAccessible(true);
    tagk.set(tsdb, tag_names);
    
    Field tagv = tsdb.getClass().getDeclaredField("tag_values");
    tagv.setAccessible(true);
    tagv.set(tsdb, tag_values);
    
    when(metrics.width()).thenReturn((short)3);
    when(tag_names.width()).thenReturn((short)3);
    when(tag_values.width()).thenReturn((short)3);
  }
  
  private void setupResolveIds() throws Exception {

    when(tag_names.getNameAsync(new byte[] { 0, 0, 1 }))
      .thenReturn(Deferred.fromResult("host"));
    when(tag_names.getNameAsync(new byte[] { 0, 0, 2 }))
      .thenThrow(new NoSuchUniqueId("tagk", new byte[] { 0, 0, 2 }));
    when(tag_values.getNameAsync(new byte[] { 0, 0, 1 }))
      .thenReturn(Deferred.fromResult("web01"));
    when(tag_values.getNameAsync(new byte[] { 0, 0, 2 }))
        .thenThrow(new NoSuchUniqueId("tagv", new byte[] { 0, 0, 2 }));
  }
  
  private void setupResolveAll() throws Exception {
    when(tag_names.getOrCreateId(eq("host")))
      .thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getOrCreateIdAsync(eq("host"), anyString(), 
        anyMapOf(String.class, String.class)))
      .thenReturn(Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(tag_names.getOrCreateId(eq("doesnotexist")))
      .thenReturn(new byte[] { 0, 0, 3 });
    when(tag_names.getOrCreateIdAsync(eq("doesnotexist"), anyString(), 
        anyMapOf(String.class, String.class)))
      .thenReturn(Deferred.fromResult(new byte[] { 0, 0, 3 }));
    when(tag_names.getId("pop")).thenReturn(new byte[] { 0, 0, 2 });
    when(tag_names.getId("nonesuch"))
      .thenThrow(new NoSuchUniqueName("tagv", "nonesuch"));
    
    when(tag_values.getOrCreateId(eq("web01")))
      .thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getOrCreateIdAsync(eq("web01"), anyString(), 
        anyMapOf(String.class, String.class)))
      .thenReturn(Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(tag_values.getOrCreateId(eq("nohost")))
    .thenReturn(new byte[] { 0, 0, 3 });
    when(tag_values.getOrCreateIdAsync(eq("nohost"), anyString(), 
        anyMapOf(String.class, String.class)))
      .thenReturn(Deferred.fromResult(new byte[] { 0, 0, 3 }));
    when(tag_values.getId("web02")).thenReturn(new byte[] { 0, 0, 2 });
    when(tag_values.getId("invalidhost"))
      .thenThrow(new NoSuchUniqueName("tagk", "invalidhost"));
  }

  @Test
  public void getTagUids() throws Exception {
    byte[] row = new byte[] { 0, 0, 1,  0x50, (byte)0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 2};
    ByteMap<byte[]> uids = Tags.getTagUids(row);
    assertEquals(1, uids.size());
    assertEquals(0, Bytes.memcmp(new byte[] { 0, 0, 1 }, uids.firstKey()));
    assertEquals(0, Bytes.memcmp(new byte[] { 0, 0, 2 }, uids.firstEntry()
        .getValue()));
    
    row = new byte[] { 0, 0, 1,  0x50, (byte)0xE2, 0x27, 0, 
       0, 0, 1, 0, 0, 2, 0, 0, 3, 0, 0, 4};
    uids = Tags.getTagUids(row);
    assertEquals(2, uids.size());
    assertEquals(0, Bytes.memcmp(new byte[] { 0, 0, 1 }, uids.firstKey()));
    assertEquals(0, Bytes.memcmp(new byte[] { 0, 0, 2 }, uids.firstEntry()
        .getValue()));
    assertEquals(0, Bytes.memcmp(new byte[] { 0, 0, 3 }, uids.lastKey()));
    assertEquals(0, Bytes.memcmp(new byte[] { 0, 0, 4 }, uids.lastEntry()
        .getValue()));
    
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    
    row = new byte[] { 1, 0, 0, 1,  0x50, (byte)0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 2};
    uids = Tags.getTagUids(row);
    assertEquals(1, uids.size());
    assertEquals(0, Bytes.memcmp(new byte[] { 0, 0, 1 }, uids.firstKey()));
    assertEquals(0, Bytes.memcmp(new byte[] { 0, 0, 2 }, uids.firstEntry()
        .getValue()));
    
    row = new byte[] { 1, 0, 0, 1,  0x50, (byte)0xE2, 0x27, 0, 
       0, 0, 1, 0, 0, 2, 0, 0, 3, 0, 0, 4};
    uids = Tags.getTagUids(row);
    assertEquals(2, uids.size());
    assertEquals(0, Bytes.memcmp(new byte[] { 0, 0, 1 }, uids.firstKey()));
    assertEquals(0, Bytes.memcmp(new byte[] { 0, 0, 2 }, uids.firstEntry()
        .getValue()));
    assertEquals(0, Bytes.memcmp(new byte[] { 0, 0, 3 }, uids.lastKey()));
    assertEquals(0, Bytes.memcmp(new byte[] { 0, 0, 4 }, uids.lastEntry()
        .getValue()));
  }
  
  @Test (expected = ArrayIndexOutOfBoundsException.class)
  public void getTagUidsMissingTagV() throws Exception {
    byte[] row = new byte[] { 0, 0, 1,  0x50, (byte)0xE2, 0x27, 0, 0, 0, 1 };
    Tags.getTagUids(row);
  }
  
  @Test (expected = ArrayIndexOutOfBoundsException.class)
  public void getTagUidsMissingTagVSalted() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    
    byte[] row = new byte[] { 1, 0, 0, 1,  0x50, (byte)0xE2, 0x27, 0, 0, 0, 1 };
    Tags.getTagUids(row);
  }
  
  @Test
  public void getTagUidsMissingTags() throws Exception {
    byte[] row = new byte[] { 0, 0, 1,  0x50, (byte)0xE2, 0x27, 0 };
    ByteMap<byte[]> uids = Tags.getTagUids(row);
    assertEquals(0, uids.size());
    
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    
    row = new byte[] { 1, 0, 0, 1,  0x50, (byte)0xE2, 0x27, 0 };
    uids = Tags.getTagUids(row);
    assertEquals(0, uids.size());
  }
  
  @Test (expected = NullPointerException.class)
  public void getTagUidsNullRow() throws Exception {
    Tags.getTagUids(null);
  }
  
  @Test
  public void getTagUidsEmptyRow() throws Exception {
    final ByteMap<byte[]> uids = Tags.getTagUids(new byte[] {});
    assertEquals(0, uids.size());
  }

  @Test
  public void setAllowSpecialChars() throws Exception {
    assertFalse(Tags.isAllowSpecialChars('!'));

    Tags.setAllowSpecialChars(null);
    assertFalse(Tags.isAllowSpecialChars('!'));

    Tags.setAllowSpecialChars("");
    assertFalse(Tags.isAllowSpecialChars('!'));

    Tags.setAllowSpecialChars("!)(%");
    assertTrue(Tags.isAllowSpecialChars('!'));
    assertTrue(Tags.isAllowSpecialChars('('));
    assertTrue(Tags.isAllowSpecialChars('%'));
  }
}
