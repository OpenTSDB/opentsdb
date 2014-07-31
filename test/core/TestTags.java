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
import java.util.Map;

import net.opentsdb.storage.MemoryStore;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.Pair;

import org.junit.Test;

import static net.opentsdb.uid.UniqueId.UniqueIdType;
import static org.junit.Assert.*;

public final class TestTags {
  private TSDB tsdb;
  private Config config;
  private MemoryStore tsdb_store;

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
    final List<byte[]> uids = Tags.resolveOrCreateAllAsync(tsdb, tags).joinUninterruptibly();
    assertEquals(1, uids.size());
    assertArrayEquals(new byte[] { 0, 0, 1, 0, 0, 1}, uids.get(0));
  }
  
  @Test
  public void resolveOrCreateTagkAllowed() throws Exception {
    setupStorage();
    setupResolveAll();
    
    final Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("doesnotexist", "web01");
    final List<byte[]> uids = Tags.resolveOrCreateAllAsync(tsdb, tags).joinUninterruptibly();
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
    final List<byte[]> uids = Tags.resolveOrCreateAllAsync(tsdb, tags).joinUninterruptibly();
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
    Tags.resolveOrCreateAllAsync(tsdb, tags).joinUninterruptibly();
  }
  
  @Test
  public void resolveOrCreateTagvAllowed() throws Exception {
    setupStorage();
    setupResolveAll();
    
    final Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "nohost");
    final List<byte[]> uids = Tags.resolveOrCreateAllAsync(tsdb, tags).joinUninterruptibly();
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
    final List<byte[]> uids = Tags.resolveOrCreateAllAsync(tsdb, tags).joinUninterruptibly();
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
    Tags.resolveOrCreateAllAsync(tsdb, tags).joinUninterruptibly();
  }
  
  // PRIVATE helpers to setup unit tests
  
  private void setupStorage() throws Exception {
    config = new Config(false);
    tsdb_store = new MemoryStore();
    tsdb = new TSDB(tsdb_store, config);
  }
  
  private void setupResolveIds() throws Exception {
    tsdb_store.allocateUID("host", new byte[]{0, 0, 1}, UniqueIdType.TAGK);
    tsdb_store.allocateUID("web01", new byte[]{0, 0, 1}, UniqueIdType.TAGV);
  }
  
  private void setupResolveAll() throws Exception {
    tsdb_store.allocateUID("host", new byte[]{0, 0, 1}, UniqueIdType.TAGK);
    tsdb_store.allocateUID("pop", new byte[]{0, 0, 2}, UniqueIdType.TAGK);
    tsdb_store.allocateUID("doesnotexist", new byte[]{0, 0, 3}, UniqueIdType.TAGK);

    tsdb_store.allocateUID("web01", new byte[]{0, 0, 1}, UniqueIdType.TAGV);
    tsdb_store.allocateUID("web02", new byte[]{0, 0, 2}, UniqueIdType.TAGV);
    tsdb_store.allocateUID("nohost", new byte[]{0, 0, 3}, UniqueIdType.TAGV);
  }
}
