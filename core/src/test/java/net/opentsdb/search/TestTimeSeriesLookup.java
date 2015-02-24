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
package net.opentsdb.search;

import java.util.ArrayList;
import java.util.List;

import dagger.ObjectGraph;
import net.opentsdb.TestModuleMemoryStore;
import net.opentsdb.core.TSDB;
import net.opentsdb.storage.HBaseConst;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.utils.Pair;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.uid.UniqueIdType;

import javax.inject.Inject;

import static org.junit.Assert.*;

public class TestTimeSeriesLookup {
  @Inject TSDB tsdb;
  @Inject MemoryStore tsdb_store;
  
  // tsuids
  private static List<byte[]> test_tsuids = new ArrayList<byte[]>(7);
  static {
    test_tsuids.add(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 });
    test_tsuids.add(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 2 });
    test_tsuids.add(new byte[] { 0, 0, 2, 0, 0, 1, 0, 0, 1 });
    test_tsuids.add(new byte[] { 0, 0, 3, 0, 0, 1, 0, 0, 1, 0, 0, 4, 0, 0, 5});
    test_tsuids.add(new byte[] { 0, 0, 3, 0, 0, 1, 0, 0, 2, 0, 0, 4, 0, 0, 5});
    test_tsuids.add(new byte[] { 0, 0, 3, 0, 0, 6, 0, 0, 7, 0, 0, 8, 0, 0, 1, 
        0, 0, 9, 0, 0, 3});
    test_tsuids.add(new byte[] { 0, 0, 3, 0, 0, 6, 0, 0, 7, 0, 0, 8, 0, 0, 10, 
        0, 0, 9, 0, 0, 3});
  }
  
  @Before
  public void before() throws Exception {
    ObjectGraph.create(new TestModuleMemoryStore()).inject(this);

    tsdb_store.allocateUID("sys.cpu.user", new byte[] {0, 0, 1}, UniqueIdType.METRIC);
    tsdb_store.allocateUID("sys.cpu.nice", new byte[] {0, 0, 2}, UniqueIdType.METRIC);
    tsdb_store.allocateUID("sys.cpu.idle", new byte[] {0, 0, 3}, UniqueIdType.METRIC);
    tsdb_store.allocateUID("no.values", new byte[] {0, 0, 11}, UniqueIdType.METRIC);

    tsdb_store.allocateUID("host", new byte[] {0, 0, 1}, UniqueIdType.TAGK);
    tsdb_store.allocateUID("owner", new byte[] {0, 0, 4}, UniqueIdType.TAGK);

    tsdb_store.allocateUID("web01", new byte[] {0, 0, 1}, UniqueIdType.TAGV);
    tsdb_store.allocateUID("web02", new byte[] {0, 0, 2}, UniqueIdType.TAGV);
  }

  @Test
  public void metricOnlyMeta() throws Exception {
    generateMeta();
    final SearchQuery query = new SearchQuery("sys.cpu.user");
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(2, tsuids.size());
    assertArrayEquals(test_tsuids.get(0), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(1), tsuids.get(1));
  }
  
  // returns everything
  @Test
  public void metricOnlyMetaStar() throws Exception {
    generateMeta();
    final SearchQuery query = new SearchQuery("*");
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(7, tsuids.size());
  }
  
  @Test
  public void metricOnly2Meta() throws Exception {
    generateMeta();
    final SearchQuery query = new SearchQuery("sys.cpu.nice");
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(2), tsuids.get(0));
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void noSuchMetricMeta() throws Exception {
    final SearchQuery query = new SearchQuery("sys.cpu.system");
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    lookup.lookup();
  }
  
  @Test
  public void metricOnlyNoValuesMeta() throws Exception {
    generateMeta();
    final SearchQuery query = new SearchQuery("no.values");
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(0, tsuids.size());
  }
  
  @Test
  public void tagkOnlyMeta() throws Exception {
    generateMeta();
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>("host", null));
    final SearchQuery query = new SearchQuery(tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(5, tsuids.size());
    for (int i = 0; i < 5; i++) {
      assertArrayEquals(test_tsuids.get(i), tsuids.get(i));
    }
  }
  
  @Test
  public void tagkOnlyMetaStar() throws Exception {
    generateMeta();
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>("host", "*"));
    final SearchQuery query = new SearchQuery(tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(5, tsuids.size());
    for (int i = 0; i < 5; i++) {
      assertArrayEquals(test_tsuids.get(i), tsuids.get(i));
    }
  }
  
  @Test
  public void tagkOnly2Meta() throws Exception {
    generateMeta();
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>("owner", null));
    final SearchQuery query = new SearchQuery(tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(2, tsuids.size());
    assertArrayEquals(test_tsuids.get(3), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(4), tsuids.get(1));
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void noSuchTagkMeta() throws Exception {
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>("dc", null));
    final SearchQuery query = new SearchQuery(tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    lookup.lookup();
  }
  
  @Test
  public void tagvOnlyMeta() throws Exception {
    generateMeta();
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(null, "web01"));
    final SearchQuery query = new SearchQuery(tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(4, tsuids.size());
    assertArrayEquals(test_tsuids.get(0), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(2), tsuids.get(1));
    assertArrayEquals(test_tsuids.get(3), tsuids.get(2));
    assertArrayEquals(test_tsuids.get(5), tsuids.get(3));
  }
  
  @Test
  public void tagvOnlyMetaStar() throws Exception {
    generateMeta();
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>("*", "web01"));
    final SearchQuery query = new SearchQuery(tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(4, tsuids.size());
    assertArrayEquals(test_tsuids.get(0), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(2), tsuids.get(1));
    assertArrayEquals(test_tsuids.get(3), tsuids.get(2));
    assertArrayEquals(test_tsuids.get(5), tsuids.get(3));
  }
  
  @Test
  public void tagvOnly2Meta() throws Exception {
    generateMeta();
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(null, "web02"));
    final SearchQuery query = new SearchQuery(tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(2, tsuids.size());
    assertArrayEquals(test_tsuids.get(1), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(4), tsuids.get(1));
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void noSuchTagvMeta() throws Exception {
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(null, "web03"));
    final SearchQuery query = new SearchQuery(tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    lookup.lookup();
  }
  
  @Test
  public void metricAndTagkMeta() throws Exception {
    generateMeta();
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>("host", null));
    final SearchQuery query = new SearchQuery("sys.cpu.nice", 
        tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(2), tsuids.get(0));
  }
  
  @Test
  public void metricAndTagkMetaStar() throws Exception {
    generateMeta();
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>("host", "*"));
    final SearchQuery query = new SearchQuery("sys.cpu.nice", 
        tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(2), tsuids.get(0));
  }
  
  @Test
  public void metricAndTagvMeta() throws Exception {
    generateMeta();
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(null, "web02"));
    final SearchQuery query = new SearchQuery("sys.cpu.idle",
        tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(4), tsuids.get(0));
  }
  
  @Test
  public void metricAndTagvMetaStar() throws Exception {
    generateMeta();
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>("*", "web02"));
    final SearchQuery query = new SearchQuery("sys.cpu.idle",
        tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(4), tsuids.get(0));
  }
  
  @Test
  public void metricAndTagPairMeta() throws Exception {
    generateMeta();
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>("host", "web01"));
    final SearchQuery query = new SearchQuery("sys.cpu.idle", 
        tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(3), tsuids.get(0));
  }
  
  @Test
  public void tagPairOnlyMeta() throws Exception {
    generateMeta();
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>("host", "web01"));
    final SearchQuery query = new SearchQuery(tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(3, tsuids.size());
    assertArrayEquals(test_tsuids.get(0), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(2), tsuids.get(1));
    assertArrayEquals(test_tsuids.get(3), tsuids.get(2));
  }
  
  /**
   * Stores some data in the mock tsdb-meta table for unit testing
   */
  private void generateMeta() {
    final byte[] val = new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 };
    for (final byte[] tsuid : test_tsuids) {
      tsdb_store.addColumn(tsuid, HBaseConst.TSMeta.COUNTER_QUALIFIER, val);
    }
  }
}
