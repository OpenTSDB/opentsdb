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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.opentsdb.core.BaseTsdbTest;
import net.opentsdb.core.Const;
import net.opentsdb.core.RowKey;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.Pair;

import org.hbase.async.Bytes;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class, 
  KeyValue.class, Scanner.class, TimeSeriesLookup.class})
public class TestTimeSeriesLookup extends BaseTsdbTest {
  
  // tsuids
  public static List<byte[]> test_tsuids = new ArrayList<byte[]>(7);
  static {
    test_tsuids.add(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 });
    test_tsuids.add(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 2 });
    test_tsuids.add(new byte[] { 0, 0, 2, 0, 0, 1, 0, 0, 1 });
    test_tsuids.add(new byte[] { 0, 0, 4, 0, 0, 1, 0, 0, 1, 0, 0, 3, 0, 0, 5});
    test_tsuids.add(new byte[] { 0, 0, 4, 0, 0, 1, 0, 0, 2, 0, 0, 3, 0, 0, 5});
    test_tsuids.add(new byte[] { 0, 0, 4, 0, 0, 6, 0, 0, 7, 0, 0, 8, 0, 0, 1, 
        0, 0, 9, 0, 0, 3});
    test_tsuids.add(new byte[] { 0, 0, 4, 0, 0, 6, 0, 0, 7, 0, 0, 8, 0, 0, 10, 
        0, 0, 9, 0, 0, 3});
  }
  
  @Before
  public void beforeLocal() {
    storage = new MockBase(tsdb, client, true, true, true, true);
    when(metrics.getIdAsync("no.values"))
      .thenReturn(Deferred.fromResult(new byte[] { 0, 0, 11 }));
    when(metrics.getIdAsync("filtered"))
      .thenReturn(Deferred.fromResult(new byte[] { 0, 0, 4 }));
  }
  
  @Test
  public void metricOnlyMeta() throws Exception {
    generateMeta(tsdb, storage);
    final SearchQuery query = new SearchQuery(METRIC_STRING);
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
    generateMeta(tsdb, storage);
    final SearchQuery query = new SearchQuery("*");
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(7, tsuids.size());
  }
  
  @Test
  public void metricOnlyData() throws Exception {
    generateData(tsdb, storage);
    final SearchQuery query = new SearchQuery(METRIC_STRING);
    query.setUseMeta(false);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(2, tsuids.size());
    assertArrayEquals(test_tsuids.get(0), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(1), tsuids.get(1));
  }
  
  @Test
  public void metricOnly2Meta() throws Exception {
    generateMeta(tsdb, storage);
    final SearchQuery query = new SearchQuery(METRIC_B_STRING);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(2), tsuids.get(0));
  }
  
  @Test
  public void metricOnly2Data() throws Exception {
    generateData(tsdb, storage);
    final SearchQuery query = new SearchQuery(METRIC_B_STRING);
    query.setUseMeta(false);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);    
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(2), tsuids.get(0));
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void noSuchMetricMeta() throws Exception {
    final SearchQuery query = new SearchQuery(NSUN_METRIC);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    lookup.lookup();
  }
  
  @Test
  public void metricOnlyNoValuesMeta() throws Exception {
    generateMeta(tsdb, storage);
    final SearchQuery query = new SearchQuery("no.values");
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(0, tsuids.size());
  }
  
  @Test
  public void metricOnlyNoValuesData() throws Exception {
    generateData(tsdb, storage);
    final SearchQuery query = new SearchQuery("no.values");
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    query.setUseMeta(false);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(0, tsuids.size());
  }
  
  @Test
  public void tagkOnlyMeta() throws Exception {
    generateMeta(tsdb, storage);
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(TAGK_STRING, null));
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
    generateMeta(tsdb, storage);
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(TAGK_STRING, "*"));
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
  public void tagkOnlyData() throws Exception {
    generateData(tsdb, storage);
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(TAGK_STRING, null));
    final SearchQuery query = new SearchQuery(tags);
    query.setUseMeta(false);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    Collections.sort(tsuids, Bytes.MEMCMP); // for salting
    assertNotNull(tsuids);
    assertEquals(5, tsuids.size());
    for (int i = 0; i < 5; i++) {
      assertArrayEquals(test_tsuids.get(i), tsuids.get(i));
    }
  }
  
  @Test
  public void tagkOnly2Meta() throws Exception {
    generateMeta(tsdb, storage);
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(TAGK_B_STRING, null));
    final SearchQuery query = new SearchQuery(tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(2, tsuids.size());
    assertArrayEquals(test_tsuids.get(3), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(4), tsuids.get(1));
  }
  
  @Test
  public void tagkOnly2Data() throws Exception {
    generateData(tsdb, storage);
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(TAGK_B_STRING, null));
    final SearchQuery query = new SearchQuery(tags);
    query.setUseMeta(false);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    Collections.sort(tsuids, Bytes.MEMCMP); // for salting
    assertNotNull(tsuids);
    assertEquals(2, tsuids.size());
    assertArrayEquals(test_tsuids.get(3), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(4), tsuids.get(1));
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void noSuchTagkMeta() throws Exception {
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(NSUN_TAGK, null));
    final SearchQuery query = new SearchQuery(tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    lookup.lookup();
  }
  
  @Test
  public void tagvOnlyMeta() throws Exception {
    generateMeta(tsdb, storage);
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(null, TAGV_STRING));
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
    generateMeta(tsdb, storage);
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>("*", TAGV_STRING));
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
  public void tagvOnlyData() throws Exception {
    generateData(tsdb, storage);
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(null, TAGV_STRING));
    final SearchQuery query = new SearchQuery(tags);
    query.setUseMeta(false);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    Collections.sort(tsuids, Bytes.MEMCMP); // for salting
    assertNotNull(tsuids);
    assertEquals(4, tsuids.size());
    assertArrayEquals(test_tsuids.get(0), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(2), tsuids.get(1));
    assertArrayEquals(test_tsuids.get(3), tsuids.get(2));
    assertArrayEquals(test_tsuids.get(5), tsuids.get(3));
  }
  
  @Test
  public void tagvOnly2Meta() throws Exception {
    generateMeta(tsdb, storage);
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(null, TAGV_B_STRING));
    final SearchQuery query = new SearchQuery(tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(2, tsuids.size());
    assertArrayEquals(test_tsuids.get(1), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(4), tsuids.get(1));
  }
  
  @Test
  public void tagvOnly2Data() throws Exception {
    generateData(tsdb, storage);
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(null, TAGV_B_STRING));
    final SearchQuery query = new SearchQuery(tags);
    query.setUseMeta(false);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    Collections.sort(tsuids, Bytes.MEMCMP); // for salting
    assertNotNull(tsuids);
    assertEquals(2, tsuids.size());
    assertArrayEquals(test_tsuids.get(1), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(4), tsuids.get(1));
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void noSuchTagvMeta() throws Exception {
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(null, NSUN_TAGV));
    final SearchQuery query = new SearchQuery(tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    lookup.lookup();
  }
  
  @Test
  public void metricAndTagkMeta() throws Exception {
    generateMeta(tsdb, storage);
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(TAGK_STRING, null));
    final SearchQuery query = new SearchQuery(METRIC_B_STRING, 
        tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(2), tsuids.get(0));
  }
  
  @Test
  public void metricAndTagkMetaStar() throws Exception {
    generateMeta(tsdb, storage);
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(TAGK_STRING, "*"));
    final SearchQuery query = new SearchQuery(METRIC_B_STRING, 
        tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(2), tsuids.get(0));
  }
  
  @Test
  public void metricAndTagkData() throws Exception {
    generateData(tsdb, storage);
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(TAGK_STRING, null));
    final SearchQuery query = new SearchQuery(METRIC_B_STRING, 
        tags);
    query.setUseMeta(false);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(2), tsuids.get(0));
  }
  
  @Test
  public void metricAndTagvMeta() throws Exception {
    generateMeta(tsdb, storage);
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(null, TAGV_B_STRING));
    final SearchQuery query = new SearchQuery("filtered", tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(4), tsuids.get(0));
  }
  
  @Test
  public void metricAndTagvMetaStar() throws Exception {
    generateMeta(tsdb, storage);
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>("*", TAGV_B_STRING));
    final SearchQuery query = new SearchQuery("filtered",tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(4), tsuids.get(0));
  }
  
  @Test
  public void metricAndTagvData() throws Exception {
    generateData(tsdb, storage);
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(null, TAGV_B_STRING));
    final SearchQuery query = new SearchQuery("filtered", tags);
    query.setUseMeta(false);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(4), tsuids.get(0));
  }
  
  @Test
  public void metricAndTagPairMeta() throws Exception {
    generateMeta(tsdb, storage);
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(TAGK_STRING, TAGV_STRING));
    final SearchQuery query = new SearchQuery("filtered", tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(3), tsuids.get(0));
  }
  
  @Test
  public void metricAndTagPairData() throws Exception {
    generateData(tsdb, storage);
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(TAGK_STRING, TAGV_STRING));
    final SearchQuery query = new SearchQuery("filtered", tags);
    query.setUseMeta(false);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    query.setUseMeta(false);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(3), tsuids.get(0));
  }
  
  @Test
  public void tagPairOnlyMeta() throws Exception {
    generateMeta(tsdb, storage);
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(TAGK_STRING, TAGV_STRING));
    final SearchQuery query = new SearchQuery(tags);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(3, tsuids.size());
    assertArrayEquals(test_tsuids.get(0), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(2), tsuids.get(1));
    assertArrayEquals(test_tsuids.get(3), tsuids.get(2));
  }
  
  @Test
  public void tagPairOnlyData() throws Exception {
    generateData(tsdb, storage);
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(TAGK_STRING, TAGV_STRING));
    final SearchQuery query = new SearchQuery(tags);
    query.setUseMeta(false);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    Collections.sort(tsuids, Bytes.MEMCMP); // for salting
    assertNotNull(tsuids);
    assertEquals(3, tsuids.size());
    assertArrayEquals(test_tsuids.get(0), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(2), tsuids.get(1));
    assertArrayEquals(test_tsuids.get(3), tsuids.get(2));
  }
  
  @Test
  public void limitVerification() throws Exception {
    generateData(tsdb, storage);
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(TAGK_STRING, TAGV_STRING));
    final SearchQuery query = new SearchQuery(tags);
    query.setUseMeta(false);
    query.setLimit(1);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    final List<byte[]> tsuids = lookup.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(0), tsuids.get(0));
  }
  
  @Test (expected = RuntimeException.class)
  public void scannerException() throws Exception {
    generateData(tsdb, storage);
    final byte[] row = Const.SALT_WIDTH() > 0 ?
        MockBase.stringToBytes(
            "0300000400000000000001000001000003000005") :
        MockBase.stringToBytes(
            "00000400000000000001000001000003000005");
    storage.throwException(row, new RuntimeException("Boo!"));
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(1);
    tags.add(new Pair<String, String>(TAGK_STRING, TAGV_STRING));
    final SearchQuery query = new SearchQuery(tags);
    query.setUseMeta(false);
    query.setLimit(1);
    final TimeSeriesLookup lookup = new TimeSeriesLookup(tsdb, query);
    lookup.lookup();
  }
  
  // TODO test the dump to stdout
  
  /**
   * Stores some data in the mock tsdb-meta table for unit testing
   */
  public static void generateMeta(final TSDB tsdb, final MockBase storage) {
    final List<byte[]> families = new ArrayList<byte[]>(1);
    families.add(TSMeta.FAMILY);
    storage.addTable("tsdb-meta".getBytes(), families);
    
    final byte[] val = new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 };
    for (final byte[] tsuid : test_tsuids) {
      storage.addColumn("tsdb-meta".getBytes(), tsuid, TSMeta.FAMILY, 
          TSMeta.COUNTER_QUALIFIER(), val);
    }
  }
  
  /**
   * Stores some data in the mock tsdb data table for unit testing
   */
  public static void generateData(final TSDB tsdb, final MockBase storage) {
    storage.setFamily("t".getBytes(MockBase.ASCII()));
    
    final byte[] qual = new byte[] { 0, 0 };
    final byte[] val = new byte[] { 1 };
    for (final byte[] tsuid : test_tsuids) {
      byte[] row_key = new byte[Const.SALT_WIDTH() + tsuid.length + 
                                Const.TIMESTAMP_BYTES];
      System.arraycopy(tsuid, 0, row_key, Const.SALT_WIDTH(), 
          TSDB.metrics_width());
      System.arraycopy(tsuid, TSDB.metrics_width(), row_key, 
          Const.SALT_WIDTH() + TSDB.metrics_width() + Const.TIMESTAMP_BYTES, 
          tsuid.length - TSDB.metrics_width());
      RowKey.prefixKeyWithSalt(row_key);
      storage.addColumn(row_key, qual, val);
    }
  }
}
