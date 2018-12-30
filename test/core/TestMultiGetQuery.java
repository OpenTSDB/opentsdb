// This file is part of OpenTSDB.
// Copyright (C) 2016-2017  The OpenTSDB Authors.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.hbase.async.Bytes.ByteMap;
import org.hbase.async.GetRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.Lists;

import net.opentsdb.core.MultiGetQuery.MultiGetTask;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupQuery;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.utils.ByteSet;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*", "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*", "javax.net.ssl.*"})
public class TestMultiGetQuery extends BaseTsdbTest {
  protected List<ByteMap<byte[][]>> q_tags;
  protected List<ByteMap<byte[][]>> q_tags_nometa;
  protected List<ByteMap<byte[][]>> q_tags_AD;

  protected long start_ts;
  protected long end_ts;
  protected TreeMap<byte[], Span> spans;
  protected TreeMap<byte[], HistogramSpan> histogramSpans;
  protected QueryStats query_stats;
  protected Aggregator aggregator;
  protected int max_bytes;
  protected boolean multiget_no_meta;
  protected TsdbQuery query;
  
  @Before
  public void localBefore() {
    max_bytes = 1000000;
    multiget_no_meta = false;
    start_ts = 1481227200;
    end_ts = 1481284800;
    q_tags = new ArrayList<ByteMap<byte[][]>>();
    q_tags_AD = new ArrayList<ByteMap<byte[][]>>();
    ByteMap<byte[][]> q_tags1;
    q_tags1 = new ByteMap<byte[][]>();
    byte[][] val;
    val = new byte[1][];
    val[0] = UIDS.get("A");
    
    q_tags1.put(TAGK_BYTES, val);
    val = new byte[1][];
    val[0] = UIDS.get("D");
    q_tags1.put(TAGK_B_BYTES, val);
    ByteMap<byte[][]> q_tags2;
    q_tags2 = new ByteMap<byte[][]>();
    val = new byte[1][];
    val[0] = UIDS.get("B");
    q_tags2.put(TAGK_BYTES, val);
    val = new byte[1][];
    val[0] = UIDS.get("D");
    q_tags2.put(TAGK_B_BYTES, val);
    ByteMap<byte[][]> q_tags3;
    q_tags3 = new ByteMap<byte[][]>();
    val = new byte[1][];
    val[0] = UIDS.get("C");
    q_tags3.put(TAGK_BYTES, val);
    val = new byte[1][];
    val[0] = UIDS.get("D");
    q_tags3.put(TAGK_B_BYTES, val);
    
    q_tags.add(q_tags1);
    q_tags.add(q_tags2);
    q_tags.add(q_tags3);
    
    q_tags_AD.add(q_tags1);
    
    q_tags_nometa = new ArrayList<ByteMap<byte[][]>>();
    ByteMap<byte[][]> q_tags_map = new ByteMap<byte[][]>();
    q_tags_map.put(TAGK_BYTES, new byte[][] { UIDS.get("A"), UIDS.get("B"), UIDS.get("C") });
    q_tags_map.put(TAGK_B_BYTES, new byte[][] { UIDS.get("D") });
    q_tags_map.put(UIDS.get("E"), new byte[][] { UIDS.get("F"), UIDS.get("G") });
    q_tags_nometa.add(q_tags_map);
//    q_tags1.put(TAGK_BYTES, new byte[][] { UIDS.get("A"), UIDS.get("B"), UIDS.get("C") });
//    q_tags1.put(TAGK_B_BYTES, new byte[][] { UIDS.get("D") });
//    q_tags1.put(UIDS.get("E"), new byte[][] { UIDS.get("F"), UIDS.get("G") });
    
    aggregator = Aggregators.get("sum");
    
    RollupConfig rollup_config = RollupConfig.builder()
        .addAggregationId("sum", 0)
        .addAggregationId("count", 1)
        .addAggregationId("max", 2)
        .addAggregationId("min", 3)
        .addInterval(RollupInterval.builder()
            .setTable("tsdb-rollup-10m")
            .setPreAggregationTable("tsdb-rollup-agg-10m")
            .setInterval("10m")
            .setRowSpan("6h"))
        .addInterval(RollupInterval.builder()
            .setTable("tsdb-rollup-1h")
            .setPreAggregationTable("tsdb-rollup-agg-1h")
            .setInterval("1h")
            .setRowSpan("1d"))
        .addInterval(RollupInterval.builder()
            .setTable("tsdb-rollup-1d")
            .setPreAggregationTable("tsdb-rollup-agg-1d")
            .setInterval("1d")
            .setRowSpan("1n"))
        .build();
    Whitebox.setInternalState(tsdb, "rollup_config", rollup_config);
  }
  
  @Test
  public void ctor() throws Exception {
    TsdbQuery query = new TsdbQuery(tsdb);
    final MultiGetQuery mgq = new MultiGetQuery(tsdb, query, METRIC_BYTES, q_tags, 
        start_ts, end_ts, tsdb.dataTable(), spans, null, 0, null, query_stats, 
        0, max_bytes, false, multiget_no_meta);
    // TODO - validations
  }
  
  @Test
  public void prepareAllTagvCompounds() throws Exception {
    multiget_no_meta = true;
    MultiGetQuery mgq = new MultiGetQuery(tsdb, query, METRIC_BYTES, q_tags_nometa, 
        start_ts, end_ts, tsdb.dataTable(), spans, null, 0, null, query_stats, 
        0, 0, false, multiget_no_meta);
    
    List<byte[][]> tagvs = mgq.prepareAllTagvCompounds();
    assertEquals(6, tagvs.size());
    assertEquals(3, tagvs.get(0).length);
    assertArrayEquals(UIDS.get("A"), tagvs.get(0)[0]);
    assertArrayEquals(UIDS.get("D"), tagvs.get(0)[1]);
    assertArrayEquals(UIDS.get("F"), tagvs.get(0)[2]);
    
    assertEquals(3, tagvs.get(1).length);
    assertArrayEquals(UIDS.get("B"), tagvs.get(1)[0]);
    assertArrayEquals(UIDS.get("D"), tagvs.get(1)[1]);
    assertArrayEquals(UIDS.get("F"), tagvs.get(1)[2]);
    
    assertEquals(3, tagvs.get(2).length);
    assertArrayEquals(UIDS.get("C"), tagvs.get(2)[0]);
    assertArrayEquals(UIDS.get("D"), tagvs.get(2)[1]);
    assertArrayEquals(UIDS.get("F"), tagvs.get(2)[2]);
    
    assertEquals(3, tagvs.get(3).length);
    assertArrayEquals(UIDS.get("A"), tagvs.get(3)[0]);
    assertArrayEquals(UIDS.get("D"), tagvs.get(3)[1]);
    assertArrayEquals(UIDS.get("G"), tagvs.get(3)[2]);
    
    assertEquals(3, tagvs.get(4).length);
    assertArrayEquals(UIDS.get("B"), tagvs.get(4)[0]);
    assertArrayEquals(UIDS.get("D"), tagvs.get(4)[1]);
    assertArrayEquals(UIDS.get("G"), tagvs.get(4)[2]);
    
    assertEquals(3, tagvs.get(5).length);
    assertArrayEquals(UIDS.get("C"), tagvs.get(5)[0]);
    assertArrayEquals(UIDS.get("D"), tagvs.get(5)[1]);
    assertArrayEquals(UIDS.get("G"), tagvs.get(5)[2]);
    
    // simple test
    q_tags_nometa = new ArrayList<ByteMap<byte[][]>>();
    ByteMap<byte[][]> q_tags_nometa_map = new ByteMap<byte[][]>();
    q_tags_nometa_map.put(TAGK_BYTES, new byte[][] { UIDS.get("A") });
    q_tags_nometa.add(q_tags_nometa_map);
    mgq = new MultiGetQuery(tsdb, query, METRIC_BYTES, q_tags_nometa, 
        start_ts, end_ts, tsdb.dataTable(), spans, null, 0, null, query_stats, 
        0, 0, false, multiget_no_meta);
    tagvs = mgq.prepareAllTagvCompounds();
    assertEquals(1, tagvs.size());
    assertEquals(1, tagvs.get(0).length);
    assertArrayEquals(UIDS.get("A"), tagvs.get(0)[0]);
  }

  @Test
  public void prepareRowBaseTimes() throws Exception {
    // aligned timestamps
    MultiGetQuery mgq = new MultiGetQuery(tsdb, query, METRIC_BYTES, q_tags, 
        start_ts, end_ts, tsdb.dataTable(), spans, null, 0, null, query_stats, 
        0, max_bytes, false, multiget_no_meta);
    List<Long> timestamps = mgq.prepareRowBaseTimes();
    assertEquals(17, timestamps.size());
    long expected = 1481227200;
    for (final long ts : timestamps) {
      assertEquals(expected, ts);
      expected += 3600;
    }
    
    // unaligned
    start_ts = 1481229792;
    end_ts = 1481284801;
    mgq = new MultiGetQuery(tsdb, query, METRIC_BYTES, q_tags, 
        start_ts, end_ts, tsdb.dataTable(), spans, null, 0, null, query_stats, 
        0, max_bytes, false, multiget_no_meta);
    timestamps = mgq.prepareRowBaseTimes();
    assertEquals(17, timestamps.size());
    expected = 1481227200;
    for (final long ts : timestamps) {
      assertEquals(expected, ts);
      expected += 3600;
    }
    
    // short interval
    start_ts = 1481229792;
    end_ts = 1481229961;
    mgq = new MultiGetQuery(tsdb, query, METRIC_BYTES, q_tags, 
        start_ts, end_ts, tsdb.dataTable(), spans, null, 0, null, query_stats, 
        0, max_bytes, false, multiget_no_meta);
    timestamps = mgq.prepareRowBaseTimes();
    assertEquals(1, timestamps.size());
    assertEquals(1481227200, (long) timestamps.get(0));
  }

  @Test
  public void prepareRowBaseTimesRollup() throws Exception {
    RollupInterval interval = RollupInterval.builder()
        .setTable("tsdb")
        .setPreAggregationTable("tsdb_agg")
        .setInterval("1m")
        .setRowSpan("1h")
        .build();
    RollupQuery rq = new RollupQuery(interval, aggregator, 0, aggregator);
    MultiGetQuery mgq = new MultiGetQuery(tsdb, query, METRIC_BYTES, q_tags, 
        start_ts, end_ts, tsdb.dataTable(), spans, null, 0, rq, query_stats, 
        0, max_bytes, false, multiget_no_meta);
    List<Long> timestamps = mgq.prepareRowBaseTimesRollup();
    assertEquals(17, timestamps.size());
    long expected = 1481227200;
    for (final long ts : timestamps) {
      assertEquals(expected, ts);
      expected += 3600;
    }

    interval = RollupInterval.builder()
        .setTable("tsdb")
        .setPreAggregationTable("tsdb_agg")
        .setInterval("1m")
        .setRowSpan("1d")
        .build();
    rq = new RollupQuery(interval, aggregator, 0, aggregator);
    mgq = new MultiGetQuery(tsdb, query, METRIC_BYTES, q_tags, 
        start_ts, end_ts, tsdb.dataTable(), spans, null, 0, rq, query_stats, 
        0, max_bytes, false, multiget_no_meta);
    timestamps = mgq.prepareRowBaseTimesRollup();
    timestamps = mgq.prepareRowBaseTimesRollup();
    assertEquals(2, timestamps.size());
    expected = 1481155200;
    for (final long ts : timestamps) {
      assertEquals(expected, ts);
      expected += 86400;
    }
  }

  @Test
  public void prepareRequests() throws Exception {
    MultiGetQuery mgq = new MultiGetQuery(tsdb, query, METRIC_BYTES, q_tags, 
        start_ts, end_ts, tsdb.dataTable(), spans, null, 0, null, query_stats, 
        0, max_bytes, false, multiget_no_meta);
    List<Long> timestamps = mgq.prepareRowBaseTimes();
    ByteMap<ByteMap<List<GetRequest>>> row_map = mgq.prepareRequests(timestamps, q_tags);
    ByteSet tsuids = new ByteSet();
    for (ByteMap<List<GetRequest>> rows : row_map.values()) {
      tsuids.addAll(rows.keySet());
    }
    assertEquals(3, tsuids.size());
    
    List<GetRequest> rows = new ArrayList<GetRequest>();
    for (Entry<byte[], ByteMap<List<GetRequest>>> salt_entry : row_map.entrySet()) {
      System.out.println(salt_entry.getValue());
      rows.addAll(salt_entry.getValue().get(getTSUID(METRIC_STRING, TAGK_STRING, 
         "A", TAGK_B_STRING, "D")));
    }

    assertEquals(timestamps.size(), rows.size());
    for (int i = 0; i < timestamps.size(); i++) {
      byte[] key = getRowKey(METRIC_STRING, timestamps.get(i).intValue(), 
          TAGK_STRING, "A", TAGK_B_STRING, "D");
      assertArrayEquals(key, rows.get(i).key());
    }
    
    rows = new ArrayList<GetRequest>();
    for (Entry<byte[], ByteMap<List<GetRequest>>> salt_entry : row_map.entrySet()) {
      rows.addAll(salt_entry.getValue().get(getTSUID(METRIC_STRING, TAGK_STRING, 
         "B", TAGK_B_STRING, "D")));
    }
    assertEquals(timestamps.size(), rows.size());
    for (int i = 0; i < timestamps.size(); i++) {
      byte[] key = getRowKey(METRIC_STRING, timestamps.get(i).intValue(), 
          TAGK_STRING, "B", TAGK_B_STRING, "D");
      assertArrayEquals(key, rows.get(i).key());
    }
    
    rows = new ArrayList<GetRequest>();
    for (Entry<byte[], ByteMap<List<GetRequest>>> salt_entry : row_map.entrySet()) {
      rows.addAll(salt_entry.getValue().get(getTSUID(METRIC_STRING, TAGK_STRING, 
         "C", TAGK_B_STRING, "D")));
    }
    assertEquals(timestamps.size(), rows.size());
    for (int i = 0; i < timestamps.size(); i++) {
      byte[] key = getRowKey(METRIC_STRING, timestamps.get(i).intValue(), 
          TAGK_STRING, "C", TAGK_B_STRING, "D");
      assertArrayEquals(key, rows.get(i).key());
    }
    
    rows = new ArrayList<GetRequest>();
    for (Entry<byte[], ByteMap<List<GetRequest>>> salt_entry : row_map.entrySet()) {
      rows.addAll(salt_entry.getValue().get(getTSUID(METRIC_STRING, TAGK_STRING, 
          "A", TAGK_B_STRING, "D")));
    }
   
    assertEquals(timestamps.size(), rows.size());
    for (int i = 0; i < timestamps.size(); i++) {
      byte[] key = getRowKey(METRIC_STRING, timestamps.get(i).intValue(), 
          TAGK_STRING, "A", TAGK_B_STRING, "D");
      assertArrayEquals(key, rows.get(i).key());
    }
    
    rows = new ArrayList<GetRequest>();
    for (Entry<byte[], ByteMap<List<GetRequest>>> salt_entry : row_map.entrySet()) {
      rows.addAll(salt_entry.getValue().get(getTSUID(METRIC_STRING, TAGK_STRING, 
          "B", TAGK_B_STRING, "D")));
    }
    assertEquals(timestamps.size(), rows.size());
    for (int i = 0; i < timestamps.size(); i++) {
      byte[] key = getRowKey(METRIC_STRING, timestamps.get(i).intValue(), 
          TAGK_STRING, "B", TAGK_B_STRING, "D");
      assertArrayEquals(key, rows.get(i).key());
    }
    
    rows = new ArrayList<GetRequest>();
    for (Entry<byte[], ByteMap<List<GetRequest>>> salt_entry : row_map.entrySet()) {
      rows.addAll(salt_entry.getValue().get(getTSUID(METRIC_STRING, TAGK_STRING, 
          "C", TAGK_B_STRING, "D")));
    }
    assertEquals(timestamps.size(), rows.size());
    for (int i = 0; i < timestamps.size(); i++) {
      byte[] key = getRowKey(METRIC_STRING, timestamps.get(i).intValue(), 
          TAGK_STRING, "C", TAGK_B_STRING, "D");
      assertArrayEquals(key, rows.get(i).key());
    }
  }
  
  @Test
  public void prepareRowKeysAllSalts() throws Exception {
    multiget_no_meta = true;
    if (Const.SALT_WIDTH() == 0) {
      return;
    }
    config.overrideConfig("tsd.query.multi_get.get_all_salts", "true");
    
    MultiGetQuery mgq = new MultiGetQuery(tsdb, query, METRIC_BYTES, q_tags_nometa, 
        start_ts, end_ts, tsdb.dataTable(), spans, null, 0, null, query_stats, 
        0, 0, false, multiget_no_meta);
    List<Long> timestamps = Lists.newArrayList(1481227200L, 1481230800L);
    List<byte[][]> q_tags_compounds = mgq.prepareAllTagvCompounds();
    ByteMap<ByteMap<List<GetRequest>>> row_map_map = mgq.prepareRequestsNoMeta( q_tags_compounds, timestamps);
    ByteMap<List<GetRequest>> row_map = row_map_map.get("0".getBytes());
    assertEquals(6, row_map.size());
    
    List<GetRequest> rows = row_map.get(getTSUID(METRIC_STRING, TAGK_STRING, 
        "A", TAGK_B_STRING, "D", "E", "F"));
    assertEquals(timestamps.size() * Const.SALT_BUCKETS(), rows.size());
    for (int i = 0; i < timestamps.size(); i += Const.SALT_BUCKETS()) {
      for (int x = 0; x < Const.SALT_BUCKETS(); x++) {
        byte[] key = getRowKey(METRIC_STRING, timestamps.get(i).intValue(), 
            TAGK_STRING, "A", TAGK_B_STRING, "D", "E", "F");
        key[0] = (byte) x;
        assertArrayEquals(key, rows.get(i + x).key());
      }
    }
    
    rows = row_map.get(getTSUID(METRIC_STRING, TAGK_STRING, 
        "B", TAGK_B_STRING, "D", "E", "F"));
    assertEquals(timestamps.size() * Const.SALT_BUCKETS(), rows.size());
    for (int i = 0; i < timestamps.size(); i += Const.SALT_BUCKETS()) {
      for (int x = 0; x < Const.SALT_BUCKETS(); x++) {
        byte[] key = getRowKey(METRIC_STRING, timestamps.get(i).intValue(), 
            TAGK_STRING, "B", TAGK_B_STRING, "D", "E", "F");
        key[0] = (byte) x;
        assertArrayEquals(key, rows.get(i + x).key());
      }
    }
    
    rows = row_map.get(getTSUID(METRIC_STRING, TAGK_STRING, 
        "C", TAGK_B_STRING, "D", "E", "F"));
    assertEquals(timestamps.size() * Const.SALT_BUCKETS(), rows.size());
    for (int i = 0; i < timestamps.size(); i += Const.SALT_BUCKETS()) {
      for (int x = 0; x < Const.SALT_BUCKETS(); x++) {
        byte[] key = getRowKey(METRIC_STRING, timestamps.get(i).intValue(), 
            TAGK_STRING, "C", TAGK_B_STRING, "D", "E", "F");
        key[0] = (byte) x;
        assertArrayEquals(key, rows.get(i + x).key());
      }
    }
    
    rows = row_map.get(getTSUID(METRIC_STRING, TAGK_STRING, 
        "B", TAGK_B_STRING, "D", "E", "G"));
    assertEquals(timestamps.size() * Const.SALT_BUCKETS(), rows.size());
    for (int i = 0; i < timestamps.size(); i += Const.SALT_BUCKETS()) {
      for (int x = 0; x < Const.SALT_BUCKETS(); x++) {
        byte[] key = getRowKey(METRIC_STRING, timestamps.get(i).intValue(), 
            TAGK_STRING, "B", TAGK_B_STRING, "D", "E", "G");
        key[0] = (byte) x;
        assertArrayEquals(key, rows.get(i + x).key());
      }
    }
    rows = row_map.get(getTSUID(METRIC_STRING, TAGK_STRING, 
        "C", TAGK_B_STRING, "D", "E", "G"));
    assertEquals(timestamps.size() * Const.SALT_BUCKETS(), rows.size());
    for (int i = 0; i < timestamps.size(); i += Const.SALT_BUCKETS()) {
      for (int x = 0; x < Const.SALT_BUCKETS(); x++) {
        byte[] key = getRowKey(METRIC_STRING, timestamps.get(i).intValue(), 
            TAGK_STRING, "C", TAGK_B_STRING, "D", "E", "G");
        key[0] = (byte) x;
        assertArrayEquals(key, rows.get(i + x).key());
      }
    }
  }
  
  @Test
  public void prepareConcurrentMultiGetTasks() throws Exception {
    MultiGetQuery mgq = new MultiGetQuery(tsdb, query, METRIC_BYTES, q_tags, 
        start_ts, end_ts, tsdb.dataTable(), spans, null, 0, null, query_stats, 
        0, max_bytes, false, multiget_no_meta);
    mgq.prepareConcurrentMultiGetTasks();
    final List<List<MultiGetTask>> tasks = mgq.getMultiGetTasks();
  
    assertEquals(config.getInt("tsd.query.multi_get.concurrent"), tasks.size());
    for (int i = 1; i < tasks.size(); i++) {
      assertTrue(tasks.get(i).isEmpty());
    }
    
    for (List<MultiGetTask> taskList : tasks) {
      for (MultiGetTask task : taskList) {
        byte salt = task.getGets().get(0).key()[0];
        for (GetRequest request : task.getGets()) {
         assertEquals(salt, request.key()[0]);
        }
      }
    }
    
    assertEquals(1, tasks.get(0).size());
    MultiGetTask task = tasks.get(0).get(0);
    assertEquals(3, task.getTSUIDs().size());
    assertNotNull(task.getTSUIDs().contains(getTSUID(METRIC_STRING, TAGK_STRING, 
        "A", TAGK_B_STRING, "D")));
    assertNotNull(task.getTSUIDs().contains(getTSUID(METRIC_STRING, TAGK_STRING, 
        "B", TAGK_B_STRING, "D")));
    assertNotNull(task.getTSUIDs().contains(getTSUID(METRIC_STRING, TAGK_STRING, 
        "C", TAGK_B_STRING, "D")));
    assertNotNull(task.getTSUIDs().contains(getTSUID(METRIC_STRING, TAGK_STRING, 
        "A", TAGK_B_STRING, "D")));
    assertNotNull(task.getTSUIDs().contains(getTSUID(METRIC_STRING, TAGK_STRING, 
        "B", TAGK_B_STRING, "D")));
    assertNotNull(task.getTSUIDs().contains(getTSUID(METRIC_STRING, TAGK_STRING, 
        "C", TAGK_B_STRING, "D")));
    
    assertEquals(51, task.getGets().size());
    
    
    // 6 sets of 17 timestamps. Ugly UT
    int idx = 0;
    int ts = 1481227200;
    while (idx < 17) {
    
      assertArrayEquals(task.getGets().get(idx++).key(), 
          getRowKey(METRIC_STRING, ts, TAGK_STRING, "A", TAGK_B_STRING, "D"));
      ts += 3600;
    }

    ts = 1481227200;
    while (idx < 17) {
      assertArrayEquals(task.getGets().get(idx++).key(), 
          getRowKey(METRIC_STRING, ts, TAGK_STRING, "B", TAGK_B_STRING, "D"));
      ts += 3600;
    }
    ts = 1481227200;
   
    while (idx < 17) {
      assertArrayEquals(task.getGets().get(idx++).key(), 
          getRowKey(METRIC_STRING, ts, TAGK_STRING, "C", TAGK_B_STRING, "D"));
      ts += 3600;
    }
   
  }
  
  @Test
  public void prepareConcurrentMultiGetTasksSmallBatch() throws Exception {
    config.overrideConfig("tsd.query.multi_get.batch_size", "17");
    MultiGetQuery mgq = new MultiGetQuery(tsdb, query, METRIC_BYTES, q_tags, 
        start_ts, end_ts, tsdb.dataTable(), spans, null, 0, null, query_stats, 
        0, max_bytes, false, multiget_no_meta);
    mgq.prepareConcurrentMultiGetTasks();
    final List<List<MultiGetTask>> tasks = mgq.getMultiGetTasks();
    assertEquals(config.getInt("tsd.query.multi_get.concurrent"), tasks.size());
    assertEquals(1, tasks.get(0).size());
    
    // first batch
    MultiGetTask task = tasks.get(0).get(0);
    Set<byte[]> tsuids = task.getTSUIDs();
    assertEquals(1, task.getTSUIDs().size());
    assertNotNull(task.getTSUIDs().contains(getTSUID(METRIC_STRING, TAGK_STRING, 
        "A", TAGK_B_STRING, "D")));
    assertEquals(17, task.getGets().size());
    int idx = 0;
    int ts = 1481227200;
    while (idx < 17) { // notice the early cut off. The last hour should spill over.
      assertArrayEquals(task.getGets().get(idx++).key(), 
          getRowKey(METRIC_STRING, ts, TAGK_STRING, "A", TAGK_B_STRING, "D"));
      ts += 3600;
    }
    
    // next batch
    task = tasks.get(1).get(0);
    assertEquals(1, task.getTSUIDs().size());
    assertNotNull(task.getTSUIDs().contains(getTSUID(METRIC_STRING, TAGK_STRING, 
        "B", TAGK_B_STRING, "D")));
    assertNotNull(task.getTSUIDs().contains(getTSUID(METRIC_STRING, TAGK_STRING, 
        "B", TAGK_B_STRING, "D")));
    assertEquals(17, task.getGets().size());
    idx = 0;
    ts = 1481227200;

   
    while (idx < 17) {
      assertArrayEquals(task.getGets().get(idx++).key(), 
          getRowKey(METRIC_STRING, ts, TAGK_STRING, "B", TAGK_B_STRING, "D"));
      ts += 3600;
    }
    
    // next batch
    task = tasks.get(2).get(0);
    assertEquals(1, task.getTSUIDs().size());
    assertNotNull(task.getTSUIDs().contains(getTSUID(METRIC_STRING, TAGK_STRING, 
        "C", TAGK_B_STRING, "D")));
    assertNotNull(task.getTSUIDs().contains(getTSUID(METRIC_STRING, TAGK_STRING, 
        "C", TAGK_B_STRING, "D")));
    assertEquals(17, task.getGets().size());
    idx = 0;
    ts = 1481227200;
    while (idx < 17) {
      assertArrayEquals(task.getGets().get(idx++).key(), 
          getRowKey(METRIC_STRING, ts, TAGK_STRING, "C", TAGK_B_STRING, "D"));
      ts += 3600;
    }
    
  }
  
  
  @Test
  public void prepareConcurrentMultiSortedSalts() 
      throws Exception {
    config.overrideConfig("tsd.query.multi_get.concurrent", "2");
    config.overrideConfig("tsd.query.multi_get.batch_size", "2");
    Const.setSaltWidth(1);
    MultiGetQuery mgq = new MultiGetQuery(tsdb, query, METRIC_BYTES, q_tags, 
        start_ts, end_ts, tsdb.dataTable(), spans, null, 0, null, query_stats, 
        0, max_bytes, false, multiget_no_meta);
    mgq.prepareConcurrentMultiGetTasks();
    final List<List<MultiGetTask>> tasks = mgq.getMultiGetTasks();
    assertEquals(config.getInt("tsd.query.multi_get.concurrent"), tasks.size());

    for (List<MultiGetTask> taskList : tasks) {
      for (MultiGetTask task : taskList) {
        byte salt = task.getGets().get(0).key()[0];
        for (GetRequest request : task.getGets()) {
         assertEquals(salt, request.key()[0]);
        }
      }
    }
    Const.setSaltWidth(0);

  }
  
  @Test
  public void prepareConcurrentMultiGetTasksSmallBatchAndSmallConcurrent() 
      throws Exception {
    config.overrideConfig("tsd.query.multi_get.concurrent", "2");
    config.overrideConfig("tsd.query.multi_get.batch_size", "17");
    MultiGetQuery mgq = new MultiGetQuery(tsdb, query, METRIC_BYTES, q_tags, 
        start_ts, end_ts, tsdb.dataTable(), spans, null, 0, null, query_stats, 
        0, max_bytes, false, multiget_no_meta);
    mgq.prepareConcurrentMultiGetTasks();
    final List<List<MultiGetTask>> tasks = mgq.getMultiGetTasks();
    assertEquals(config.getInt("tsd.query.multi_get.concurrent"), tasks.size());
    assertEquals(2, tasks.get(0).size());
    assertEquals(1, tasks.get(1).size());
    
    for (List<MultiGetTask> taskList : tasks) {
      for (MultiGetTask task : taskList) {
        byte salt = task.getGets().get(0).key()[0];
        for (GetRequest request : task.getGets()) {
         assertEquals(salt, request.key()[0]);
        }
      }
    }
    
    // first batch
    MultiGetTask task = tasks.get(0).get(0);
    assertEquals(1, task.getTSUIDs().size());
    assertNotNull(task.getTSUIDs().contains(getTSUID(METRIC_STRING, TAGK_STRING, 
        "A", TAGK_B_STRING, "D")));
    assertEquals(17, task.getGets().size());
    int idx = 0;
    int ts = 1481227200;
    while (idx < 16) { // notice the early cut off. The last hour should spill over.
      
      assertArrayEquals(task.getGets().get(idx++).key(), 
          getRowKey(METRIC_STRING, ts, TAGK_STRING, "A", TAGK_B_STRING, "D"));
      ts += 3600;
    }
    // 17th request
    assertArrayEquals(task.getGets().get(idx++).key(), 
        getRowKey(METRIC_STRING, ts, TAGK_STRING, "A", TAGK_B_STRING, "D"));
    ts += 3600;
    // next batch
    task = tasks.get(1).get(0);
    assertEquals(1, task.getTSUIDs().size());
    assertNotNull(task.getTSUIDs().contains(getTSUID(METRIC_STRING, TAGK_STRING, 
        "B", TAGK_B_STRING, "D")));
    assertNotNull(task.getTSUIDs().contains(getTSUID(METRIC_STRING, TAGK_STRING, 
        "B", TAGK_B_STRING, "D")));
    assertEquals(17, task.getGets().size());
    idx = 0;
    ts = 1481227200;

//    assertArrayEquals(task.getGets().get(idx++).key(), 
//        getRowKey(METRIC_STRING, ts, TAGK_STRING, "B", TAGK_B_STRING, "D"));
   
    while (idx < 17) {
      assertArrayEquals(task.getGets().get(idx++).key(), 
          getRowKey(METRIC_STRING, ts, TAGK_STRING, "B", TAGK_B_STRING, "D"));
      ts += 3600;
    }
    
    // next batch
    task = tasks.get(0).get(1);
    assertEquals(1, task.getTSUIDs().size());
    assertNotNull(task.getTSUIDs().contains(getTSUID(METRIC_STRING, TAGK_STRING, 
        "C", TAGK_B_STRING, "D")));
    assertNotNull(task.getTSUIDs().contains(getTSUID(METRIC_STRING, TAGK_STRING, 
        "C", TAGK_B_STRING, "D")));
    assertEquals(17, task.getGets().size());
    idx = 0;
    ts = 1481227200;
    while (idx < 17) {
      assertArrayEquals(task.getGets().get(idx++).key(), 
          getRowKey(METRIC_STRING, ts, TAGK_STRING, "C", TAGK_B_STRING, "D"));
      ts += 3600;
    }
    
    
  }

  @Test
  public void fetch() throws Exception {
    setupStorage();
    MultiGetQuery mgq = new MultiGetQuery(tsdb, query, METRIC_BYTES, q_tags, 
        start_ts, end_ts, tsdb.dataTable(), spans, null, 0, null, query_stats, 
        0, max_bytes, false, multiget_no_meta);
    
    final TreeMap<byte[], Span> results = mgq.fetch().join();
    assertSame(spans, results);
    verify(client, times(1)).get(anyList());
    System.out.println(spans);
    validateSpans();
  }
  
  @Test
  public void fetchMultigetNoMeta() throws Exception {
    setupStorageNoMeta();
    multiget_no_meta = true;
    MultiGetQuery mgq = new MultiGetQuery(tsdb, query, METRIC_BYTES, q_tags_nometa, 
        start_ts, end_ts, tsdb.dataTable(), spans, null, 0, null, query_stats, 
        0, max_bytes, false, multiget_no_meta);
    
    final TreeMap<byte[], Span> results = mgq.fetch().join();
    assertSame(spans, results);
    verify(client, times(1)).get(anyList());
    System.out.println(spans);
    validateSpansNometa();
  }

  @Test (expected=QueryException.class)
  public void fetchMoreThanMaxBytes() throws Exception {
    setupStorage();
    max_bytes = 0;
    MultiGetQuery mgq = new MultiGetQuery(tsdb, query, METRIC_BYTES, q_tags, 
        start_ts, end_ts, tsdb.dataTable(), spans, null, 0, null, query_stats, 
        0, max_bytes, false, multiget_no_meta);
    final TreeMap<byte[], Span> results = mgq.fetch().join();
  }
  
  @Test
  public void fetchEmptyTable() throws Exception {
    setDataPointStorage();
    spans = new TreeMap<byte[], Span>(new TsdbQuery.SpanCmp(TSDB.metrics_width()));
    MultiGetQuery mgq = new MultiGetQuery(tsdb, query, METRIC_BYTES, q_tags, 
        start_ts, end_ts, tsdb.dataTable(), spans, null, 0, null, query_stats, 
        0, max_bytes, false, multiget_no_meta);
    
    final TreeMap<byte[], Span> results = mgq.fetch().join();
    assertSame(spans, results);
    assertTrue(spans.isEmpty());
    verify(client, times(1)).get(anyList());
  }
  
  @Test
  public void fetchSmallBatch() throws Exception {
    setupStorage();
    config.overrideConfig("tsd.query.multi_get.batch_size", "16");
    
    MultiGetQuery mgq = new MultiGetQuery(tsdb, query, METRIC_BYTES, q_tags, 
        start_ts, end_ts, tsdb.dataTable(), spans, null, 0, null, query_stats, 
        0, max_bytes, false, multiget_no_meta);
    
    final TreeMap<byte[], Span> results = mgq.fetch().join();
    assertSame(spans, results);
    verify(client, times(4)).get(anyList());
    validateSpans();
  }
  
  @Test
  public void fetchSmallBatchAndSmallConcurrent() throws Exception {
    setupStorage();
    config.overrideConfig("tsd.query.multi_get.concurrent", "2");
    config.overrideConfig("tsd.query.multi_get.batch_size", "16");
    
    MultiGetQuery mgq = new MultiGetQuery(tsdb, query, METRIC_BYTES, q_tags, 
        start_ts, end_ts, tsdb.dataTable(), spans, null, 0, null, query_stats, 
        0, max_bytes, false, multiget_no_meta);
    
    final TreeMap<byte[], Span> results = mgq.fetch().join();
    assertSame(spans, results);
    verify(client, times(4)).get(anyList());
    validateSpans();
  }
  
  @Test
  public void fetchException() throws Exception {
    setupStorage();
    final RuntimeException e = new RuntimeException("Boo!");
    storage.throwException(getRowKey(METRIC_STRING, (int) start_ts, TAGK_STRING, 
        "A", TAGK_B_STRING, "D"), e);
    MultiGetQuery mgq = new MultiGetQuery(tsdb, query, METRIC_BYTES, q_tags_AD, 
        start_ts, end_ts, tsdb.dataTable(), spans, null, 0, null, query_stats, 
        0, 10000000, false, false);
    
    try {
      mgq.fetch().join();
      fail("Expected RuntimeException");
    } catch (RuntimeException ex) {
      assertSame(ex, e);
    }
    verify(client, times(1)).get(anyList());
  }
  
  /**
   * Validates the data setup in {@link #setupStorage()} is returned in the requests.
   * @throws Exception if something went pear shaped.
   */
  protected void validateSpansNometa() throws Exception {
    assertEquals(6, spans.size());
    Span span = spans.get(getRowKey(METRIC_STRING, (int) start_ts, TAGK_STRING, 
        "A", TAGK_B_STRING, "D", "E", "F"));
    SeekableView view = span.iterator();
    long ts = start_ts * 1000;
    long v = 1;
    while (view.hasNext()) {
      DataPoint dp = view.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(v++, dp.longValue());
      ts += 3600000;
    }
    
    span = spans.get(getRowKey(METRIC_STRING, (int) start_ts, TAGK_STRING, 
        "B", TAGK_B_STRING, "D", "E", "F"));
    view = span.iterator();
    ts = start_ts * 1000;
    v = 11;
    while (view.hasNext()) {
      DataPoint dp = view.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(v++, dp.longValue());
      ts += 3600000;
    }
    
    span = spans.get(getRowKey(METRIC_STRING, (int) start_ts, TAGK_STRING, 
        "C", TAGK_B_STRING, "D", "E", "F"));
    view = span.iterator();
    ts = start_ts * 1000;
    v = 111;
    while (view.hasNext()) {
      DataPoint dp = view.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(v++, dp.longValue());
      ts += 3600000;
    }
    
    span = spans.get(getRowKey(METRIC_STRING, (int) start_ts, TAGK_STRING, 
        "A", TAGK_B_STRING, "D", "E", "G"));
    view = span.iterator();
    ts = start_ts * 1000;
    v = 1111;
    while (view.hasNext()) {
      DataPoint dp = view.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(v++, dp.longValue());
      ts += 3600000;
    }
    
    span = spans.get(getRowKey(METRIC_STRING, (int) start_ts, TAGK_STRING, 
        "B", TAGK_B_STRING, "D", "E", "G"));
    view = span.iterator();
    ts = start_ts * 1000;
    v = 11111;
    while (view.hasNext()) {
      DataPoint dp = view.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(v++, dp.longValue());
      ts += 3600000;
    }
    
    span = spans.get(getRowKey(METRIC_STRING, (int) start_ts, TAGK_STRING, 
        "C", TAGK_B_STRING, "D", "E", "G"));
    view = span.iterator();
    ts = start_ts * 1000;
    v = 111111;
    while (view.hasNext()) {
      DataPoint dp = view.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(v++, dp.longValue());
      ts += 3600000;
    }
  }
  
  /**
   * Validates the data setup in {@link #setupStorage()} is returned in the requests.
   * @throws Exception if something went pear shaped.
   */
  protected void validateSpans() throws Exception {
    System.out.println(spans);
    assertEquals(3, spans.size());
    Span span = spans.get(getRowKey(METRIC_STRING, (int) start_ts, TAGK_STRING, 
        "A", TAGK_B_STRING, "D"));
    SeekableView view = span.iterator();
    long ts = start_ts * 1000;
    long v = 1;
    while (view.hasNext()) {
      DataPoint dp = view.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(v++, dp.longValue());
      ts += 3600000;
    }
    
    span = spans.get(getRowKey(METRIC_STRING, (int) start_ts, TAGK_STRING, 
        "B", TAGK_B_STRING, "D"));
    view = span.iterator();
    ts = start_ts * 1000;
    v = 11;
    while (view.hasNext()) {
      DataPoint dp = view.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(v++, dp.longValue());
      ts += 3600000;
    }
    
    span = spans.get(getRowKey(METRIC_STRING, (int) start_ts, TAGK_STRING, 
        "C", TAGK_B_STRING, "D"));
    view = span.iterator();
    ts = start_ts * 1000;
    v = 111;
    while (view.hasNext()) {
      DataPoint dp = view.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(v++, dp.longValue());
      ts += 3600000;
    }
    
    span = spans.get(getRowKey(METRIC_STRING, (int) start_ts, TAGK_STRING, 
        "A", TAGK_B_STRING, "D"));
    view = span.iterator();
    ts = start_ts * 1000;
    v = 1;
    while (view.hasNext()) {
      DataPoint dp = view.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(v++, dp.longValue());
      ts += 3600000;
    }
    
    span = spans.get(getRowKey(METRIC_STRING, (int) start_ts, TAGK_STRING, 
        "B", TAGK_B_STRING, "D"));
    view = span.iterator();
    ts = start_ts * 1000;
    v = 11;
    while (view.hasNext()) {
      DataPoint dp = view.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(v++, dp.longValue());
      ts += 3600000;
    }
    
    span = spans.get(getRowKey(METRIC_STRING, (int) start_ts, TAGK_STRING, 
        "C", TAGK_B_STRING, "D"));
    view = span.iterator();
    ts = start_ts * 1000;
    v = 111;
    while (view.hasNext()) {
      DataPoint dp = view.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(v++, dp.longValue());
      ts += 3600000;
    }
  }
  
  /**
   * Helper that writes a data point for each row that the get request should
   * cover.
   * @throws Exception if something went pear shaped.
   */
  protected void setupStorageNoMeta() throws Exception {
    setDataPointStorage();
    spans = new TreeMap<byte[], Span>(new TsdbQuery.SpanCmp(TSDB.metrics_width()));
    
    int value = 1;
    int ts = (int) start_ts;
    tags.clear();
    tags.put(TAGK_STRING, "A");
    tags.put(TAGK_B_STRING, "D");
    tags.put("E", "F");
    while (ts <= (int) end_ts) {
      tsdb.addPoint(METRIC_STRING, (long) ts, (long) value++, tags).join();
      ts += 3600;
    }
    
    value = 11;
    ts = (int) start_ts;
    tags.clear();
    tags.put(TAGK_STRING, "B");
    tags.put(TAGK_B_STRING, "D");
    tags.put("E", "F");
    while (ts <= (int) end_ts) {
      tsdb.addPoint(METRIC_STRING, (long) ts, (long) value++, tags).join();
      ts += 3600;
    }
    
    value = 111;
    ts = (int) start_ts;
    tags.clear();
    tags.put(TAGK_STRING, "C");
    tags.put(TAGK_B_STRING, "D");
    tags.put("E", "F");
    while (ts <= (int) end_ts) {
      tsdb.addPoint(METRIC_STRING, (long) ts, (long) value++, tags).join();
      ts += 3600;
    }
    
    value = 1111;
    ts = (int) start_ts;
    tags.clear();
    tags.put(TAGK_STRING, "A");
    tags.put(TAGK_B_STRING, "D");
    tags.put("E", "G");
    while (ts <= (int) end_ts) {
      tsdb.addPoint(METRIC_STRING, (long) ts, (long) value++, tags).join();
      ts += 3600;
    }
    
    value = 11111;
    ts = (int) start_ts;
    tags.clear();
    tags.put(TAGK_STRING, "B");
    tags.put(TAGK_B_STRING, "D");
    tags.put("E", "G");
    while (ts <= (int) end_ts) {
      tsdb.addPoint(METRIC_STRING, (long) ts, (long) value++, tags).join();
      ts += 3600;
    }
    
    value = 111111;
    ts = (int) start_ts;
    tags.clear();
    tags.put(TAGK_STRING, "C");
    tags.put(TAGK_B_STRING, "D");
    tags.put("E", "G");
    while (ts <= (int) end_ts) {
      tsdb.addPoint(METRIC_STRING, (long) ts, (long) value++, tags).join();
      ts += 3600;
    }
  }

  
  /**
   * Helper that writes a data point for each row that the get request should
   * cover.
   * @throws Exception if something went pear shaped.
   */
  protected void setupStorage() throws Exception {
    setDataPointStorage();
    spans = new TreeMap<byte[], Span>(new TsdbQuery.SpanCmp(TSDB.metrics_width()));
    
    int value = 1;
    int ts = (int) start_ts;
    tags.clear();
    tags.put(TAGK_STRING, "A");
    tags.put(TAGK_B_STRING, "D");
    //tags.put("E", "F");
    while (ts <= (int) end_ts) {
      tsdb.addPoint(METRIC_STRING, (long) ts, (long) value++, tags).join();
      ts += 3600;
    }
    
    value = 11;
    ts = (int) start_ts;
    tags.clear();
    tags.put(TAGK_STRING, "B");
    tags.put(TAGK_B_STRING, "D");
   // tags.put("E", "F");
    while (ts <= (int) end_ts) {
      tsdb.addPoint(METRIC_STRING, (long) ts, (long) value++, tags).join();
      ts += 3600;
    }
    
    value = 111;
    ts = (int) start_ts;
    tags.clear();
    tags.put(TAGK_STRING, "C");
    tags.put(TAGK_B_STRING, "D");
   // tags.put("E", "F");
    while (ts <= (int) end_ts) {
      tsdb.addPoint(METRIC_STRING, (long) ts, (long) value++, tags).join();
      ts += 3600;
    }
    
//    value = 1111;
//    ts = (int) start_ts;
//    tags.clear();
//    tags.put(TAGK_STRING, "A");
//    tags.put(TAGK_B_STRING, "D");
//   // tags.put("E", "G");
//    while (ts <= (int) end_ts) {
//      tsdb.addPoint(METRIC_STRING, (long) ts, (long) value++, tags).join();
//      ts += 3600;
//    }
//    
//    value = 11111;
//    ts = (int) start_ts;
//    tags.clear();
//    tags.put(TAGK_STRING, "B");
//    tags.put(TAGK_B_STRING, "D");
//   // tags.put("E", "G");
//    while (ts <= (int) end_ts) {
//      tsdb.addPoint(METRIC_STRING, (long) ts, (long) value++, tags).join();
//      ts += 3600;
//    }
//    
//    value = 111111;
//    ts = (int) start_ts;
//    tags.clear();
//    tags.put(TAGK_STRING, "C");
//    tags.put(TAGK_B_STRING, "D");
//   // tags.put("E", "G");
//    while (ts <= (int) end_ts) {
//      tsdb.addPoint(METRIC_STRING, (long) ts, (long) value++, tags).join();
//      ts += 3600;
//    }
  }
}


