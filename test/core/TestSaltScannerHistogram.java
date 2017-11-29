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

import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;

import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.filter.TagVRegexFilter;
import net.opentsdb.query.filter.TagVWildcardFilter;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.Threads;

import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.jboss.netty.util.HashedWheelTimer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({ TSDB.class, Scanner.class, SaltScanner.class, Span.class,
  Const.class, UniqueId.class, Tags.class, QueryStats.class, DateTime.class,
  HistogramCodecManager.class,
  SimpleHistogram.class, SimpleHistogramDecoder.class})
public class TestSaltScannerHistogram extends BaseTsdbTest {
  protected final static byte[] FAMILY = "t".getBytes();
  protected final static byte[] QUALIFIER_A = { 0x06, 0x00, 0x00};
  protected final static byte[] QUALIFIER_B = { 0x06, 0x10, 0x00 };

  protected byte[] VALUE;
  
  protected List<Scanner> scanners;
  protected TreeMap<byte[], HistogramSpan> spans;

  protected List<ArrayList<ArrayList<KeyValue>>> kvs_a;
  protected List<ArrayList<ArrayList<KeyValue>>> kvs_b;

  protected Scanner scanner_a;
  protected Scanner scanner_b;
  protected QueryStats query_stats;

  protected byte[] key_a;
  //different tagv
  protected byte[] key_b;
  //same as A bug different time
  protected byte[] key_c;
  
  @Before
  public void before() throws Exception {
    // Copying the whole thing as the SPY in the base mucks up the references.
    uid_map = Maps.newHashMap();
    PowerMockito.mockStatic(Threads.class);
    timer = new FakeTaskTimer();
    PowerMockito.when(Threads.newTimer(anyString())).thenReturn(timer);
    PowerMockito.when(Threads.newTimer(anyInt(), anyString())).thenReturn(timer);
    
    PowerMockito.whenNew(HashedWheelTimer.class).withNoArguments()
      .thenReturn(timer);
    PowerMockito.whenNew(HBaseClient.class).withAnyArguments()
      .thenReturn(client);
    
    config = new Config(false);
    config.overrideConfig("tsd.storage.enable_compaction", "false");
    tsdb = new TSDB(config);

    config.setAutoMetric(true);
    
    Whitebox.setInternalState(tsdb, "metrics", metrics);
    Whitebox.setInternalState(tsdb, "tag_names", tag_names);
    Whitebox.setInternalState(tsdb, "tag_values", tag_values);

    setupMetricMaps();
    setupTagkMaps();
    setupTagvMaps();
    
    mockUID(UniqueIdType.METRIC, HISTOGRAM_METRIC_STRING, HISTOGRAM_METRIC_BYTES);
    
    // add metrics and tags to the UIDs list for other functions to share
    uid_map.put(METRIC_STRING, METRIC_BYTES);
    uid_map.put(METRIC_B_STRING, METRIC_B_BYTES);
    uid_map.put(NSUN_METRIC, NSUI_METRIC);
    uid_map.put(HISTOGRAM_METRIC_STRING, HISTOGRAM_METRIC_BYTES);
    
    uid_map.put(TAGK_STRING, TAGK_BYTES);
    uid_map.put(TAGK_B_STRING, TAGK_B_BYTES);
    uid_map.put(NSUN_TAGK, NSUI_TAGK);
    
    uid_map.put(TAGV_STRING, TAGV_BYTES);
    uid_map.put(TAGV_B_STRING, TAGV_B_BYTES);
    uid_map.put(NSUN_TAGV, NSUI_TAGV);
    
    uid_map.putAll(UIDS);
    
    when(metrics.width()).thenReturn((short)3);
    when(tag_names.width()).thenReturn((short)3);
    when(tag_values.width()).thenReturn((short)3);
    
    tags = new HashMap<String, String>(1);
    tags.put(TAGK_STRING, TAGV_STRING);
    config.overrideConfig("tsd.core.histograms.config", 
        "{\"net.opentsdb.core.LongHistogramDataPointForTestDecoder\": 0}");
    HistogramCodecManager manager = 
        new HistogramCodecManager(tsdb);
    Whitebox.setInternalState(tsdb, "histogram_manager", manager);
    
    ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    Output output = new Output(outBuffer);
    output.writeByte(0 /*HistoType.SimpleHistogramType.ordinal()*/);
    output.writeShort(8);
    output.writeFloat(1.0f);
    output.writeFloat(2.0f);
    output.writeLong(5, true);
    output.writeFloat(2.0f);
    output.writeFloat(3.0f);
    output.writeLong(5, true);
    output.writeFloat(3.0f);
    output.writeFloat(4.0f);
    output.writeLong(5, true);
    output.writeFloat(4.0f);
    output.writeFloat(5.0f);
    output.writeLong(0, true);
    output.writeFloat(5.0f);
    output.writeFloat(6.0f);
    output.writeLong(0, true);
    output.writeFloat(6.0f);
    output.writeFloat(7.0f);
    output.writeLong(0, true);
    output.writeFloat(7.0f);
    output.writeFloat(8.0f);
    output.writeLong(0, true);
    output.writeFloat(8.0f);
    output.writeFloat(9.0f);
    output.writeLong(0, true);
    output.writeLong(0, true);
    output.writeLong(2, true);
    output.close();
    VALUE = outBuffer.toByteArray();

    query_stats = mock(QueryStats.class);
    spans = new TreeMap<byte[], HistogramSpan>(new RowKey.SaltCmp());
    setupMockScanners(true);
    
    key_a = getRowKey(METRIC_STRING, 1356998400, TAGK_STRING, TAGV_STRING, 
        TAGK_B_STRING, TAGV_STRING);
    key_b = getRowKey(METRIC_STRING, 1356998400, TAGK_STRING, TAGV_B_STRING, 
        TAGK_B_STRING, TAGV_STRING);
    key_c = getRowKey(METRIC_STRING, 1359680400, TAGK_STRING, TAGV_STRING, 
        TAGK_B_STRING, TAGV_STRING);
  }

  @Test
  public void scan() throws Exception {
    setupMockScanners(false);

    SimpleHistogram y1Hist = mock(SimpleHistogram.class);
    PowerMockito.whenNew(SimpleHistogram.class).withAnyArguments().thenReturn(y1Hist);

    final SaltScanner scanner = new SaltScanner(tsdb, METRIC_BYTES, scanners, 
        null, null, false, null, query_stats, 0, spans, 0, 0);
    assertTrue(spans == scanner.scanHistogram().joinUninterruptibly());
    assertEquals(3, spans.size());

    HistogramSpan span = spans.get(key_a);
    assertEquals(2, span.size());
    assertEquals(1356998400000L, span.timestamp(0));
    assertEquals(1357002496000L, span.timestamp(1));
    assertEquals(1, span.getAnnotations().size());

    span = spans.get(key_b);
    assertEquals(1, span.size());
    assertEquals(1356998400000L, span.timestamp(0));
    assertEquals(0, span.getAnnotations().size());

    span = spans.get(key_c);
    assertEquals(2, span.size());
    assertEquals(1359680400000L, span.timestamp(0));
    assertEquals(1359684496000L, span.timestamp(1));
    assertEquals(0, span.getAnnotations().size());
  }

  @Test
  public void scanWithFilter() throws Exception {
    setupMockScanners(false); 
    List<TagVFilter> filters = new ArrayList<TagVFilter>(1);
    filters.add(new TagVWildcardFilter(TAGK_STRING, "web*"));

    SimpleHistogram y1Hist = mock(SimpleHistogram.class);
    PowerMockito.whenNew(SimpleHistogram.class).withAnyArguments().thenReturn(y1Hist);

    final SaltScanner scanner = new SaltScanner(tsdb, METRIC_BYTES, scanners, 
        null, null, false, null, query_stats, 0, spans, 0, 0);

    assertTrue(spans == scanner.scanHistogram().joinUninterruptibly());
    assertEquals(3, spans.size());

    HistogramSpan span = spans.get(key_a);
    assertEquals(2, span.size());
    assertEquals(1356998400000L, span.timestamp(0));
    assertEquals(1357002496000L, span.timestamp(1));
    assertEquals(1, span.getAnnotations().size());

    span = spans.get(key_b);
    assertEquals(1, span.size());
    assertEquals(1356998400000L, span.timestamp(0));
    assertEquals(0, span.getAnnotations().size());

    span = spans.get(key_c);
    assertEquals(2, span.size());
    assertEquals(1359680400000L, span.timestamp(0));
    assertEquals(1359684496000L, span.timestamp(1));
    assertEquals(0, span.getAnnotations().size());
  }

  @Test
  public void scanWithFiltersOnSameTag() throws Exception {
    setupMockScanners(false);
    List<TagVFilter> filters = new ArrayList<TagVFilter>(1);
    filters.add(new TagVWildcardFilter("host", "web*"));
    filters.add(new TagVWildcardFilter("host", "w*b*"));
    filters.add(new TagVRegexFilter("host", "w.*"));

    SimpleHistogram y1Hist = mock(SimpleHistogram.class);
    PowerMockito.whenNew(SimpleHistogram.class).withAnyArguments().thenReturn(y1Hist);

    final SaltScanner scanner = new SaltScanner(tsdb, METRIC_BYTES, scanners, 
        null, null, false, null, query_stats, 0, spans, 0, 0);

    assertTrue(spans == scanner.scanHistogram().joinUninterruptibly());
    assertEquals(3, spans.size());

    HistogramSpan span = spans.get(key_a);
    assertEquals(2, span.size());
    assertEquals(1356998400000L, span.timestamp(0));
    assertEquals(1357002496000L, span.timestamp(1));
    assertEquals(1, span.getAnnotations().size());

    span = spans.get(key_b);
    assertEquals(1, span.size());
    assertEquals(1356998400000L, span.timestamp(0));
    assertEquals(0, span.getAnnotations().size());

    span = spans.get(key_c);
    assertEquals(2, span.size());
    assertEquals(1359680400000L, span.timestamp(0));
    assertEquals(1359684496000L, span.timestamp(1));
    assertEquals(0, span.getAnnotations().size());
  }

  @Test
  public void scanWithFiltersOnSameTagOneFail() throws Exception {
    setupMockScanners(false);
    List<TagVFilter> filters = new ArrayList<TagVFilter>(1);
    filters.add(new TagVWildcardFilter("host", "web*"));
    filters.add(new TagVWildcardFilter("host", "drood*"));

    SimpleHistogram y1Hist = mock(SimpleHistogram.class);
    PowerMockito.whenNew(SimpleHistogram.class).withAnyArguments().thenReturn(y1Hist);

    final SaltScanner scanner = new SaltScanner(tsdb, METRIC_BYTES, scanners, 
        null, filters, false, null, query_stats, 0, spans, 0, 0);

    assertTrue(spans == scanner.scanHistogram().joinUninterruptibly());
    assertEquals(0, spans.size());
  }

  /**
   * Sets up a pair of scanners with either a list of values or no data
   * @param no_data Whether or not to return 0 data.
   */
  protected void setupMockScanners(final boolean no_data) {
    if (Const.SALT_WIDTH() > 0) {
      scanners = new ArrayList<Scanner>(Const.SALT_BUCKETS());
      scanner_a = mock(Scanner.class);
      scanner_b = mock(Scanner.class);
      if (no_data) {
        when(scanner_a.nextRows()).thenReturn(
                Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));
        when(scanner_b.nextRows()).thenReturn(
                Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));
      } else {
        setupValues();
      }
      scanners.add(scanner_a);
      scanners.add(scanner_b);
    } else {
      scanners = new ArrayList<Scanner>(1);
      scanner_a = mock(Scanner.class);
      if (no_data) {
        when(scanner_a.nextRows()).thenReturn(
            Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));
      } else {
        setupValues();
      }
      scanners.add(scanner_a);
    }
  }

  /**
   * This method sets up some row keys and values to pass to the scanners.
   * The values aren't exactly what would normally be passed to a salt scanner
   * in that we have the same series salted across separate buckets. That would
   * only happen if you add the timestamp to the salt calculation, which we
   * may do in the future. We're testing now for future proofing.
   */
  protected void setupValues() {
    kvs_a = new ArrayList<ArrayList<ArrayList<KeyValue>>>(3);
    kvs_b = new ArrayList<ArrayList<ArrayList<KeyValue>>>(2);

    final String note = "{\"tsuid\":\"000000010000000100000001\","
          + "\"startTime\":1356998490,\"endTime\":0,\"description\":"
          + "\"The Great A'Tuin!\",\"notes\":\"Millenium hand and shrimp\","
          + "\"custom\":null}";

    for (int i = 0; i < 5; i++) {
      final ArrayList<ArrayList<KeyValue>> rows =
            new ArrayList<ArrayList<KeyValue>>(1);
      final ArrayList<KeyValue> row = new ArrayList<KeyValue>(2);
      rows.add(row);
      byte[] key = null;

      switch (i) {
        case 0:
            row.add(new KeyValue(key_a, FAMILY, QUALIFIER_A, 0, VALUE));
            kvs_a.add(rows);
            break;
        case 1:
            row.add(new KeyValue(key_b, FAMILY, QUALIFIER_A, 0, VALUE));
            kvs_a.add(rows);
            break;
        case 2:
            row.add(new KeyValue(key_c, FAMILY, QUALIFIER_A, 0, VALUE));
            kvs_a.add(rows);
            break;
        case 3:
            key = Arrays.copyOf(key_a, key_a.length);
            row.add(new KeyValue(key, FAMILY, QUALIFIER_B, 0, VALUE));
            row.add(new KeyValue(key, FAMILY, new byte[] { 1, 0, 0 }, 0,
                    note.getBytes(Charset.forName("UTF8"))));
            kvs_b.add(rows);
            break;
        case 4:
            key = Arrays.copyOf(key_c, key_c.length);
            row.add(new KeyValue(key, FAMILY, QUALIFIER_B, 0, VALUE));
            kvs_b.add(rows);
            break;
      }
    }

    if (Const.SALT_WIDTH() > 0) {
      when(scanner_a.nextRows())
        .thenReturn(Deferred.fromResult(kvs_a.get(0)))
        .thenReturn(Deferred.fromResult(kvs_a.get(1)))
        .thenReturn(Deferred.fromResult(kvs_a.get(2)))
        .thenReturn(Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));
      when(scanner_b.nextRows())
        .thenReturn(Deferred.fromResult(kvs_b.get(0)))
        .thenReturn(Deferred.fromResult(kvs_b.get(1)))
        .thenReturn(Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));
    } else {
      when(scanner_a.nextRows())
      .thenReturn(Deferred.fromResult(kvs_a.get(0)))
      .thenReturn(Deferred.fromResult(kvs_a.get(1)))
      .thenReturn(Deferred.fromResult(kvs_a.get(2)))
      .thenReturn(Deferred.fromResult(kvs_b.get(0)))
      .thenReturn(Deferred.fromResult(kvs_b.get(1)))
      .thenReturn(Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));
    }
  }
}
