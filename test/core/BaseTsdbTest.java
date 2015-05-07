// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.HashMap;
import java.util.Map;

import net.opentsdb.meta.Annotation;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.hbase.async.HBaseClient;
import org.hbase.async.Scanner;
import org.jboss.netty.util.HashedWheelTimer;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;

/**
 * Sets up a real TSDB with mocked client, compaction queue and timer along
 * with mocked UID assignment, fetches for common unit tests.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class, 
  HashedWheelTimer.class, Scanner.class, Const.class })
public class BaseTsdbTest {
  
  public static final String METRIC_STRING = "sys.cpu.user";
  public static final byte[] METRIC_BYTES = new byte[] { 0, 0, 1 };
  public static final String METRIC_B_STRING = "sys.cpu.system";
  public static final byte[] METRIC_B_BYTES = new byte[] { 0, 0, 2 };
  public static final String NSUN_METRIC = "sys.cpu.nice";
  public static final byte[] NSUI_METRIC = new byte[] { 0, 0, 3 };
  
  public static final String TAGK_STRING = "host";
  public static final byte[] TAGK_BYTES = new byte[] { 0, 0, 1 };
  public static final String TAGK_B_STRING = "owner";
  public static final byte[] TAGK_B_BYTES = new byte[] { 0, 0, 3 };
  public static final String NSUN_TAGK = "dc";
  public static final byte[] NSUI_TAGK = new byte[] { 0, 0, 4 };
  
  public static final String TAGV_STRING = "web01";
  public static final byte[] TAGV_BYTES = new byte[] { 0, 0, 1 };
  public static final String TAGV_B_STRING = "web02";
  public static final byte[] TAGV_B_BYTES = new byte[] { 0, 0, 2 };
  public static final String NSUN_TAGV = "web03";
  public static final byte[] NSUI_TAGV = new byte[] { 0, 0, 3 };

  static final String NOTE_DESCRIPTION = "Hello DiscWorld!";
  static final String NOTE_NOTES = "Millenium hand and shrimp";
  
  protected HashedWheelTimer timer;
  protected Config config;
  protected TSDB tsdb;
  protected HBaseClient client = mock(HBaseClient.class);
  protected UniqueId metrics = mock(UniqueId.class);
  protected UniqueId tag_names = mock(UniqueId.class);
  protected UniqueId tag_values = mock(UniqueId.class);
  protected Map<String, String> tags = new HashMap<String, String>(1);
  protected MockBase storage;
  
  @Before
  public void before() throws Exception {
    timer = mock(HashedWheelTimer.class);

    PowerMockito.whenNew(HashedWheelTimer.class).withNoArguments()
      .thenReturn(timer);
    PowerMockito.whenNew(HBaseClient.class).withAnyArguments()
      .thenReturn(client);
    
    config = new Config(false);
    config.overrideConfig("tsd.storage.enable_compaction", "false");
    tsdb = PowerMockito.spy(new TSDB(config));

    config.setAutoMetric(true);
    
    Whitebox.setInternalState(tsdb, "metrics", metrics);
    Whitebox.setInternalState(tsdb, "tag_names", tag_names);
    Whitebox.setInternalState(tsdb, "tag_values", tag_values);

    setupMetricMaps();
    setupTagkMaps();
    setupTagvMaps();
    
    when(metrics.width()).thenReturn((short)3);
    when(tag_names.width()).thenReturn((short)3);
    when(tag_values.width()).thenReturn((short)3);
    
    tags.put(TAGK_STRING, TAGV_STRING);
  }
  
  void setupMetricMaps() {
    when(metrics.getId(METRIC_STRING)).thenReturn(METRIC_BYTES);
    when(metrics.getIdAsync(METRIC_STRING))
      .thenReturn(Deferred.fromResult(METRIC_BYTES));
    when(metrics.getOrCreateId(METRIC_STRING))
      .thenReturn(METRIC_BYTES);
    
    when(metrics.getId(METRIC_B_STRING)).thenReturn(METRIC_B_BYTES);
    when(metrics.getIdAsync(METRIC_B_STRING))
      .thenReturn(Deferred.fromResult(METRIC_B_BYTES));
    when(metrics.getOrCreateId(METRIC_B_STRING))
      .thenReturn(METRIC_B_BYTES);
    
    when(metrics.getNameAsync(METRIC_BYTES))
      .thenReturn(Deferred.fromResult(METRIC_STRING));
    when(metrics.getNameAsync(METRIC_B_BYTES))
      .thenReturn(Deferred.fromResult(METRIC_B_STRING));
    when(metrics.getNameAsync(NSUI_METRIC))
      .thenThrow(new NoSuchUniqueId("metrics", NSUI_METRIC));
    
    final NoSuchUniqueName nsun = new NoSuchUniqueName(NSUN_METRIC, "metrics");
    
    when(metrics.getId(NSUN_METRIC)).thenThrow(nsun);
    when(metrics.getIdAsync(NSUN_METRIC))
      .thenReturn(Deferred.<byte[]>fromError(nsun));
    when(metrics.getOrCreateId(NSUN_METRIC)).thenThrow(nsun);
  }
  
  void setupTagkMaps() {
    when(tag_names.getId(TAGK_STRING)).thenReturn(TAGK_BYTES);
    when(tag_names.getOrCreateId(TAGK_STRING)).thenReturn(TAGK_BYTES);
    when(tag_names.getIdAsync(TAGK_STRING))
      .thenReturn(Deferred.fromResult(TAGK_BYTES));
    when(tag_names.getOrCreateIdAsync(TAGK_STRING))
      .thenReturn(Deferred.fromResult(TAGK_BYTES));
    
    when(tag_names.getId(TAGK_B_STRING)).thenReturn(TAGK_B_BYTES);
    when(tag_names.getOrCreateId(TAGK_B_STRING)).thenReturn(TAGK_B_BYTES);
    when(tag_names.getIdAsync(TAGK_B_STRING))
      .thenReturn(Deferred.fromResult(TAGK_B_BYTES));
    when(tag_names.getOrCreateIdAsync(TAGK_B_STRING))
      .thenReturn(Deferred.fromResult(TAGK_B_BYTES));
    
    when(tag_names.getNameAsync(TAGK_BYTES))
      .thenReturn(Deferred.fromResult(TAGK_STRING));
    when(tag_names.getNameAsync(TAGK_B_BYTES))
      .thenReturn(Deferred.fromResult(TAGK_B_STRING));
    when(tag_names.getNameAsync(NSUI_TAGK))
      .thenThrow(new NoSuchUniqueId("tagk", NSUI_TAGK));
    
    final NoSuchUniqueName nsun = new NoSuchUniqueName(NSUN_TAGK, "tagk");
    
    when(tag_names.getId(NSUN_TAGK))
      .thenThrow(nsun);
    when(tag_names.getIdAsync(NSUN_TAGK))
      .thenReturn(Deferred.<byte[]>fromError(nsun));
  }
  
  void setupTagvMaps() {
    when(tag_values.getId(TAGV_STRING)).thenReturn(TAGV_BYTES);
    when(tag_values.getOrCreateId(TAGV_STRING)).thenReturn(TAGV_BYTES);
    when(tag_values.getIdAsync(TAGV_STRING))
      .thenReturn(Deferred.fromResult(TAGV_BYTES));
    when(tag_values.getOrCreateIdAsync(TAGV_STRING))
      .thenReturn(Deferred.fromResult(TAGV_BYTES));
  
    when(tag_values.getId(TAGV_B_STRING)).thenReturn(TAGV_B_BYTES);
    when(tag_values.getOrCreateId(TAGV_B_STRING)).thenReturn(TAGV_B_BYTES);
    when(tag_values.getIdAsync(TAGV_B_STRING))
      .thenReturn(Deferred.fromResult(TAGV_B_BYTES));
    when(tag_values.getOrCreateIdAsync(TAGV_B_STRING))
      .thenReturn(Deferred.fromResult(TAGV_B_BYTES));
    
    when(tag_values.getNameAsync(TAGV_BYTES))
      .thenReturn(Deferred.fromResult(TAGV_STRING));
    when(tag_values.getNameAsync(TAGV_B_BYTES))
      .thenReturn(Deferred.fromResult(TAGV_B_STRING));
    when(tag_values.getNameAsync(NSUI_TAGV))
      .thenThrow(new NoSuchUniqueId("tagv", NSUI_TAGV));
    
    final NoSuchUniqueName nsun = new NoSuchUniqueName(NSUN_TAGV, "tagv");
    
    when(tag_values.getId(NSUN_TAGV)).thenThrow(nsun);
    when(tag_values.getIdAsync(NSUN_TAGV))
      .thenReturn(Deferred.<byte[]>fromError(nsun));
  }

  // ----------------- //
  // Helper functions. //
  // ----------------- //
  
  /** @return a row key template with the default metric and tags */
  protected byte[] getRowKeyTemplate() {
    return IncomingDataPoints.rowKeyTemplate(tsdb, METRIC_STRING, tags);
  }
  
  protected void setDataPointStorage() throws Exception {
    storage = new MockBase(tsdb, client, true, true, true, true);
    storage.setFamily("t".getBytes(MockBase.ASCII()));
  }
  
  protected void storeLongTimeSeriesSeconds(final boolean two_metrics, 
      final boolean offset) throws Exception {
    setDataPointStorage();
    
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags_local = new HashMap<String, String>(tags);
    long timestamp = 1356998400;
    for (int i = 1; i <= 300; i++) {
      tsdb.addPoint(METRIC_STRING, timestamp += 30, i, tags_local)
        .joinUninterruptibly();
      if (two_metrics) {
        tsdb.addPoint(METRIC_B_STRING, timestamp, i, tags_local)
          .joinUninterruptibly();
      }
    }

    // dump a parallel set but invert the values
    tags_local.clear();
    tags_local.put(TAGK_STRING, TAGV_B_STRING);
    timestamp = offset ? 1356998415 : 1356998400;
    for (int i = 300; i > 0; i--) {
      tsdb.addPoint(METRIC_STRING, timestamp += 30, i, tags_local)
        .joinUninterruptibly();
      if (two_metrics) {
        tsdb.addPoint(METRIC_B_STRING, timestamp, i, tags_local)
          .joinUninterruptibly();
      }
    }
  }
 
  protected void storeLongTimeSeriesMs() throws Exception {
    setDataPointStorage();
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags_local = new HashMap<String, String>(tags);
    long timestamp = 1356998400000L;
    for (int i = 1; i <= 300; i++) {
      tsdb.addPoint(METRIC_STRING, timestamp += 500, i, tags_local)
        .joinUninterruptibly();
      tsdb.addPoint(METRIC_B_STRING, timestamp, i, tags_local)
        .joinUninterruptibly();
    }

    // dump a parallel set but invert the values
    tags_local.clear();
    tags_local.put(TAGK_STRING, TAGV_B_STRING);
    timestamp = 1356998400000L;
    for (int i = 300; i > 0; i--) {
      tsdb.addPoint(METRIC_STRING, timestamp += 500, i, tags_local)
        .joinUninterruptibly();
      tsdb.addPoint(METRIC_B_STRING, timestamp, i, tags_local)
        .joinUninterruptibly();
    }
  }
  
  /**
   * Create two metrics with same name, skipping every third point in host=web01
   * and every other point in host=web02. To wit:
   *
   *       METRIC    TAG  t0   t1   t2   t3   t4   t5   ...
   * sys.cpu.user  web01   X    2    3    X    5    6   ...
   * sys.cpu.user  web02   X  299    X  297    X  295   ...
   */
  protected void storeLongTimeSeriesWithMissingData() throws Exception {
    setDataPointStorage();

    // host=web01
    HashMap<String, String> tags_local = new HashMap<String, String>(tags);
    long timestamp = 1356998400L;
    for (int i = 0; i < 300; ++i) {
      // Skip every third point.
      if (0 != (i % 3)) {
        tsdb.addPoint(METRIC_STRING, timestamp, i + 1, tags_local)
          .joinUninterruptibly();
      }
      timestamp += 10L;
    }

    // host=web02
    tags_local.clear();
    tags_local.put(TAGK_STRING, TAGV_B_STRING);
    timestamp = 1356998400L;
    for (int i = 300; i > 0; --i) {
      // Skip every other point.
      if (0 != (i % 2)) {
        tsdb.addPoint(METRIC_STRING, timestamp, i, tags_local)
          .joinUninterruptibly();
      }
      timestamp += 10L;
    }
  }
  
  protected void storeFloatTimeSeriesSeconds(final boolean two_metrics, 
      final boolean offset) throws Exception {
    setDataPointStorage();
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags_local = new HashMap<String, String>(tags);
    long timestamp = 1356998400;
    for (float i = 1.25F; i <= 76; i += 0.25F) {
      tsdb.addPoint(METRIC_STRING, timestamp += 30, i, tags_local)
        .joinUninterruptibly();
      if (two_metrics) {
        tsdb.addPoint(METRIC_B_STRING, timestamp, i, tags_local)
          .joinUninterruptibly();
      }
    }

    // dump a parallel set but invert the values
    tags_local.clear();
    tags_local.put(TAGK_STRING, TAGV_B_STRING);
    timestamp = offset ? 1356998415 : 1356998400;
    for (float i = 75F; i > 0; i -= 0.25F) {
      tsdb.addPoint(METRIC_STRING, timestamp += 30, i, tags_local)
        .joinUninterruptibly();
      if (two_metrics) {
        tsdb.addPoint(METRIC_B_STRING, timestamp, i, tags_local)
          .joinUninterruptibly();
      }
    }
  }
  
  protected void storeFloatTimeSeriesMs() throws Exception {
    setDataPointStorage();
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags_local = new HashMap<String, String>(tags);
    long timestamp = 1356998400000L;
    for (float i = 1.25F; i <= 76; i += 0.25F) {
      tsdb.addPoint(METRIC_STRING, timestamp += 500, i, tags_local)
        .joinUninterruptibly();
      tsdb.addPoint(METRIC_B_STRING, timestamp, i, tags_local)
        .joinUninterruptibly();
    }

    // dump a parallel set but invert the values
    tags_local.clear();
    tags_local.put(TAGK_STRING, TAGV_B_STRING);
    timestamp = 1356998400000L;
    for (float i = 75F; i > 0; i -= 0.25F) {
      tsdb.addPoint(METRIC_STRING, timestamp += 500, i, tags_local)
        .joinUninterruptibly();
      tsdb.addPoint(METRIC_B_STRING, timestamp, i, tags_local)
        .joinUninterruptibly();
    }
  }
  
  protected void storeMixedTimeSeriesSeconds() throws Exception {
    setDataPointStorage();
    HashMap<String, String> tags_local = new HashMap<String, String>(tags);
    long timestamp = 1356998400;
    for (float i = 1.25F; i <= 76; i += 0.25F) {
      if (i % 2 == 0) {
        tsdb.addPoint(METRIC_STRING, timestamp += 30, (long)i, tags_local)
          .joinUninterruptibly();
      } else {
        tsdb.addPoint(METRIC_STRING, timestamp += 30, i, tags_local)
          .joinUninterruptibly();
      }
    }
  }
  
  // dumps ints, floats, seconds and ms
  protected void storeMixedTimeSeriesMsAndS() throws Exception {
    setDataPointStorage();
    HashMap<String, String> tags_local = new HashMap<String, String>(tags);
    long timestamp = 1356998400000L;
    for (float i = 1.25F; i <= 76; i += 0.25F) {
      long ts = timestamp += 500;
      if (ts % 1000 == 0) {
        ts /= 1000;
      }
      if (i % 2 == 0) {
        tsdb.addPoint(METRIC_STRING, ts, (long)i, tags_local).joinUninterruptibly();
      } else {
        tsdb.addPoint(METRIC_STRING, ts, i, tags_local).joinUninterruptibly();
      }
    }
  }

  /**
   * Validates the metric name, tags and annotations
   * @param dps The datapoints array returned from the query
   * @param index The index to peek into the array
   * @param agged_tags Whether or not the tags were aggregated out
   */
  protected void assertMeta(final DataPoints[] dps, final int index, 
      final boolean agged_tags) {
    assertMeta(dps, index, agged_tags, false);
  }
  
  /**
   * Validates the metric name, tags and annotations
   * @param dps The datapoints array returned from the query
   * @param index The index to peek into the array
   * @param agged_tags Whether or not the tags were aggregated out
   * @param annotation Whether we're expecting a note or not
   */
  protected void assertMeta(final DataPoints[] dps, final int index, 
      final boolean agged_tags, final boolean annotation) {
    assertNotNull(dps);
    assertEquals(METRIC_STRING, dps[index].metricName());
    
    if (agged_tags) {
      assertTrue(dps[index].getTags().isEmpty());
      assertEquals(TAGK_STRING, dps[index].getAggregatedTags().get(0));
    } else {
      if (index == 0) {
        assertTrue(dps[index].getAggregatedTags().isEmpty());
        assertEquals(TAGV_STRING, dps[index].getTags().get(TAGK_STRING));
      } else {
        assertEquals(TAGV_B_STRING, dps[index].getTags().get(TAGK_STRING));
      }
    }
    
    if (annotation) {
      assertEquals(1, dps[index].getAnnotations().size());
      assertEquals(NOTE_DESCRIPTION, dps[index].getAnnotations().get(0)
          .getDescription());
      assertEquals(NOTE_NOTES, dps[index].getAnnotations().get(0).getNotes());
    } else {
      assertNull(dps[index].getAnnotations());
    }
  }

  /**
   * Stores a single annotation in the given row
   * @param timestamp The time to store the data point at
   * @throws Exception
   */
  protected void storeAnnotation(final long timestamp) throws Exception {
    final Annotation note = new Annotation();
    note.setTSUID("000001000001000001");
    note.setStartTime(timestamp);
    note.setDescription(NOTE_DESCRIPTION);
    note.setNotes(NOTE_NOTES);
    note.syncToStorage(tsdb, false).joinUninterruptibly();
  }

}