// This file is part of OpenTSDB.
// Copyright (C) 2015-2016  The OpenTSDB Authors.
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
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import net.opentsdb.meta.Annotation;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.Threads;

import org.hbase.async.Bytes;
import org.hbase.async.HBaseClient;
import org.hbase.async.Scanner;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.Maps;
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
  HashedWheelTimer.class, Scanner.class, Const.class, Threads.class })
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
  
  //histgoram metric
  public static final String HISTOGRAM_METRIC_STRING = "msg.end2end.latency";
  public static final byte[] HISTOGRAM_METRIC_BYTES = new byte[] { 0, 0, 5 };
  
  public static final Map<String, byte[]> UIDS = new HashMap<String, byte[]>(26);
  static {
    char letter = 'A';
    byte[] uid = new byte[] { 0, 0, 10 };
    for (int i = 0; i < 26; i++) {
      UIDS.put(Character.toString(letter++), Arrays.copyOf(uid, uid.length));
      uid[2]++;
    }
  }
  
  protected FakeTaskTimer timer;
  protected Config config;
  protected TSDB tsdb;
  protected HBaseClient client = mock(HBaseClient.class);
  protected UniqueId metrics = mock(UniqueId.class);
  protected UniqueId tag_names = mock(UniqueId.class);
  protected UniqueId tag_values = mock(UniqueId.class);
  protected Map<String, String> tags;
  protected MockBase storage;
  protected Map<String, byte[]> uid_map;
  
  @Before
  public void before() throws Exception {
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
    tsdb = PowerMockito.spy(new TSDB(config));

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
  }
  
  /** Adds the static UIDs to the metrics UID mock object */
  public void setupMetricMaps() {
    mockUID(UniqueIdType.METRIC, METRIC_STRING, METRIC_BYTES);
    mockUID(UniqueIdType.METRIC, METRIC_B_STRING, METRIC_B_BYTES);

    final NoSuchUniqueName nsun = new NoSuchUniqueName(NSUN_METRIC, "metric");

    when(metrics.getId(NSUN_METRIC)).thenThrow(nsun);
    when(metrics.getIdAsync(NSUN_METRIC))
        .thenReturn(Deferred.<byte[]> fromError(nsun));
    when(metrics.getOrCreateId(NSUN_METRIC)).thenThrow(nsun);
    final NoSuchUniqueName nsunic = new NoSuchUniqueName(NSUN_METRIC, "metric");
    when(metrics.getOrCreateIdAsync(eq(NSUN_METRIC))).thenThrow(nsunic);
    when(metrics.getNameAsync(NSUI_METRIC)).thenReturn(
        Deferred.<String>fromError(new NoSuchUniqueId("metrics", NSUI_METRIC)));
    
    for (final Map.Entry<String, byte[]> uid : UIDS.entrySet()) {
      mockUID(UniqueIdType.METRIC, uid.getKey(), uid.getValue());
    }
  }
  
  /** Adds the static UIDs to the tag keys UID mock object */
  public void setupTagkMaps() {
    mockUID(UniqueIdType.TAGK, TAGK_STRING, TAGK_BYTES);
    mockUID(UniqueIdType.TAGK, TAGK_B_STRING, TAGK_B_BYTES);

    final NoSuchUniqueName nsunic = new NoSuchUniqueName(NSUN_TAGK, "tagk");
    when(tag_names.getIdAsync(NSUN_TAGK)).thenReturn(
        Deferred.<byte[]> fromError(nsunic));
    when(tag_names.getOrCreateId(eq(NSUN_TAGK))).thenThrow(nsunic);
    when(tag_names.getOrCreateIdAsync(eq(NSUN_TAGK))).thenReturn(
        Deferred.<byte[]> fromError(nsunic));
    when(tag_names.getName(NSUI_TAGK))
      .thenThrow(new NoSuchUniqueId("tagk", NSUI_TAGK));
    when(tag_names.getNameAsync(NSUI_TAGK)).thenReturn(
        Deferred.<String>fromError(new NoSuchUniqueId("tagk", NSUI_TAGK)));
    
    for (final Map.Entry<String, byte[]> uid : UIDS.entrySet()) {
      mockUID(UniqueIdType.TAGK, uid.getKey(), uid.getValue());
    }
  }
  
  /** Adds the static UIDs to the tag values UID mock object */
  public void setupTagvMaps() {
    mockUID(UniqueIdType.TAGV, TAGV_STRING, TAGV_BYTES);
    mockUID(UniqueIdType.TAGV, TAGV_B_STRING, TAGV_B_BYTES);
    
    final NoSuchUniqueName nsun = new NoSuchUniqueName(NSUN_TAGV, "tagv");
    final NoSuchUniqueId nsui = new NoSuchUniqueId("tagv", NSUI_TAGV);
    when(tag_values.getId(NSUN_TAGV)).thenThrow(nsun);
    when(tag_values.getIdAsync(NSUN_TAGV))
        .thenReturn(Deferred.<byte[]> fromError(nsun));
    when(tag_values.getName(NSUI_TAGV)).thenThrow(nsui);
    when(tag_values.getNameAsync(NSUI_TAGV))
      .thenReturn(Deferred.<String>fromError(nsui));
    final NoSuchUniqueName nsunic = new NoSuchUniqueName(NSUN_TAGV, "tagv");
    when(tag_values.getOrCreateId(eq(NSUN_TAGV))).thenThrow(nsunic);
    when(tag_values.getOrCreateIdAsync(eq(NSUN_TAGV))).thenReturn(
        Deferred.<byte[]> fromError(nsunic));
    
    for (final Map.Entry<String, byte[]> uid : UIDS.entrySet()) {
      mockUID(UniqueIdType.TAGV, uid.getKey(), uid.getValue());
    }
  }

  /**
   * Helper method that sets up UIDs for rollup and pre-agg testing.
   */
  protected void setupGroupByTagValues() {
    // set the aggregate tag and value
    mockUID(UniqueIdType.TAGK, config.getString("tsd.rollups.agg_tag_key"),
        new byte[] { 0, 0, 42 });
    uid_map.put(config.getString("tsd.rollups.agg_tag_key"), new byte[] { 0, 0, 42 });
    mockUID(UniqueIdType.TAGV, config.getString("tsd.rollups.raw_agg_tag_value"),
        new byte[] { 0, 0, 42 });
    uid_map.put(config.getString("tsd.rollups.raw_agg_tag_value"), 
        new byte[] { 0, 0, 42 });
    mockUID(UniqueIdType.TAGV, "SUM", new byte[] { 0, 0, 43 });
    uid_map.put("SUM", new byte[] { 0, 0, 43 });
    mockUID(UniqueIdType.TAGV, "MAX", new byte[] { 0, 0, 44 });
    uid_map.put("MAX", new byte[] { 0, 0, 44 });
    mockUID(UniqueIdType.TAGV, "MIN", new byte[] { 0, 0, 45 });
    uid_map.put("MIN", new byte[] { 0, 0, 45 });
    mockUID(UniqueIdType.TAGV, "COUNT", new byte[] {  0, 0, 46 });
    uid_map.put("COUNT", new byte[] { 0, 0, 46 });
    mockUID(UniqueIdType.TAGV, "AVG", new byte[] { 0, 0, 47 });
    uid_map.put("AVG", new byte[] { 0, 0, 47 });
    mockUID(UniqueIdType.TAGV, "NOSUCHAGG", new byte[] { 0, 0, 0, 48 });
    uid_map.put("NOSUCHAGG", new byte[] { 0, 0, 48 });
  }
  
  // ----------------- //
  // Helper functions. //
  // ----------------- //
  
  /**
   * Mocks out the UID calls to match keys and values
   * 
   * @param type
   *          The type of UID to deal with
   * @param key
   *          The String name of the UID
   * @param uid
   *          The byte array UID to pair up with
   */
  protected void mockUID(final UniqueIdType type, final String key,
      final byte[] uid) {
    switch (type) {
    case METRIC:
      when(metrics.getId(key)).thenReturn(uid);
      when(metrics.getIdAsync(key))
        .thenAnswer(new Answer<Deferred<byte[]>>() {
          @Override
          public Deferred<byte[]> answer(InvocationOnMock invocation)
              throws Throwable {
            return Deferred.fromResult(uid);
          }
        });
      when(metrics.getOrCreateId(key)).thenReturn(uid);
      when(metrics.getOrCreateIdAsync(key))
          .thenAnswer(new Answer<Deferred<byte[]>>() {
        @Override
        public Deferred<byte[]> answer(InvocationOnMock invocation)
            throws Throwable {
          return Deferred.fromResult(uid);
        }
      });
      when(metrics.getName(uid)).thenReturn(key);
      when(metrics.getNameAsync(uid)).thenAnswer(new Answer<Deferred<String>>() {
        @Override
        public Deferred<String> answer(InvocationOnMock invocation)
            throws Throwable {
          return Deferred.fromResult(key);
        }
      });
      break;
    case TAGK:
      when(tag_names.getId(key)).thenReturn(uid);
      when(tag_names.getIdAsync(key))
        .thenAnswer(new Answer<Deferred<byte[]>>() {
          @Override
          public Deferred<byte[]> answer(InvocationOnMock invocation)
              throws Throwable {
            return Deferred.fromResult(uid);
          }
        });
      when(tag_names.getOrCreateId(key)).thenReturn(uid);
      when(tag_names.getOrCreateIdAsync(key))
          .thenAnswer(new Answer<Deferred<byte[]>>() {
        @Override
        public Deferred<byte[]> answer(InvocationOnMock invocation)
            throws Throwable {
          return Deferred.fromResult(uid);
        }
      });
      when(tag_names.getName(uid)).thenReturn(key);
      when(tag_names.getNameAsync(uid)).thenAnswer(new Answer<Deferred<String>>() {
        @Override
        public Deferred<String> answer(InvocationOnMock invocation)
            throws Throwable {
          return Deferred.fromResult(key);
        }
      });
      break;
    case TAGV:
      when(tag_values.getId(key)).thenReturn(uid);
      when(tag_values.getIdAsync(key))
        .thenAnswer(new Answer<Deferred<byte[]>>() {
          @Override
          public Deferred<byte[]> answer(InvocationOnMock invocation)
              throws Throwable {
            return Deferred.fromResult(uid);
          }
        });
      when(tag_values.getOrCreateId(key)).thenReturn(uid);
      when(tag_values.getOrCreateIdAsync(key))
          .thenAnswer(new Answer<Deferred<byte[]>>() {
          @Override
          public Deferred<byte[]> answer(InvocationOnMock invocation)
              throws Throwable {
            return Deferred.fromResult(uid);
          }
        });
      when(tag_values.getName(uid)).thenReturn(key);
      when(tag_values.getNameAsync(uid)).thenAnswer(new Answer<Deferred<String>>() {
          @Override
          public Deferred<String> answer(InvocationOnMock invocation)
              throws Throwable {
            return Deferred.fromResult(key);
          }
        });
      break;
    }
  }
  
  /** @return a row key template with the default metric and tags */
  protected byte[] getRowKeyTemplate() {
    return IncomingDataPoints.rowKeyTemplate(tsdb, METRIC_STRING, tags);
  }
  
  /**
   * Generates a proper key storage row key based on the metric, base time 
   * and tags. Adds salting when mocked properly.
   * @param metric A non-null byte array representing the metric.
   * @param base_time The base time for the row.
   * @param tags A non-null list of tag key/value pairs as UIDs.
   * @return A row key to check mock storage for.
   */
  public static byte[] getRowKey(final byte[] metric, final int base_time, 
      final byte[]... tags) {
    int tags_length = 0;
    for (final byte[] tag : tags) {
      tags_length += tag.length;
    }
    final byte[] key = new byte[Const.SALT_WIDTH() + metric.length + 
                                Const.TIMESTAMP_BYTES + tags_length];
    
    System.arraycopy(metric, 0, key, Const.SALT_WIDTH(), metric.length);
    System.arraycopy(Bytes.fromInt(base_time), 0, key, 
        Const.SALT_WIDTH() + metric.length, Const.TIMESTAMP_BYTES);
    int offset = Const.SALT_WIDTH() + metric.length + Const.TIMESTAMP_BYTES;
    for (final byte[] tag : tags) {
      System.arraycopy(tag, 0, key, offset, tag.length);
      offset += tag.length;
    }
    RowKey.prefixKeyWithSalt(key);
    return key;
  }
  
  /**
   * Generates a proper key storage row key based on the metric, base time 
   * and tags. Adds salting when mocked properly.
   * @param metric
   * @param base_time
   * @param tags
   * @return
   */
  protected byte[] getRowKey(final String metric, final int base_time, 
      final String... tags) {
    final int m = TSDB.metrics_width();
    final int tk = TSDB.tagk_width();
    final int tv = TSDB.tagv_width();
    
    final byte[] key = new byte[Const.SALT_WIDTH() + m + 4 
       + (tags.length / 2) * tk + (tags.length / 2) * tv];
    byte[] uid = uid_map.get(metric);
    
    // metrics first
    if (uid != null) {
      System.arraycopy(uid, 0, key, Const.SALT_WIDTH(), m);
    } else {
      throw new IllegalArgumentException("No METRIC UID was mocked for: " + metric);
    }
    
    // timestamp
    System.arraycopy(Bytes.fromInt(base_time), 0, key, Const.SALT_WIDTH() + m, 
        Const.TIMESTAMP_BYTES);
    
    // shortcut for offsets
    final int pl = Const.SALT_WIDTH() + m + Const.TIMESTAMP_BYTES;
    int ctr = 0;
    int offset = 0;
    for (final String tag : tags) {
      uid = uid_map.get(tag);
      
      if (ctr % 2 == 0) {
        // TAGK
        if (uid != null) {
          System.arraycopy(uid, 0, key, pl + offset, tk);
        } else {
          throw new IllegalArgumentException("No TAGK UID was mocked for: " + tag);
        }
        offset += tk;
      } else {
        // TAGV
        if (uid != null) {
          System.arraycopy(uid, 0, key, pl + offset, tv);
        } else {
          throw new IllegalArgumentException("No TAGK UID was mocked for: " + tag);
        }
        offset += tv;
      }
      
      ctr++;
    }
    
    RowKey.prefixKeyWithSalt(key);
    return key;
  }
  
  /**
   * Generates a TSUID given the metric and tag UIDs.
   * @param metric A metric UID.
   * @param tags A set of UIDs
   * @return A TSUID byte array
   */
  public static byte[] getTSUID(final byte[] metric, final byte[]... tags) {
    int tags_length = 0;
    for (final byte[] tag : tags) {
      tags_length += tag.length;
    }
    final byte[] tsuid = new byte[metric.length + tags_length];
    System.arraycopy(metric, 0, tsuid, 0, metric.length);
    int offset = metric.length;
    for (final byte[] tag : tags) {
      System.arraycopy(tag, 0, tsuid, offset, tag.length);
      offset += tag.length;
    }
    RowKey.prefixKeyWithSalt(tsuid);
    return tsuid;
  }
  
  /**
   * Generates a UID of the proper length given a type and ID. 
   * @param type The type of UID.
   * @param id The ID to set (just tweaks the last byte)
   * @return A Unique ID of the proper width.
   */
  public static byte[] generateUID(final UniqueIdType type, byte id) {
    final byte[] uid;
    switch (type) {
    case METRIC:
      uid = new byte[TSDB.metrics_width()];
      break;
    case TAGK:
      uid = new byte[TSDB.tagk_width()];
      break;
    case TAGV:
      uid = new byte[TSDB.tagv_width()];
      break;
    default:
      throw new IllegalArgumentException("Yo! You have to mock out " + type + "!");
    }
    uid[uid.length - 1] = id;
    return uid;
  }
  
  /**
   * Generates a UID of the proper length given a type and ID. 
   * @param type The type of UID.
   * @param id The ID to set (just tweaks the last byte)
   * @return A Unique ID of the proper width.
   */
  public static String generateUIDString(final UniqueIdType type, byte id) {
    return UniqueId.uidToString(generateUID(type, id));
  }
  
  /**
   * Generates a TSUID given the metric and tag UIDs.
   * @param metric A metric UID.
   * @param tags A set of UIDs
   * @return A TSUID as a hex string
   */
  public static String getTSUIDString(final byte[] metric, final byte[]... tags) {
    return UniqueId.uidToString(getTSUID(metric, tags));
  }
  
  /**
   * Generates a TSUID given the mocked UID strings.
   * @param metric A mocked metric name.
   * @param tags A set of mocked tag key and values.
   * @return A TSUID byte array
   */
  protected byte[] getTSUID(final String metric, final String... tags) {
    final int m = TSDB.metrics_width();
    final int tk = TSDB.tagk_width();
    final int tv = TSDB.tagv_width();
    
    final byte[] tsuid = new byte[m + (tags.length / 2) * tk + (tags.length / 2) * tv];
    byte[] uid = uid_map.get(metric);
    
    // metrics first
    if (uid != null) {
      System.arraycopy(uid, 0, tsuid, 0, m);
    } else {
      throw new IllegalArgumentException("No METRIC UID was mocked for: " + metric);
    }
    
    int ctr = 0;
    int offset = 0;
    for (final String tag : tags) {
      uid = uid_map.get(tag);
      
      if (ctr % 2 == 0) {
        // TAGK
        if (uid != null) {
          System.arraycopy(uid, 0, tsuid, m + offset, tk);
        } else {
          throw new IllegalArgumentException("No TAGK UID was mocked for: " + tag);
        }
        offset += tk;
      } else {
        // TAGV
        if (uid != null) {
          System.arraycopy(uid, 0, tsuid, m + offset, tv);
        } else {
          throw new IllegalArgumentException("No TAGK UID was mocked for: " + tag);
        }
        offset += tv;
      }
      
      ctr++;
    }
    
    return tsuid;
  }
  
  /**
   * Generates a TSUID given the mocked UID strings.
   * @param metric A mocked metric name.
   * @param tags A set of mocked tag key and values.
   * @return A TSUID hex string.
   */
  protected String getTSUIDString(final String metric, final String... tags) {
    return UniqueId.uidToString(getTSUID(metric, tags));
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
  
  //store histogram data points of {@link LongHistogramDataPointForTest} with second timestamp
  protected void storeTestHistogramTimeSeriesSeconds(final boolean offset) throws Exception {
    setDataPointStorage();
     
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags_local = new HashMap<String, String>();
    tags_local.put("host", "web01");
    
    // note that the mock must have been configured properly
    final int id = tsdb.histogramManager()
        .getCodec(LongHistogramDataPointForTestDecoder.class);
    
    long timestamp = 1356998400;
    for (int i = 1; i <= 300; i++) {
      final LongHistogramDataPointForTest hdp = 
          new LongHistogramDataPointForTest(id, i);
      tsdb.addHistogramPoint(HISTOGRAM_METRIC_STRING, timestamp += 30, 
          hdp.histogram(true), tags_local).joinUninterruptibly();
    }
  
    // dump a parallel set but invert the values
    tags_local.clear();
    tags_local.put("host", "web02");
    timestamp = offset ? 1356998415 : 1356998400;
    for (int i = 300; i > 0; i--) {
      final LongHistogramDataPointForTest hdp = 
          new LongHistogramDataPointForTest(id, i);
      tsdb.addHistogramPoint(HISTOGRAM_METRIC_STRING, timestamp += 30, 
          hdp.histogram(true), tags_local).joinUninterruptibly();
    }
  }
   
  // store histogram data points of {@link LongHistogramDataPointForTest} with ms timestamp
  protected void storeTestHistogramTimeSeriesMs() throws Exception {
    setDataPointStorage();
    
    // note that the mock must have been configured properly
    final int id = tsdb.histogramManager()
        .getCodec(LongHistogramDataPointForTestDecoder.class);
    
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400000L;
    for (int i = 1; i <= 300; i++) {
      timestamp += 500;
      final LongHistogramDataPointForTest hdp = 
          new LongHistogramDataPointForTest(id, i);
      tsdb.addHistogramPoint("msg.end2end.latency", timestamp, 
          hdp.histogram(true), tags).joinUninterruptibly();
    } // end for
   
    // dump a parallel set but invert the values
    tags.clear();
    tags.put("host", "web02");
    timestamp = 1356998400000L;
    for (int i = 300; i > 0; i--) {
      timestamp += 500;
      final LongHistogramDataPointForTest hdp = 
          new LongHistogramDataPointForTest(id, i);
      tsdb.addHistogramPoint("msg.end2end.latency", timestamp, 
          hdp.histogram(true), tags).joinUninterruptibly();
    } // end for
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

  /**
   * A fake {@link org.jboss.netty.util.Timer} implementation.
   * Instead of executing the task it will store that task in a internal state
   * and provides a function to start the execution of the stored task.
   * This implementation thus allows the flexibility of simulating the
   * things that will be going on during the time out period of a TimerTask.
   * This was mainly return to simulate the timeout period for
   * alreadyNSREdRegion test, where the region will be in the NSREd mode only
   * during this timeout period, which was difficult to simulate using the
   * above {@link FakeTimer} implementation, as we don't get back the control
   * during the timeout period
   *
   * Here it will hold at most two Tasks. We have two tasks here because when
   * one is being executed, it may call for newTimeOut for another task.
   */
  public static final class FakeTaskTimer extends HashedWheelTimer {

    public TimerTask newPausedTask = null;
    public TimerTask pausedTask = null;
    public Timeout timeout = null;

    @Override
    public synchronized Timeout newTimeout(final TimerTask task,
                                           final long delay,
                                           final TimeUnit unit) {
      if (pausedTask == null) {
        pausedTask = task;
      }  else if (newPausedTask == null) {
        newPausedTask = task;
      } else {
        throw new IllegalStateException("Cannot Pause Two Timer Tasks");
      }
      timeout = mock(Timeout.class);
      return timeout;
    }

    @Override
    public Set<Timeout> stop() {
      return null;
    }

    public boolean continuePausedTask() {
      if (pausedTask == null) {
        return false;
      }
      try {
        if (newPausedTask != null) {
          throw new IllegalStateException("Cannot be in this state");
        }
        pausedTask.run(null);  // Argument never used in this code base
        pausedTask = newPausedTask;
        newPausedTask = null;
        return true;
      } catch (Exception e) {
        throw new RuntimeException("Timer task failed: " + pausedTask, e);
      }
    }
  }

  /**
   * A little class used to throw a very specific type of exception for matching
   * in Unit Tests.
   */
  public static class UnitTestException extends RuntimeException {
    public UnitTestException() { }
    public UnitTestException(final String msg) {
      super(msg);
    }
    private static final long serialVersionUID = -4404095849459619922L;
  }
}