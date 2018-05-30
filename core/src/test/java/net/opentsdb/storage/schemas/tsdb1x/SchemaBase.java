// This file is part of OpenTSDB.
// Copyright (C) 2015-2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.storage.schemas.tsdb1x;

import static org.mockito.Matchers.anyString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import net.opentsdb.common.Const;
import net.opentsdb.configuration.UnitTestConfiguration;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.MockTrace;
import net.opentsdb.uid.LRUUniqueId;
import net.opentsdb.uid.MockUIDStore;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdFactory;
import net.opentsdb.uid.UniqueIdStore;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Bytes;

import org.junit.BeforeClass;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;

/**
 * The base Schema class to extend for tests that incorporate the schema.
 */
public class SchemaBase {
  
  public static final String METRIC_STRING = "sys.cpu.user";
  public static final byte[] METRIC_BYTES = new byte[] { 0, 0, 1 };
  public static final String METRIC_B_STRING = "sys.cpu.system";
  public static final byte[] METRIC_B_BYTES = new byte[] { 0, 0, 2 };
  public static final String NSUN_METRIC = "sys.cpu.nice";
  public static final byte[] NSUI_METRIC = new byte[] { 0, 0, 3 };
  public static final String METRIC_STRING_EX = "sys.cpu.idle";
  public static final byte[] METRIC_BYTES_EX = new byte[] { 0, 0, 4 };
  
  public static final String TAGK_STRING = "host";
  public static final byte[] TAGK_BYTES = new byte[] { 0, 0, 1 };
  public static final String TAGK_B_STRING = "owner";
  public static final byte[] TAGK_B_BYTES = new byte[] { 0, 0, 3 };
  public static final String NSUN_TAGK = "dc";
  public static final byte[] NSUI_TAGK = new byte[] { 0, 0, 4 };
  public static final String TAGK_STRING_EX = "colo";
  public static final byte[] TAGK_BYTES_EX = new byte[] { 0, 0, 5 };
  
  public static final String TAGV_STRING = "web01";
  public static final byte[] TAGV_BYTES = new byte[] { 0, 0, 1 };
  public static final String TAGV_B_STRING = "web02";
  public static final byte[] TAGV_B_BYTES = new byte[] { 0, 0, 2 };
  public static final String NSUN_TAGV = "web03";
  public static final byte[] NSUI_TAGV = new byte[] { 0, 0, 3 };
  public static final String TAGV_STRING_EX = "web04";
  public static final byte[] TAGV_BYTES_EX = new byte[] { 0, 0, 4 };

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
  
  public static MockTSDB tsdb;
  public static Tsdb1xDataStoreFactory store_factory;
  public static Tsdb1xDataStore store;
  public static UniqueIdStore uid_store;
  public static UniqueIdFactory uid_factory;
  public static UniqueId metrics;
  public static UniqueId tag_names;
  public static UniqueId tag_values;
  public static MockTrace trace;
  
  protected FakeTaskTimer timer;
  protected Map<String, String> tags;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    tsdb = new MockTSDB();
    store_factory = mock(Tsdb1xDataStoreFactory.class);
    store = mock(Tsdb1xDataStore.class);
    uid_store = spy(new MockUIDStore(Const.ISO_8859_CHARSET));
    uid_factory = mock(UniqueIdFactory.class);
    
    // return the default
    when(tsdb.registry.getDefaultPlugin(Tsdb1xDataStoreFactory.class))
      .thenReturn(store_factory);
    when(store_factory.newInstance(any(TSDB.class), anyString(), any(Schema.class)))
      .thenReturn(store);    
    when(tsdb.registry.getSharedObject("default_uidstore"))
      .thenReturn(uid_store);
    when(tsdb.registry.getPlugin(UniqueIdFactory.class, "LRU"))
      .thenReturn(uid_factory);
    
    metrics = new LRUUniqueId(tsdb, null, UniqueIdType.METRIC, uid_store);
    tag_names = new LRUUniqueId(tsdb, null, UniqueIdType.TAGK, uid_store);
    tag_values = new LRUUniqueId(tsdb, null, UniqueIdType.TAGV, uid_store);
    when(uid_factory.newInstance(any(TSDB.class), anyString(), 
        eq(UniqueIdType.METRIC), eq(uid_store))).thenReturn(metrics);
    when(uid_factory.newInstance(any(TSDB.class), anyString(), 
        eq(UniqueIdType.TAGK), eq(uid_store))).thenReturn(tag_names);
    when(uid_factory.newInstance(any(TSDB.class), anyString(), 
        eq(UniqueIdType.TAGV), eq(uid_store))).thenReturn(tag_values);
    
    
    ((MockUIDStore) uid_store).addBoth(UniqueIdType.METRIC, 
        METRIC_STRING, METRIC_BYTES);
    ((MockUIDStore) uid_store).addBoth(UniqueIdType.METRIC, 
        METRIC_B_STRING, METRIC_B_BYTES);
    
    ((MockUIDStore) uid_store).addBoth(UniqueIdType.TAGK, TAGK_STRING, TAGK_BYTES);
    ((MockUIDStore) uid_store).addBoth(UniqueIdType.TAGK, TAGK_B_STRING, TAGK_B_BYTES);
    
    ((MockUIDStore) uid_store).addBoth(UniqueIdType.TAGV, TAGV_STRING, TAGV_BYTES);
    ((MockUIDStore) uid_store).addBoth(UniqueIdType.TAGV, TAGV_B_STRING, TAGV_B_BYTES);
    
    ((MockUIDStore) uid_store).addException(UniqueIdType.METRIC, METRIC_STRING_EX);
    ((MockUIDStore) uid_store).addException(UniqueIdType.TAGK, TAGK_STRING_EX);
    ((MockUIDStore) uid_store).addException(UniqueIdType.TAGV, TAGV_STRING_EX);
    
    ((MockUIDStore) uid_store).addException(UniqueIdType.METRIC, METRIC_BYTES_EX);
    ((MockUIDStore) uid_store).addException(UniqueIdType.TAGK, TAGK_BYTES_EX);
    ((MockUIDStore) uid_store).addException(UniqueIdType.TAGV, TAGV_BYTES_EX);
    
    for (final Map.Entry<String, byte[]> uid : UIDS.entrySet()) {
      ((MockUIDStore) uid_store).addBoth(UniqueIdType.METRIC, uid.getKey(), uid.getValue());
      ((MockUIDStore) uid_store).addBoth(UniqueIdType.TAGK, uid.getKey(), uid.getValue());
      ((MockUIDStore) uid_store).addBoth(UniqueIdType.TAGV, uid.getKey(), uid.getValue());
    }
  }
  
  /**
   * Helper method that sets up UIDs for rollup and pre-agg testing.
   */
//  protected void setupGroupByTagValues() {
//    // set the aggregate tag and value
//    mockUID(UniqueIdType.TAGK, config.getString("tsd.rollups.agg_tag_key"),
//        new byte[] { 0, 0, 42 });
//    uid_map.put(config.getString("tsd.rollups.agg_tag_key"), new byte[] { 0, 0, 42 });
//    mockUID(UniqueIdType.TAGV, config.getString("tsd.rollups.raw_agg_tag_value"),
//        new byte[] { 0, 0, 42 });
//    uid_map.put(config.getString("tsd.rollups.raw_agg_tag_value"), 
//        new byte[] { 0, 0, 42 });
//    mockUID(UniqueIdType.TAGV, "SUM", new byte[] { 0, 0, 43 });
//    uid_map.put("SUM", new byte[] { 0, 0, 43 });
//    mockUID(UniqueIdType.TAGV, "MAX", new byte[] { 0, 0, 44 });
//    uid_map.put("MAX", new byte[] { 0, 0, 44 });
//    mockUID(UniqueIdType.TAGV, "MIN", new byte[] { 0, 0, 45 });
//    uid_map.put("MIN", new byte[] { 0, 0, 45 });
//    mockUID(UniqueIdType.TAGV, "COUNT", new byte[] {  0, 0, 46 });
//    uid_map.put("COUNT", new byte[] { 0, 0, 46 });
//    mockUID(UniqueIdType.TAGV, "AVG", new byte[] { 0, 0, 47 });
//    uid_map.put("AVG", new byte[] { 0, 0, 47 });
//    mockUID(UniqueIdType.TAGV, "NOSUCHAGG", new byte[] { 0, 0, 0, 48 });
//    uid_map.put("NOSUCHAGG", new byte[] { 0, 0, 48 });
//  }
  
  /** @return A schema instantiation mocked for use in Unit tests. */
  public Schema schema() throws Exception {
    resetConfig();
    metrics.dropCaches(null);
    tag_names.dropCaches(null);
    tag_values.dropCaches(null);
    return new Schema(tsdb, null);
  }
  
  /** Sets the UID widths and salt back to their defaults. */
  public static void resetConfig() throws Exception {
    final UnitTestConfiguration c = tsdb.config;
    if (c.hasProperty("tsd.storage.uid.width.metric")) {
      c.override("tsd.storage.uid.width.metric", 3);
    }
    if (c.hasProperty("tsd.storage.uid.width.tagk")) {
      c.override("tsd.storage.uid.width.tagk", 3);
    }
    if (c.hasProperty("tsd.storage.uid.width.tagv")) {
      c.override("tsd.storage.uid.width.tagv", 3);
    }
    if (c.hasProperty("tsd.storage.salt.buckets")) {
      c.override("tsd.storage.salt.buckets", 20);
    }
    if (c.hasProperty("tsd.storage.salt.width")) {
      c.override("tsd.storage.salt.width", 0);
    }
  }
  
  // ----------------- //
  // Helper functions. //
  // ----------------- //
  
  /** @return a row key template with the default metric and tags */
  protected byte[] getRowKeyTemplate() {
    // TODO
    return null;
    //return IncomingDataPoints.rowKeyTemplate(tsdb, METRIC_STRING, tags);
  }
  
  /**
   * Generates a proper key storage row key based on the metric, base time 
   * and tags. Adds salting when mocked properly.
   * 
   * @param schema The schema.
   * @param metric A non-null byte array representing the metric.
   * @param base_time The base time for the row.
   * @param tags A non-null list of tag key/value pairs as UIDs.
   * @return A row key to check mock storage for.
   */
  public static byte[] getRowKey(final Schema schema, 
                                 final byte[] metric, 
                                 final int base_time, 
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
    schema.prefixKeyWithSalt(key);
    return key;
  }
  
  /**
   * Generates a proper key storage row key based on the metric, base time 
   * and tags. Adds salting when mocked properly.
   * 
   * @param schema The schema.
   * @param metric A non-null byte array representing the metric.
   * @param base_time The base time for the row.
   * @param tags A non-null list of tag key/value pairs as UIDs.
   * @return A row key to check mock storage for.
   */
  protected byte[] getRowKey(final Schema schema, 
                             final String metric, 
                             final int base_time, 
                             final String... tags) throws Exception {
    final int m = schema.metricWidth();
    final int tk = schema.tagkWidth();
    final int tv = schema.tagvWidth();
    
    final byte[] key = new byte[Const.SALT_WIDTH() + m + 4 
       + (tags.length / 2) * tk + (tags.length / 2) * tv];
    byte[] uid = metrics.getId(metric, null).join();
    
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
      if (ctr % 2 == 0) {
        // TAGK
        uid = tag_names.getId(tag, null).join();
        if (uid != null) {
          System.arraycopy(uid, 0, key, pl + offset, tk);
        } else {
          throw new IllegalArgumentException("No TAGK UID was mocked for: " + tag);
        }
        offset += tk;
      } else {
        // TAGV
        uid = tag_values.getId(tag, null).join();
        if (uid != null) {
          System.arraycopy(uid, 0, key, pl + offset, tv);
        } else {
          throw new IllegalArgumentException("No TAGK UID was mocked for: " + tag);
        }
        offset += tv;
      }
      
      ctr++;
    }
    
    schema.prefixKeyWithSalt(key);
    return key;
  }
  
  /**
   * Generates a TSUID given the metric and tag UIDs.
   * 
   * @param schema The schema.
   * @param metric A metric UID.
   * @param tags A set of UIDs
   * @return A TSUID byte array
   */
  public static byte[] getTSUID(final Schema schema,
                                final byte[] metric, 
                                final byte[]... tags) {
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
    schema.prefixKeyWithSalt(tsuid);
    return tsuid;
  }
  
  /**
   * Generates a UID of the proper length given a type and ID. 
   * 
   * @param schema The schema.
   * @param type The type of UID.
   * @param id The ID to set (just tweaks the last byte)
   * @return A Unique ID of the proper width.
   */
  public static byte[] generateUID(final Schema schema,
                                   final UniqueIdType type, 
                                   byte id) {
    final byte[] uid;
    switch (type) {
    case METRIC:
      uid = new byte[schema.metricWidth()];
      break;
    case TAGK:
      uid = new byte[schema.tagkWidth()];
      break;
    case TAGV:
      uid = new byte[schema.tagvWidth()];
      break;
    default:
      throw new IllegalArgumentException("Yo! You have to mock out " + type + "!");
    }
    uid[uid.length - 1] = id;
    return uid;
  }
  
  /**
   * Generates a UID of the proper length given a type and ID. 
   * 
   * @param schema The schema.
   * @param type The type of UID.
   * @param id The ID to set (just tweaks the last byte)
   * @return A Unique ID of the proper width.
   */
  public static String generateUIDString(final Schema schema,
                                         final UniqueIdType type, 
                                         final byte id) {
    return UniqueId.uidToString(generateUID(schema, type, id));
  }
  
  /**
   * Generates a TSUID given the metric and tag UIDs.
   * 
   * @param schema The schema.
   * @param metric A metric UID.
   * @param tags A set of UIDs
   * @return A TSUID as a hex string
   */
  public static String getTSUIDString(final Schema schema,
                                      final byte[] metric, 
                                      final byte[]... tags) {
    return UniqueId.uidToString(getTSUID(schema, metric, tags));
  }
  
  /**
   * Generates a TSUID given the mocked UID strings.
   * 
   * @param schema The schema.
   * @param metric A mocked metric name.
   * @param tags A set of mocked tag key and values.
   * @return A TSUID byte array
   */
  protected byte[] getTSUID(final Schema schema,
                            final String metric, 
                            final String... tags) throws Exception {
    final int m = schema.metricWidth();
    final int tk = schema.tagkWidth();
    final int tv = schema.tagvWidth();
    
    final byte[] tsuid = new byte[m + (tags.length / 2) * tk + (tags.length / 2) * tv];
    byte[] uid = metrics.getId(metric, null).join();
    
    // metrics first
    if (uid != null) {
      System.arraycopy(uid, 0, tsuid, 0, m);
    } else {
      throw new IllegalArgumentException("No METRIC UID was mocked for: " + metric);
    }
    
    int ctr = 0;
    int offset = 0;
    for (final String tag : tags) {
      if (ctr % 2 == 0) {
        // TAGK
        uid = tag_names.getId(tag, null).join();
        if (uid != null) {
          System.arraycopy(uid, 0, tsuid, m + offset, tk);
        } else {
          throw new IllegalArgumentException("No TAGK UID was mocked for: " + tag);
        }
        offset += tk;
      } else {
        // TAGV
        uid = tag_values.getId(tag, null).join();
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
   * 
   * @param schema The schema.
   * @param metric A mocked metric name.
   * @param tags A set of mocked tag key and values.
   * @return A TSUID hex string.
   */
  protected String getTSUIDString(final Schema schema,
                                  final String metric, 
                                  final String... tags) throws Exception {
    return UniqueId.uidToString(getTSUID(schema, metric, tags));
  }
  
//  protected void setDataPointStorage() throws Exception {
//    storage = new MockBase(tsdb, client, true, true, true, true);
//    storage.setFamily("t".getBytes(MockBase.ASCII()));
//  }
//  
//  protected void storeLongTimeSeriesSeconds(final boolean two_metrics, 
//      final boolean offset) throws Exception {
//    setDataPointStorage();
//    
//    // dump a bunch of rows of two metrics so that we can test filtering out
//    // on the metric
//    HashMap<String, String> tags_local = new HashMap<String, String>(tags);
//    long timestamp = 1356998400;
//    for (int i = 1; i <= 300; i++) {
//      tsdb.addPoint(METRIC_STRING, timestamp += 30, i, tags_local)
//        .joinUninterruptibly();
//      if (two_metrics) {
//        tsdb.addPoint(METRIC_B_STRING, timestamp, i, tags_local)
//          .joinUninterruptibly();
//      }
//    }
//
//    // dump a parallel set but invert the values
//    tags_local.clear();
//    tags_local.put(TAGK_STRING, TAGV_B_STRING);
//    timestamp = offset ? 1356998415 : 1356998400;
//    for (int i = 300; i > 0; i--) {
//      tsdb.addPoint(METRIC_STRING, timestamp += 30, i, tags_local)
//        .joinUninterruptibly();
//      if (two_metrics) {
//        tsdb.addPoint(METRIC_B_STRING, timestamp, i, tags_local)
//          .joinUninterruptibly();
//      }
//    }
//  }
// 
//  protected void storeLongTimeSeriesMs() throws Exception {
//    setDataPointStorage();
//    // dump a bunch of rows of two metrics so that we can test filtering out
//    // on the metric
//    HashMap<String, String> tags_local = new HashMap<String, String>(tags);
//    long timestamp = 1356998400000L;
//    for (int i = 1; i <= 300; i++) {
//      tsdb.addPoint(METRIC_STRING, timestamp += 500, i, tags_local)
//        .joinUninterruptibly();
//      tsdb.addPoint(METRIC_B_STRING, timestamp, i, tags_local)
//        .joinUninterruptibly();
//    }
//
//    // dump a parallel set but invert the values
//    tags_local.clear();
//    tags_local.put(TAGK_STRING, TAGV_B_STRING);
//    timestamp = 1356998400000L;
//    for (int i = 300; i > 0; i--) {
//      tsdb.addPoint(METRIC_STRING, timestamp += 500, i, tags_local)
//        .joinUninterruptibly();
//      tsdb.addPoint(METRIC_B_STRING, timestamp, i, tags_local)
//        .joinUninterruptibly();
//    }
//  }
//  
//  /**
//   * Create two metrics with same name, skipping every third point in host=web01
//   * and every other point in host=web02. To wit:
//   *
//   *       METRIC    TAG  t0   t1   t2   t3   t4   t5   ...
//   * sys.cpu.user  web01   X    2    3    X    5    6   ...
//   * sys.cpu.user  web02   X  299    X  297    X  295   ...
//   */
//  protected void storeLongTimeSeriesWithMissingData() throws Exception {
//    setDataPointStorage();
//
//    // host=web01
//    HashMap<String, String> tags_local = new HashMap<String, String>(tags);
//    long timestamp = 1356998400L;
//    for (int i = 0; i < 300; ++i) {
//      // Skip every third point.
//      if (0 != (i % 3)) {
//        tsdb.addPoint(METRIC_STRING, timestamp, i + 1, tags_local)
//          .joinUninterruptibly();
//      }
//      timestamp += 10L;
//    }
//
//    // host=web02
//    tags_local.clear();
//    tags_local.put(TAGK_STRING, TAGV_B_STRING);
//    timestamp = 1356998400L;
//    for (int i = 300; i > 0; --i) {
//      // Skip every other point.
//      if (0 != (i % 2)) {
//        tsdb.addPoint(METRIC_STRING, timestamp, i, tags_local)
//          .joinUninterruptibly();
//      }
//      timestamp += 10L;
//    }
//  }
//  
//  protected void storeFloatTimeSeriesSeconds(final boolean two_metrics, 
//      final boolean offset) throws Exception {
//    setDataPointStorage();
//    // dump a bunch of rows of two metrics so that we can test filtering out
//    // on the metric
//    HashMap<String, String> tags_local = new HashMap<String, String>(tags);
//    long timestamp = 1356998400;
//    for (float i = 1.25F; i <= 76; i += 0.25F) {
//      tsdb.addPoint(METRIC_STRING, timestamp += 30, i, tags_local)
//        .joinUninterruptibly();
//      if (two_metrics) {
//        tsdb.addPoint(METRIC_B_STRING, timestamp, i, tags_local)
//          .joinUninterruptibly();
//      }
//    }
//
//    // dump a parallel set but invert the values
//    tags_local.clear();
//    tags_local.put(TAGK_STRING, TAGV_B_STRING);
//    timestamp = offset ? 1356998415 : 1356998400;
//    for (float i = 75F; i > 0; i -= 0.25F) {
//      tsdb.addPoint(METRIC_STRING, timestamp += 30, i, tags_local)
//        .joinUninterruptibly();
//      if (two_metrics) {
//        tsdb.addPoint(METRIC_B_STRING, timestamp, i, tags_local)
//          .joinUninterruptibly();
//      }
//    }
//  }
//  
//  protected void storeFloatTimeSeriesMs() throws Exception {
//    setDataPointStorage();
//    // dump a bunch of rows of two metrics so that we can test filtering out
//    // on the metric
//    HashMap<String, String> tags_local = new HashMap<String, String>(tags);
//    long timestamp = 1356998400000L;
//    for (float i = 1.25F; i <= 76; i += 0.25F) {
//      tsdb.addPoint(METRIC_STRING, timestamp += 500, i, tags_local)
//        .joinUninterruptibly();
//      tsdb.addPoint(METRIC_B_STRING, timestamp, i, tags_local)
//        .joinUninterruptibly();
//    }
//
//    // dump a parallel set but invert the values
//    tags_local.clear();
//    tags_local.put(TAGK_STRING, TAGV_B_STRING);
//    timestamp = 1356998400000L;
//    for (float i = 75F; i > 0; i -= 0.25F) {
//      tsdb.addPoint(METRIC_STRING, timestamp += 500, i, tags_local)
//        .joinUninterruptibly();
//      tsdb.addPoint(METRIC_B_STRING, timestamp, i, tags_local)
//        .joinUninterruptibly();
//    }
//  }
//  
//  protected void storeMixedTimeSeriesSeconds() throws Exception {
//    setDataPointStorage();
//    HashMap<String, String> tags_local = new HashMap<String, String>(tags);
//    long timestamp = 1356998400;
//    for (float i = 1.25F; i <= 76; i += 0.25F) {
//      if (i % 2 == 0) {
//        tsdb.addPoint(METRIC_STRING, timestamp += 30, (long)i, tags_local)
//          .joinUninterruptibly();
//      } else {
//        tsdb.addPoint(METRIC_STRING, timestamp += 30, i, tags_local)
//          .joinUninterruptibly();
//      }
//    }
//  }
//  
//  // dumps ints, floats, seconds and ms
//  protected void storeMixedTimeSeriesMsAndS() throws Exception {
//    setDataPointStorage();
//    HashMap<String, String> tags_local = new HashMap<String, String>(tags);
//    long timestamp = 1356998400000L;
//    for (float i = 1.25F; i <= 76; i += 0.25F) {
//      long ts = timestamp += 500;
//      if (ts % 1000 == 0) {
//        ts /= 1000;
//      }
//      if (i % 2 == 0) {
//        tsdb.addPoint(METRIC_STRING, ts, (long)i, tags_local).joinUninterruptibly();
//      } else {
//        tsdb.addPoint(METRIC_STRING, ts, i, tags_local).joinUninterruptibly();
//      }
//    }
//  }
//  
//  //store histogram data points of {@link LongHistogramDataPointForTest} with second timestamp
//  protected void storeTestHistogramTimeSeriesSeconds(final boolean offset) throws Exception {
//    setDataPointStorage();
//     
//    // dump a bunch of rows of two metrics so that we can test filtering out
//    // on the metric
//    HashMap<String, String> tags_local = new HashMap<String, String>();
//    tags_local.put("host", "web01");
//    
//    // note that the mock must have been configured properly
//    final int id = tsdb.histogramManager()
//        .getCodec(LongHistogramDataPointForTestDecoder.class);
//    
//    long timestamp = 1356998400;
//    for (int i = 1; i <= 300; i++) {
//      final LongHistogramDataPointForTest hdp = 
//          new LongHistogramDataPointForTest(id, i);
//      tsdb.addHistogramPoint(HISTOGRAM_METRIC_STRING, timestamp += 30, 
//          hdp.histogram(true), tags_local).joinUninterruptibly();
//    }
//  
//    // dump a parallel set but invert the values
//    tags_local.clear();
//    tags_local.put("host", "web02");
//    timestamp = offset ? 1356998415 : 1356998400;
//    for (int i = 300; i > 0; i--) {
//      final LongHistogramDataPointForTest hdp = 
//          new LongHistogramDataPointForTest(id, i);
//      tsdb.addHistogramPoint(HISTOGRAM_METRIC_STRING, timestamp += 30, 
//          hdp.histogram(true), tags_local).joinUninterruptibly();
//    }
//  }
//   
//  // store histogram data points of {@link LongHistogramDataPointForTest} with ms timestamp
//  protected void storeTestHistogramTimeSeriesMs() throws Exception {
//    setDataPointStorage();
//    
//    // note that the mock must have been configured properly
//    final int id = tsdb.histogramManager()
//        .getCodec(LongHistogramDataPointForTestDecoder.class);
//    
//    // dump a bunch of rows of two metrics so that we can test filtering out
//    // on the metric
//    HashMap<String, String> tags = new HashMap<String, String>(1);
//    tags.put("host", "web01");
//    long timestamp = 1356998400000L;
//    for (int i = 1; i <= 300; i++) {
//      timestamp += 500;
//      final LongHistogramDataPointForTest hdp = 
//          new LongHistogramDataPointForTest(id, i);
//      tsdb.addHistogramPoint("msg.end2end.latency", timestamp, 
//          hdp.histogram(true), tags).joinUninterruptibly();
//    } // end for
//   
//    // dump a parallel set but invert the values
//    tags.clear();
//    tags.put("host", "web02");
//    timestamp = 1356998400000L;
//    for (int i = 300; i > 0; i--) {
//      timestamp += 500;
//      final LongHistogramDataPointForTest hdp = 
//          new LongHistogramDataPointForTest(id, i);
//      tsdb.addHistogramPoint("msg.end2end.latency", timestamp, 
//          hdp.histogram(true), tags).joinUninterruptibly();
//    } // end for
//  }
//  
//  /**
//   * Validates the metric name, tags and annotations
//   * @param dps The datapoints array returned from the query
//   * @param index The index to peek into the array
//   * @param agged_tags Whether or not the tags were aggregated out
//   */
//  protected void assertMeta(final DataPoints[] dps, final int index, 
//      final boolean agged_tags) {
//    assertMeta(dps, index, agged_tags, false);
//  }
//  
//  /**
//   * Validates the metric name, tags and annotations
//   * @param dps The datapoints array returned from the query
//   * @param index The index to peek into the array
//   * @param agged_tags Whether or not the tags were aggregated out
//   * @param annotation Whether we're expecting a note or not
//   */
//  protected void assertMeta(final DataPoints[] dps, final int index, 
//      final boolean agged_tags, final boolean annotation) {
//    assertNotNull(dps);
//    assertEquals(METRIC_STRING, dps[index].metricName());
//    
//    if (agged_tags) {
//      assertTrue(dps[index].getTags().isEmpty());
//      assertEquals(TAGK_STRING, dps[index].getAggregatedTags().get(0));
//    } else {
//      if (index == 0) {
//        assertTrue(dps[index].getAggregatedTags().isEmpty());
//        assertEquals(TAGV_STRING, dps[index].getTags().get(TAGK_STRING));
//      } else {
//        assertEquals(TAGV_B_STRING, dps[index].getTags().get(TAGK_STRING));
//      }
//    }
//    
//    if (annotation) {
//      assertEquals(1, dps[index].getAnnotations().size());
//      assertEquals(NOTE_DESCRIPTION, dps[index].getAnnotations().get(0)
//          .getDescription());
//      assertEquals(NOTE_NOTES, dps[index].getAnnotations().get(0).getNotes());
//    } else {
//      assertNull(dps[index].getAnnotations());
//    }
//  }
//
//  /**
//   * Stores a single annotation in the given row
//   * @param timestamp The time to store the data point at
//   * @throws Exception
//   */
//  protected void storeAnnotation(final long timestamp) throws Exception {
//    final Annotation note = new Annotation();
//    note.setTSUID("000001000001000001");
//    note.setStartTime(timestamp);
//    note.setDescription(NOTE_DESCRIPTION);
//    note.setNotes(NOTE_NOTES);
//    note.syncToStorage(tsdb, false).joinUninterruptibly();
//  }

  static void verifySpan(final String name) {
    verifySpan(name, 1);
  }
  
  static void verifySpan(final String name, final int spans) {
    assertEquals(spans, trace.spans.size());
    assertEquals(name, trace.spans.get(spans - 1).id);
    assertEquals("OK", trace.spans.get(spans - 1).tags.get("status"));
  }
  
  static void verifySpan(final String name, final Class<?> ex) {
    verifySpan(name, ex, 1);
  }
  
  static void verifySpan(final String name, final Class<?> ex, final int size) {
    assertEquals(size, trace.spans.size());
    assertEquals(name, trace.spans.get(size - 1).id);
    assertEquals("Error", trace.spans.get(size - 1).tags.get("status"));
    System.out.println(trace.spans.get(size - 1).exceptions.get("Exception"));
    assertTrue(ex.isInstance(trace.spans.get(size - 1).exceptions.get("Exception")));
  }

  /**
   * A fake {@link io.netty.util.Timer} implementation.
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

}