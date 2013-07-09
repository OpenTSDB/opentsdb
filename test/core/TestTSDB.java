// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Field;
import java.util.HashMap;

import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
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
  CompactionQueue.class, GetRequest.class, PutRequest.class, KeyValue.class, 
  Scanner.class, AtomicIncrementRequest.class})
public final class TestTSDB {
  private Config config;
  private TSDB tsdb = null;
  private HBaseClient client = mock(HBaseClient.class);
  private UniqueId metrics = mock(UniqueId.class);
  private UniqueId tag_names = mock(UniqueId.class);
  private UniqueId tag_values = mock(UniqueId.class);
  private CompactionQueue compactionq = mock(CompactionQueue.class);
  private MockBase storage;
  
  @Before
  public void before() throws Exception {
    config = new Config(false);
    tsdb = new TSDB(config);
    
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
    
    Field cq = tsdb.getClass().getDeclaredField("compactionq");
    cq.setAccessible(true);
    cq.set(tsdb, compactionq);
  }
  
  @Test
  public void initializePluginsDefaults() {
    // no configured plugin path, plugins disabled, no exceptions
    tsdb.initializePlugins(true);
  }
  
  @Test
  public void initializePluginsPathSet() throws Exception {
    Field properties = config.getClass().getDeclaredField("properties");
    properties.setAccessible(true);
    @SuppressWarnings("unchecked")
    HashMap<String, String> props = 
      (HashMap<String, String>) properties.get(config);
    props.put("tsd.core.plugin_path", "./");
    properties.setAccessible(false);
    tsdb.initializePlugins(true);
  }
  
  @Test (expected = RuntimeException.class)
  public void initializePluginsPathBad() throws Exception {
    Field properties = config.getClass().getDeclaredField("properties");
    properties.setAccessible(true);
    @SuppressWarnings("unchecked")
    HashMap<String, String> props = 
      (HashMap<String, String>) properties.get(config);
    props.put("tsd.core.plugin_path", "./doesnotexist");
    properties.setAccessible(false);
    tsdb.initializePlugins(true);
  }
  
  @Test
  public void initializePluginsSearch() throws Exception {
    Field properties = config.getClass().getDeclaredField("properties");
    properties.setAccessible(true);
    @SuppressWarnings("unchecked")
    HashMap<String, String> props = 
      (HashMap<String, String>) properties.get(config);
    props.put("tsd.core.plugin_path", "./");
    props.put("tsd.search.enable", "true");
    props.put("tsd.search.plugin", "net.opentsdb.search.DummySearchPlugin");
    props.put("tsd.search.DummySearchPlugin.hosts", "localhost");
    props.put("tsd.search.DummySearchPlugin.port", "42");
    properties.setAccessible(false);
    tsdb.initializePlugins(true);
  }
  
  @Test (expected = RuntimeException.class)
  public void initializePluginsSearchNotFound() throws Exception {
    Field properties = config.getClass().getDeclaredField("properties");
    properties.setAccessible(true);
    @SuppressWarnings("unchecked")
    HashMap<String, String> props = 
      (HashMap<String, String>) properties.get(config);
    props.put("tsd.search.enable", "true");
    props.put("tsd.search.plugin", "net.opentsdb.search.DoesNotExist");
    properties.setAccessible(false);
    tsdb.initializePlugins(true);
  }
  
  @Test
  public void getClient() {
    assertNotNull(tsdb.getClient());
  }
  
  @Test
  public void getConfig() {
    assertNotNull(tsdb.getConfig());
  }
  
  @Test
  public void getUidNameMetric() throws Exception {
    setGetUidName();
    assertEquals("sys.cpu.0", tsdb.getUidName(UniqueIdType.METRIC, 
        new byte[] { 0, 0, 1 }).joinUninterruptibly());
  }
  
  @Test
  public void getUidNameTagk() throws Exception {
    setGetUidName();
    assertEquals("host", tsdb.getUidName(UniqueIdType.TAGK, 
        new byte[] { 0, 0, 1 }).joinUninterruptibly());
  }
  
  @Test
  public void getUidNameTagv() throws Exception {
    setGetUidName();
    assertEquals("web01", tsdb.getUidName(UniqueIdType.TAGV, 
        new byte[] { 0, 0, 1 }).joinUninterruptibly());
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void getUidNameMetricNSU() throws Exception {
    setGetUidName();
    tsdb.getUidName(UniqueIdType.METRIC, new byte[] { 0, 0, 2 })
    .joinUninterruptibly();
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void getUidNameTagkNSU() throws Exception {
    setGetUidName();
    tsdb.getUidName(UniqueIdType.TAGK, new byte[] { 0, 0, 2 })
    .joinUninterruptibly();
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void getUidNameTagvNSU() throws Exception {
    setGetUidName();
    tsdb.getUidName(UniqueIdType.TAGV, new byte[] { 0, 0, 2 })
    .joinUninterruptibly();
  }
  
  @Test (expected = NullPointerException.class)
  public void getUidNameNullType() throws Exception {
    setGetUidName();
    tsdb.getUidName(null, new byte[] { 0, 0, 2 }).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getUidNameNullUID() throws Exception {
    setGetUidName();
    tsdb.getUidName(UniqueIdType.TAGV, null).joinUninterruptibly();
  }
  
  @Test
  public void getUIDMetric() {
    setupAssignUid();
    assertArrayEquals(new byte[] { 0, 0, 1 }, 
        tsdb.getUID(UniqueIdType.METRIC, "sys.cpu.0"));
  }
  
  @Test
  public void getUIDTagk() {
    setupAssignUid();
    assertArrayEquals(new byte[] { 0, 0, 1 }, 
        tsdb.getUID(UniqueIdType.TAGK, "host"));
  }
  
  @Test
  public void getUIDTagv() {
    setupAssignUid();
    assertArrayEquals(new byte[] { 0, 0, 1 }, 
        tsdb.getUID(UniqueIdType.TAGV, "localhost"));
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void getUIDMetricNSU() {
    setupAssignUid();
    tsdb.getUID(UniqueIdType.METRIC, "sys.cpu.1");
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void getUIDTagkNSU() {
    setupAssignUid();
    tsdb.getUID(UniqueIdType.TAGK, "datacenter");
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void getUIDTagvNSU() {
    setupAssignUid();
    tsdb.getUID(UniqueIdType.TAGV, "myserver");
  }
  
  @Test (expected = NullPointerException.class)
  public void getUIDNullType() {
    setupAssignUid();
    tsdb.getUID(null, "sys.cpu.1");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getUIDNullName() {
    setupAssignUid();
    tsdb.getUID(UniqueIdType.TAGV, null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getUIDEmptyName() {
    setupAssignUid();
    tsdb.getUID(UniqueIdType.TAGV, "");
  }
  
  @Test
  public void assignUidMetric() {
    setupAssignUid();
    assertArrayEquals(new byte[] { 0, 0, 2 }, 
        tsdb.assignUid("metric", "sys.cpu.1"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidMetricExists() {
    setupAssignUid();
    tsdb.assignUid("metric", "sys.cpu.0");
  }
  
  @Test
  public void assignUidTagk() {
    setupAssignUid();
    assertArrayEquals(new byte[] { 0, 0, 2 }, 
        tsdb.assignUid("tagk", "datacenter"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidTagkExists() {
    setupAssignUid();
    tsdb.assignUid("tagk", "host");
  }
  
  @Test
  public void assignUidTagv() {
    setupAssignUid();
    assertArrayEquals(new byte[] { 0, 0, 2 }, 
        tsdb.assignUid("tagv", "myserver"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidTagvExists() {
    setupAssignUid();
    tsdb.assignUid("tagv", "localhost");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidBadType() {
    setupAssignUid();
    tsdb.assignUid("nothere", "localhost");
  }
  
  @Test (expected = NullPointerException.class)
  public void assignUidNullType() {
    setupAssignUid();
    tsdb.assignUid(null, "localhost");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidNullName() {
    setupAssignUid();
    tsdb.assignUid("metric", null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidInvalidCharacter() {
    setupAssignUid();
    tsdb.assignUid("metric", "Not!A:Valid@Name");
  }
  
  @Test
  public void uidTable() {
    assertNotNull(tsdb.uidTable());
    assertArrayEquals("tsdb-uid".getBytes(), tsdb.uidTable());
  }

  @Test
  public void addPointLong() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 7 });
    assertNotNull(value);
    assertEquals(42, Bytes.getLong(value));
  }
  
  @Test
  public void addPointLongMany() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400;
    for (int i = 1; i <= 50; i++) {
      tsdb.addPoint("sys.cpu.user", timestamp++, i, tags).joinUninterruptibly();
    }
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 7 });
    assertNotNull(value);
    assertEquals(1, Bytes.getLong(value));
    assertEquals(50, storage.numColumns(row));
  }
  
  @Test
  public void addPointLongEndOfRow() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1357001999, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { (byte) 0xE0, 
        (byte) 0xF7 });
    assertNotNull(value);
    assertEquals(42, Bytes.getLong(value));
  }
  
  @Test
  public void addPointLongOverwrite() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400, 42, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", 1356998400, 24, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 7 });
    assertNotNull(value);
    assertEquals(24, Bytes.getLong(value));
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void addPointNoAutoMetric() throws Exception {
    setupAddPointStorage();
    when(metrics.getId("sys.cpu.user"))
    .thenThrow(new NoSuchUniqueName("sys.cpu.user", "metric"));
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400, 42, tags).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addPointInvalidTimestampNegative() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", -1, 42, tags).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addPointInvalidTimestamp() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 4294967296L, 42, tags).joinUninterruptibly();
  }
  
  @Test
  public void addPointFloat() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400, 42.5F, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test
  public void addPointFloatEndOfRow() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1357001999, 42.5F, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { (byte) 0xE0, 
        (byte) 0xFB });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test
  public void addPointFloatPrecision() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400, 42.5123459999F, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.512345F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test
  public void addPointFloatOverwrite() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400, 42.5F, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", 1356998400, 25.4F, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(25.4F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test
  public void addPointBothSameTime() throws Exception {
    // this is an odd situation that can occur if the user puts an int and then
    // a float (or vice-versa) with the same timestamp. What happens in the
    // aggregators when this occurs? 
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400, 42, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", 1356998400, 42.5F, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    byte[] value = storage.getColumn(row, new byte[] { 0, 7 });
    assertEquals(2, storage.numColumns(row));
    assertNotNull(value);
    assertEquals(42, Bytes.getLong(value));
    value = storage.getColumn(row, new byte[] { 0, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  /**
   * Helper to mock the UID caches with valid responses
   */
  private void setupAssignUid() {
    when(metrics.getId("sys.cpu.0")).thenReturn(new byte[] { 0, 0, 1 });
    when(metrics.getId("sys.cpu.1")).thenThrow(
        new NoSuchUniqueName("metric", "sys.cpu.1"));
    when(metrics.getOrCreateId("sys.cpu.1")).thenReturn(new byte[] { 0, 0, 2 });
    
    when(tag_names.getId("host")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getId("datacenter")).thenThrow(
        new NoSuchUniqueName("tagk", "datacenter"));
    when(tag_names.getOrCreateId("datacenter")).thenReturn(new byte[] { 0, 0, 2 });
    
    when(tag_values.getId("localhost")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getId("myserver")).thenThrow(
        new NoSuchUniqueName("tagv", "myserver"));
    when(tag_values.getOrCreateId("myserver")).thenReturn(new byte[] { 0, 0, 2 });
  }
  
  /**
   * Helper to mock the UID caches with valid responses
   */
  private void setGetUidName() {
    when(metrics.getNameAsync(new byte[] { 0, 0, 1 }))
      .thenReturn(Deferred.fromResult("sys.cpu.0"));
    when(metrics.getNameAsync(new byte[] { 0, 0, 2 })).thenThrow(
        new NoSuchUniqueId("metric", new byte[] { 0, 0, 2}));
    
    when(tag_names.getNameAsync(new byte[] { 0, 0, 1 }))
      .thenReturn(Deferred.fromResult("host"));
    when(tag_names.getNameAsync(new byte[] { 0, 0, 2 })).thenThrow(
        new NoSuchUniqueId("tagk", new byte[] { 0, 0, 2}));
    
    when(tag_values.getNameAsync(new byte[] { 0, 0, 1 }))
      .thenReturn(Deferred.fromResult("web01"));
    when(tag_values.getNameAsync(new byte[] { 0, 0, 2 })).thenThrow(
        new NoSuchUniqueId("tag_values", new byte[] { 0, 0, 2}));
  }

  /**
   * Configures storage for the addPoint() tests to validate that we're storing
   * data points correctly.
   */
  private void setupAddPointStorage() throws Exception {
    storage = new MockBase(tsdb, client, true, true, true, true);
    
    // mock UniqueId
    when(metrics.getId("sys.cpu.user"))
      .thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getOrCreateId("host"))
      .thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getOrCreateId("web01"))
      .thenReturn(new byte[] { 0, 0, 1 });
    
    when(metrics.width()).thenReturn((short)3);
    when(tag_names.width()).thenReturn((short)3);
    when(tag_values.width()).thenReturn((short)3);
  }
}
