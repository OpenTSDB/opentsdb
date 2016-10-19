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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;

import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class, 
  CompactionQueue.class, GetRequest.class, PutRequest.class, KeyValue.class, 
  Scanner.class, AtomicIncrementRequest.class, Const.class})
public final class TestTSDB extends BaseTsdbTest {
  private MockBase storage;
  
  @Before
  public void beforeLocal() throws Exception {
    config.setFixDuplicates(true); // TODO(jat): test both ways
  }
  
  @Test
  public void ctorNullClient() throws Exception {
    assertNotNull(new TSDB(null, config));
  }
  
  @Test (expected = NullPointerException.class)
  public void ctorNullConfig() throws Exception {
    new TSDB(client, null);
  }
  
  @Test
  public void ctorRollups() throws Exception {
    
    TSDB tsdb = new TSDB(client, config);
    assertNull(Whitebox.getInternalState(tsdb, "rollup_config"));
    assertNull(Whitebox.getInternalState(tsdb, "default_interval"));
    
    List<RollupInterval> intervals = Lists.newArrayList(
        new RollupInterval("tsdb", "tsdb-agg", "1m", "1h", true));
    RollupConfig rollups = new RollupConfig(intervals);
    PowerMockito.whenNew(RollupConfig.class).withAnyArguments()
      .thenReturn(rollups);
    
    config.overrideConfig("tsd.rollups.enable", "true");
    tsdb = new TSDB(client, config);
    assertSame(rollups, Whitebox.getInternalState(tsdb, "rollup_config"));
    assertSame(intervals.get(0), Whitebox.getInternalState(tsdb, 
        "default_interval"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorRollupsNoDefault() throws Exception {
    // no default
    List<RollupInterval> intervals = Lists.newArrayList(
        new RollupInterval("tsdb", "tsdb-agg", "1m", "1h"));
    RollupConfig rollups = new RollupConfig(intervals);
    PowerMockito.whenNew(RollupConfig.class).withAnyArguments()
      .thenReturn(rollups);
    
    config.overrideConfig("tsd.rollups.enable", "true");
    new TSDB(client, config);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorRollupsEmpty() throws Exception {
    // no default
    List<RollupInterval> intervals = Lists.newArrayList();
    RollupConfig rollups = new RollupConfig(intervals);
    PowerMockito.whenNew(RollupConfig.class).withAnyArguments()
      .thenReturn(rollups);
    
    config.overrideConfig("tsd.rollups.enable", "true");
    new TSDB(client, config);
  }
  
  @Test
  public void ctorOverrideUIDWidths() throws Exception {
    // assert defaults
    assertEquals(3, TSDB.metrics_width());
    assertEquals(3, TSDB.tagk_width());
    assertEquals(3, TSDB.tagv_width());
    
    config.overrideConfig("tsd.storage.uid.width.metric", "1");
    config.overrideConfig("tsd.storage.uid.width.tagk", "4");
    config.overrideConfig("tsd.storage.uid.width.tagv", "5");
    final TSDB tsdb = new TSDB(client, config);
    assertEquals(1, TSDB.metrics_width());
    assertEquals(4, TSDB.tagk_width());
    assertEquals(5, TSDB.tagv_width());
    assertEquals(1, tsdb.metrics.width());
    assertEquals(4, tsdb.tag_names.width());
    assertEquals(5, tsdb.tag_values.width());
    
    // IMPORTANT Restore
    config.overrideConfig("tsd.storage.uid.width.metric", "3");
    config.overrideConfig("tsd.storage.uid.width.tagk", "3");
    config.overrideConfig("tsd.storage.uid.width.tagv", "3");
    new TSDB(client, config);
  }

  @Test
  public void ctorOverrideMaxNumTags() throws Exception {
    assertEquals(8, Const.MAX_NUM_TAGS());

    config.overrideConfig("tsd.storage.max_tags", "12");
    new TSDB(client, config);
    assertEquals(12, Const.MAX_NUM_TAGS());

    // IMPORTANT Restore
    config.overrideConfig("tsd.storage.max_tags", "8");
    new TSDB(client, config);
  }

  @Test
  public void ctorOverrideSalt() throws Exception {
    assertEquals(20, Const.SALT_BUCKETS());
    assertEquals(0, Const.SALT_WIDTH());
    
    config.overrideConfig("tsd.storage.salt.buckets", "15");
    config.overrideConfig("tsd.storage.salt.width", "2");
    new TSDB(client, config);
    assertEquals(15, Const.SALT_BUCKETS());
    assertEquals(2, Const.SALT_WIDTH());
    
    // IMPORTANT Restore
    config.overrideConfig("tsd.storage.salt.buckets", "20");
    config.overrideConfig("tsd.storage.salt.width", "0");
    new TSDB(client, config);
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
  public void initializePluginsSEH() throws Exception {
    config.overrideConfig("tsd.core.plugin_path", "./");
    config.overrideConfig("tsd.core.storage_exception_handler.enable", "true");
    config.overrideConfig("tsd.core.storage_exception_handler.plugin", 
        "net.opentsdb.tsd.DummySEHPlugin");
    config.overrideConfig(
        "tsd.core.storage_exception_handler.DummySEHPlugin.hosts", "localhost");
    tsdb.initializePlugins(true);
    assertNotNull(tsdb.getStorageExceptionHandler());
  }
  
  @Test (expected = RuntimeException.class)
  public void initializePluginsSEHBadConfig() throws Exception {
    config.overrideConfig("tsd.core.plugin_path", "./");
    config.overrideConfig("tsd.core.storage_exception_handler.enable", "true");
    config.overrideConfig("tsd.core.storage_exception_handler.plugin", 
        "net.opentsdb.tsd.DummySEHPlugin");
    tsdb.initializePlugins(true);
    assertNotNull(tsdb.getStorageExceptionHandler());
  }
  
  @Test (expected = NullPointerException.class)
  public void initializePluginsSEHEnabledButNoName() throws Exception {
    config.overrideConfig("tsd.core.plugin_path", "./");
    config.overrideConfig("tsd.core.storage_exception_handler.enable", "true");
    tsdb.initializePlugins(true);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializePluginsSEHNotFound() throws Exception {
    config.overrideConfig("tsd.core.plugin_path", "./");
    config.overrideConfig("tsd.core.storage_exception_handler.enable", "true");
    config.overrideConfig("tsd.core.storage_exception_handler.plugin", 
        "net.opentsdb.tsd.DoesNotExistSEHPlugin");
    config.overrideConfig(
        "tsd.core.storage_exception_handler.DummySEHPlugin.hosts", "localhost");
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
    assertArrayEquals(new byte[] { 0, 0, 1 }, 
        tsdb.getUID(UniqueIdType.METRIC, METRIC_STRING));
  }
  
  @Test
  public void getUIDTagk() {
    assertArrayEquals(new byte[] { 0, 0, 1 }, 
        tsdb.getUID(UniqueIdType.TAGK, TAGK_STRING));
  }
  
  @Test
  public void getUIDTagv() {
    assertArrayEquals(new byte[] { 0, 0, 1 }, 
        tsdb.getUID(UniqueIdType.TAGV, TAGV_STRING));
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void getUIDMetricNSU() {
    tsdb.getUID(UniqueIdType.METRIC, NSUN_METRIC);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void getUIDTagkNSU() {
    tsdb.getUID(UniqueIdType.TAGK, NSUN_TAGK);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void getUIDTagvNSU() {
    tsdb.getUID(UniqueIdType.TAGV, NSUN_TAGV);
  }
  
  @Test (expected = RuntimeException.class)
  public void getUIDNullType() {
    tsdb.getUID(null, METRIC_STRING);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getUIDNullName() {
    tsdb.getUID(UniqueIdType.TAGV, null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getUIDEmptyName() {
    tsdb.getUID(UniqueIdType.TAGV, "");
  }
  
  @Test
  public void assignUidMetric() {
    when(metrics.getId("sys.cpu.1")).thenThrow(
        new NoSuchUniqueName("metric", "sys.cpu.1"));
    when(metrics.getOrCreateId("sys.cpu.1"))
      .thenReturn(new byte[] { 0, 0, 2 });
    assertArrayEquals(new byte[] { 0, 0, 2 }, 
        tsdb.assignUid("metric", "sys.cpu.1"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidMetricExists() {
    tsdb.assignUid("metric", METRIC_STRING);
  }
  
  @Test
  public void assignUidTagk() {
    when(tag_names.getId("datacenter")).thenThrow(
        new NoSuchUniqueName("tagk", "datacenter"));
    when(tag_names.getOrCreateId("datacenter"))
      .thenReturn(new byte[] { 0, 0, 2 });
    assertArrayEquals(new byte[] { 0, 0, 2 }, 
        tsdb.assignUid("tagk", "datacenter"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidTagkExists() {
    tsdb.assignUid("tagk", TAGK_STRING);
  }
  
  @Test
  public void assignUidTagv() {
    when(tag_values.getId("localhost")).thenThrow(
        new NoSuchUniqueName("tagv", "localhost"));
    when(tag_values.getOrCreateId("localhost"))
      .thenReturn(new byte[] { 0, 0, 2 });
    assertArrayEquals(new byte[] { 0, 0, 2 }, 
        tsdb.assignUid("tagv", "localhost"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidTagvExists() {
    tsdb.assignUid("tagv", TAGV_STRING);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidBadType() {
    tsdb.assignUid("nothere", METRIC_STRING);
  }
  
  @Test (expected = NullPointerException.class)
  public void assignUidNullType() {
    tsdb.assignUid(null, METRIC_STRING);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidNullName() {
    tsdb.assignUid("metric", null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidInvalidCharacter() {
    tsdb.assignUid("metric", "Not!A:Valid@Name");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void renameUidInvalidNewname() {
    tsdb.renameUid("metric", "existing", null);
  }

  @Test (expected = IllegalArgumentException.class)
  public void renameUidNonexistentMetric() {
    when(metrics.getId("sys.cpu.1")).thenThrow(
        new NoSuchUniqueName("metric", "sys.cpu.1"));
    tsdb.renameUid("metric", "sys.cpu.1", "sys.cpu.2");
  }

  @Test
  public void renameUidMetric() {
    tsdb.renameUid("metric", "sys.cpu.1", "sys.cpu.2");
  }

  @Test (expected = IllegalArgumentException.class)
  public void renameUidNonexistentTagk() {
    when(tag_names.getId("datacenter")).thenThrow(
        new NoSuchUniqueName("tagk", "datacenter"));
    tsdb.renameUid("tagk", "datacenter", "datacluster");
  }

  @Test
  public void renameUidTagk() {
    tsdb.renameUid("tagk", "datacenter", "datacluster");
  }

  @Test (expected = IllegalArgumentException.class)
  public void renameUidNonexistentTagv() {
    when(tag_values.getId("localhost")).thenThrow(
        new NoSuchUniqueName("tagv", "localhost"));
    tsdb.renameUid("tagv", "localhost", "127.0.0.1");
  }

  @Test
  public void renameUidTagv() {
    tsdb.renameUid("tagv", "localhost", "127.0.0.1");
  }

  @Test (expected = IllegalArgumentException.class)
  public void renameUidBadType() {
    tsdb.renameUid("wrongtype", METRIC_STRING, METRIC_STRING);
  }

  @Test
  public void uidTable() {
    assertNotNull(tsdb.uidTable());
    assertArrayEquals("tsdb-uid".getBytes(), tsdb.uidTable());
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
  @Test
  public void setupAddPointStorage() throws Exception {
    storage = new MockBase(tsdb, client, true, true, true, true);
  }
}
