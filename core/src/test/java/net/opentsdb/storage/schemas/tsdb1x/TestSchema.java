// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationException;
import net.opentsdb.configuration.UnitTestConfiguration;
import net.opentsdb.core.Registry;
import net.opentsdb.core.TSDB;
import net.opentsdb.storage.TimeSeriesDataStore;
import net.opentsdb.storage.TimeSeriesDataStoreFactory;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdFactory;
import net.opentsdb.uid.UniqueIdStore;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.UnitTestException;

public class TestSchema {
  
  private static final String TESTID = "UT";
  
  private static TSDB tsdb;
  private static Configuration config;
  private static Registry registry;
  private static TimeSeriesDataStoreFactory store_factory;
  private static TimeSeriesDataStore store;
  private static UniqueIdStore uid_store;
  private static UniqueIdFactory uid_factory;
  private static UniqueId uid_cache; 
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    tsdb = mock(TSDB.class);
    config = UnitTestConfiguration.getConfiguration();
    registry = mock(Registry.class);
    store_factory = mock(TimeSeriesDataStoreFactory.class);
    store = mock(TimeSeriesDataStore.class);
    uid_store = mock(UniqueIdStore.class);
    uid_factory = mock(UniqueIdFactory.class);
    uid_cache = mock(UniqueId.class);
    
    when(tsdb.getConfig()).thenReturn(config);
    when(tsdb.getRegistry()).thenReturn(registry);
    
    // return the default
    when(registry.getPlugin(TimeSeriesDataStoreFactory.class, null))
      .thenReturn(store_factory);
    when(store_factory.newInstance(tsdb, null)).thenReturn(store);    
    when(registry.getSharedObject("default_uidstore"))
      .thenReturn(uid_store);
    when(registry.getPlugin(UniqueIdFactory.class, "LRU"))
      .thenReturn(uid_factory);
    when(uid_factory.newInstance(eq(tsdb), anyString(), 
        any(UniqueIdType.class), eq(uid_store))).thenReturn(uid_cache);
  }
  
  @Before
  public void before() throws Exception {
    resetConfig();
  }
  
  @Test
  public void ctorDefault() throws Exception {
    Schema schema = new Schema(tsdb, null);
    assertEquals(3, schema.metricWidth());
    assertEquals(3, schema.tagkWidth());
    assertEquals(3, schema.tagvWidth());
    assertEquals(20, schema.saltBuckets());
    assertEquals(0, schema.saltWidth());
    assertSame(store, schema.dataStore());
    assertSame(uid_store, schema.uidStore());
    assertSame(uid_cache, schema.metrics());
    assertSame(uid_cache, schema.tagNames());
    assertSame(uid_cache, schema.tagValues());
  }
  
  @Test
  public void ctorOverrides() throws Exception {
    config = UnitTestConfiguration.getConfiguration();
    when(tsdb.getConfig()).thenReturn(config);
    ((UnitTestConfiguration) config)
      .register("tsd.storage.uid.width.metric", 4, false, "UT");
    ((UnitTestConfiguration) config)
      .register("tsd.storage.uid.width.tagk", 5, false, "UT");
    ((UnitTestConfiguration) config)
      .register("tsd.storage.uid.width.tagv", 6, false, "UT");
    ((UnitTestConfiguration) config)
      .register("tsd.storage.salt.buckets", 16, false, "UT");
    ((UnitTestConfiguration) config)
      .register("tsd.storage.salt.width", 1, false, "UT");
    
    Schema schema = new Schema(tsdb, null);
    assertEquals(4, schema.metricWidth());
    assertEquals(5, schema.tagkWidth());
    assertEquals(6, schema.tagvWidth());
    assertEquals(16, schema.saltBuckets());
    assertEquals(1, schema.saltWidth());
    assertSame(store, schema.dataStore());
    assertSame(uid_store, schema.uidStore());
  }
  
  @Test
  public void ctorID() throws Exception {
    TSDB tsdb = mock(TSDB.class);
    Registry registry = mock(Registry.class);
    Configuration config = UnitTestConfiguration.getConfiguration();
    UniqueIdStore us = mock(UniqueIdStore.class);
    UniqueIdFactory uf = mock(UniqueIdFactory.class);
    UniqueId uc = mock(UniqueId.class);
    TimeSeriesDataStoreFactory sf = mock(TimeSeriesDataStoreFactory.class);
    TimeSeriesDataStore s = mock(TimeSeriesDataStore.class);
    
    when(tsdb.getConfig()).thenReturn(config);
    when(tsdb.getRegistry()).thenReturn(registry);
    when(registry.getPlugin(TimeSeriesDataStoreFactory.class, TESTID))
      .thenReturn(sf);
    when(sf.newInstance(tsdb, TESTID)).thenReturn(s);
    when(registry.getSharedObject(TESTID + "_uidstore"))
      .thenReturn(us);
    when(registry.getPlugin(UniqueIdFactory.class, "LRU"))
      .thenReturn(uf);
    when(uf.newInstance(eq(tsdb), anyString(), 
        any(UniqueIdType.class), eq(us))).thenReturn(uc);
    
    ((UnitTestConfiguration) config)
      .register("tsd.storage." + TESTID + ".uid.width.metric", 4, false, "UT");
    ((UnitTestConfiguration) config)
      .register("tsd.storage." + TESTID + ".uid.width.tagk", 5, false, "UT");
    ((UnitTestConfiguration) config)
      .register("tsd.storage." + TESTID + ".uid.width.tagv", 6, false, "UT");
    ((UnitTestConfiguration) config)
      .register("tsd.storage." + TESTID + ".salt.buckets", 16, false, "UT");
    ((UnitTestConfiguration) config)
      .register("tsd.storage." + TESTID + ".salt.width", 1, false, "UT");
    ((UnitTestConfiguration) config)
      .register("tsd.storage." + TESTID + ".data.store", TESTID, false, "UT");
    
    Schema schema = new Schema(tsdb, TESTID);
    assertEquals(4, schema.metricWidth());
    assertEquals(5, schema.tagkWidth());
    assertEquals(6, schema.tagvWidth());
    assertEquals(16, schema.saltBuckets());
    assertEquals(1, schema.saltWidth());
    assertSame(s, schema.dataStore());
    assertSame(us, schema.uidStore());
    assertSame(uc, schema.metrics());
    assertSame(uc, schema.tagNames());
    assertSame(uc, schema.tagValues());
  }
  
  @Test
  public void ctorNoStoreFactory() throws Exception {
    TSDB tsdb = mock(TSDB.class);
    Registry registry = mock(Registry.class);
    Configuration config = UnitTestConfiguration.getConfiguration();
    when(tsdb.getConfig()).thenReturn(config);
    when(tsdb.getRegistry()).thenReturn(registry);
    ((UnitTestConfiguration) config)
      .register("tsd.storage.data.store", "NOTTHERE", false, "UT");
    try {
      new Schema(tsdb, null);
      fail("Expected ConfigurationException");
    } catch (ConfigurationException e) { }
  }
  
  @Test
  public void ctorStoreInstantiationFailure() throws Exception {
    TSDB tsdb = mock(TSDB.class);
    Configuration config = UnitTestConfiguration.getConfiguration();
    Registry registry = mock(Registry.class);
    when(tsdb.getRegistry()).thenReturn(registry);
    when(tsdb.getConfig()).thenReturn(config);
    TimeSeriesDataStoreFactory store_factory = mock(TimeSeriesDataStoreFactory.class);
    when(registry.getPlugin(TimeSeriesDataStoreFactory.class, null)).thenReturn(store_factory);
    when(store_factory.newInstance(tsdb, null)).thenThrow(new UnitTestException());
    try {
      new Schema(tsdb, null);
      fail("Expected UnitTestException");
    } catch (UnitTestException e) { }
  }

  @Test
  public void getTSUID() throws Exception {
    Schema schema = new Schema(tsdb, null);
    try {
      schema.getTSUID(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      schema.getTSUID(new byte[0]);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      schema.getTSUID(new byte[4]);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    byte[] row = new byte[] { 0, 0, 1, 1, 2, 3, 4, 0, 0, 1, 0, 0, 1 };
    byte[] expected = new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 };
    assertArrayEquals(expected, schema.getTSUID(row));
    
    schema.salt_width = 1;
    row = new byte[] { 42, 0, 0, 1, 1, 2, 3, 4, 0, 0, 1, 0, 0, 1 };
    assertArrayEquals(expected, schema.getTSUID(row));
    
    schema.tagv_width = 5;
    row = new byte[] { 42, 0, 0, 1, 1, 2, 3, 4, 0, 0, 1, 0, 0, 0, 0, 1 };
    expected = new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 1 };
    assertArrayEquals(expected, schema.getTSUID(row));
  }
  
  @Test
  public void baseTimestamp() throws Exception {
    Schema schema = new Schema(tsdb, null);
    try {
      schema.baseTimestamp(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      schema.baseTimestamp(new byte[0]);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      schema.baseTimestamp(new byte[4]);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    byte[] row = new byte[] { 0, 0, 1, 1, 2, 3, 4, 0, 0, 1, 0, 0, 1 };
    assertEquals(16909060000L, schema.baseTimestamp(row).msEpoch());
    
    row = new byte[] { 0, 0, 1, 0x5A, 0x49, 0x7A, 0, 0, 0, 1, 0, 0, 1 };
    assertEquals(1514764800000L, schema.baseTimestamp(row).msEpoch());
  }
  
  @Test
  public void uidWidth() throws Exception {
    Schema schema = new Schema(tsdb, null);
    schema.metric_width = 1;
    schema.tagk_width = 2;
    schema.tagv_width = 5;
    try {
      schema.uidWidth(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertEquals(1, schema.uidWidth(UniqueIdType.METRIC));
    assertEquals(2, schema.uidWidth(UniqueIdType.TAGK));
    assertEquals(5, schema.uidWidth(UniqueIdType.TAGV));
    try {
      schema.uidWidth(UniqueIdType.NAMESPACE);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  private void resetConfig() throws Exception {
    final UnitTestConfiguration c = (UnitTestConfiguration) config;
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
}
