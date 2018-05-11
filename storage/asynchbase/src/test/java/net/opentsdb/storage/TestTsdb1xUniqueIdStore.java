// This file is part of OpenTSDB.
// Copyright (C) 2010-2018  The OpenTSDB Authors.
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
package net.opentsdb.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.MockTrace;
import net.opentsdb.stats.Span;
import net.opentsdb.stats.Span.SpanBuilder;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.UnitTestConfiguration;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.storage.MockBase;
import net.opentsdb.storage.schemas.tsdb1x.ResolvedFilter;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.UnitTestException;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
// "Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
// because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
                  "ch.qos.*", "org.slf4j.*",
                  "com.sum.*", "org.xml.*"})
@PrepareForTest({ HBaseClient.class, TSDB.class, Config.class, Scanner.class, 
  /**RandomUniqueId.class,*/ Const.class, Deferred.class })
public class TestTsdb1xUniqueIdStore extends UTBase {
  
  private static final String UNI_STRING = "\u00a5123";
  private static final byte[] UNI_BYTES = new byte[] { 0, 0, 6 };
  
  @BeforeClass
  public static void beforeClassLocal() throws Exception {
    // UTF-8 Encoding
    storage.addColumn(UID_TABLE, UNI_STRING.getBytes(Const.UTF8_CHARSET), 
        Tsdb1xUniqueIdStore.ID_FAMILY, Tsdb1xUniqueIdStore.METRICS_QUAL, 
        UNI_BYTES);
    storage.addColumn(UID_TABLE, UNI_BYTES, Tsdb1xUniqueIdStore.NAME_FAMILY, 
        Tsdb1xUniqueIdStore.METRICS_QUAL, 
        UNI_STRING.getBytes(Const.UTF8_CHARSET));
  }

  @Before
  public void before() throws Exception {
    resetConfig();
  }
  
//  @Test(expected=IllegalArgumentException.class)
//  public void testCtorZeroWidth() {
//    uid = new UniqueId(client, table, METRIC, 0);
//  }
//
//  @Test(expected=IllegalArgumentException.class)
//  public void testCtorNegativeWidth() {
//    uid = new UniqueId(client, table, METRIC, -1);
//  }
//
//  @Test(expected=IllegalArgumentException.class)
//  public void testCtorEmptyKind() {
//    uid = new UniqueId(client, table, "", 3);
//  }
//
//  @Test(expected=IllegalArgumentException.class)
//  public void testCtorLargeWidth() {
//    uid = new UniqueId(client, table, METRIC, 9);
//  }
//
//  @Test
//  public void kindEqual() {
//    uid = new UniqueId(client, table, METRIC, 3);
//    assertEquals(METRIC, uid.kind());
//  }
//
//  @Test
//  public void widthEqual() {
//    uid = new UniqueId(client, table, METRIC, 3);
//    assertEquals(3, uid.width());
//  }
//  
  
  @Test
  public void getName() throws Exception {
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(data_store);
    assertEquals(METRIC_STRING, uid.getName(UniqueIdType.METRIC, METRIC_BYTES, null).join());
    assertEquals(TAGK_STRING, uid.getName(UniqueIdType.TAGK, TAGK_BYTES, null).join());
    assertEquals(TAGV_STRING, uid.getName(UniqueIdType.TAGV, TAGV_BYTES, null).join());
    
    // this shouldn't decode properly!!
    assertNotEquals(UNI_STRING, uid.getName(UniqueIdType.METRIC, UNI_BYTES, null).join());
    
    // now try it with UTF8
    tsdb.config.override(
        data_store.getConfigKey(Tsdb1xUniqueIdStore.CHARACTER_SET_KEY), "UTF-8");
    uid = new Tsdb1xUniqueIdStore(data_store);
    
    assertEquals(METRIC_STRING, uid.getName(UniqueIdType.METRIC, METRIC_BYTES, null).join());
    assertEquals(TAGK_STRING, uid.getName(UniqueIdType.TAGK, TAGK_BYTES, null).join());
    assertEquals(TAGV_STRING, uid.getName(UniqueIdType.TAGV, TAGV_BYTES, null).join());
    assertEquals(UNI_STRING, uid.getName(UniqueIdType.METRIC, UNI_BYTES, null).join());
    
    // not present
    assertNull(uid.getName(UniqueIdType.METRIC, NSUI_METRIC, null).join());
    assertNull(uid.getName(UniqueIdType.TAGK, NSUI_TAGK, null).join());
    assertNull(uid.getName(UniqueIdType.TAGV, NSUI_TAGV, null).join());
  }
  
  @Test
  public void getNameIllegalArguments() throws Exception {
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(data_store);
    
    try {
      uid.getName(null, METRIC_BYTES, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      uid.getName(UniqueIdType.METRIC, null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      uid.getName(UniqueIdType.METRIC, new byte[0], null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void getNameExceptionFromGet() throws Exception {
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(data_store);
    // exception
    Deferred<String> deferred = uid.getName(UniqueIdType.METRIC, 
        METRIC_BYTES_EX, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { 
      assertTrue(e.getCause() instanceof UnitTestException);
    }
  }
  
  @Test
  public void getNameTracing() throws Exception {
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(data_store);
    // span tests. Only trace on debug.
    trace = new MockTrace();
    assertEquals(METRIC_STRING, uid.getName(UniqueIdType.METRIC, METRIC_BYTES, 
        trace.newSpan("UT").start()).join());
    assertEquals(0, trace.spans.size());
    
    trace = new MockTrace(true);
    assertEquals(METRIC_STRING, uid.getName(UniqueIdType.METRIC, METRIC_BYTES, 
        trace.newSpan("UT").start()).join());
    verifySpan(Tsdb1xUniqueIdStore.class.getName() + ".getName");
  }
  
  @Test
  public void getNameTraceException() throws Exception {
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(data_store);
    trace = new MockTrace(true);
    try {
      uid.getName(UniqueIdType.METRIC, METRIC_BYTES_EX, trace.newSpan("UT").start())
        .join();
      fail("Expected StorageException");
    } catch (StorageException e) { 
      verifySpan(Tsdb1xUniqueIdStore.class.getName() + ".getName", UnitTestException.class);
    }
  }
  
  @Test
  public void getNameClientReturnsException() throws Exception {
    // see what happens if we just return an exception
    Tsdb1xHBaseDataStore store = badClient();
    when(store.client().get(any(GetRequest.class)))
      .thenReturn(Deferred.fromError(new UnitTestException()));
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(store);
    
    Deferred<String> deferred = uid.getName(UniqueIdType.TAGV, TAGV_BYTES, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
    
    store = badClient();
    when(store.client().get(any(GetRequest.class)))
      .thenReturn(Deferred.fromError(new UnitTestException()));
    uid = new Tsdb1xUniqueIdStore(store);
    // with trace
    trace = new MockTrace(true);
    deferred = uid.getName(UniqueIdType.TAGV, TAGV_BYTES,
         trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
    verifySpan(Tsdb1xUniqueIdStore.class.getName() + ".getName", 
        UnitTestException.class);
  }
  
  @Test
  public void getNameClientThrowsException() throws Exception {
    // see what happens if we just return an exception
    Tsdb1xHBaseDataStore store = badClient();
    when(store.client().get(any(GetRequest.class)))
      .thenThrow(new UnitTestException());
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(store);
    
    Deferred<String> deferred = uid.getName(UniqueIdType.TAGV, TAGV_BYTES, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
    
    store = badClient();
    when(store.client().get(any(GetRequest.class)))
      .thenThrow(new UnitTestException());
    uid = new Tsdb1xUniqueIdStore(store);
    // with trace
    trace = new MockTrace(true);
    deferred = uid.getName(UniqueIdType.TAGV, TAGV_BYTES,
        trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
    verifySpan(Tsdb1xUniqueIdStore.class.getName() + ".getName", 
        UnitTestException.class);
  }
  
  @Test
  public void getNames() throws Exception {
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(data_store);
    List<String> names = uid.getNames(UniqueIdType.METRIC, 
        Lists.newArrayList(METRIC_BYTES, NSUI_METRIC), null).join();
    assertEquals(2, names.size());
    assertEquals(METRIC_STRING, names.get(0));
    assertNull(names.get(1));
    
    names = uid.getNames(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_BYTES, TAGV_B_BYTES), null).join();
    assertEquals(2, names.size());
    assertEquals(TAGV_STRING, names.get(0));
    assertEquals(TAGV_B_STRING, names.get(1));
    
    // test the order
    names = uid.getNames(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_B_BYTES, TAGV_BYTES), null).join();
    assertEquals(2, names.size());
    assertEquals(TAGV_B_STRING, names.get(0));
    assertEquals(TAGV_STRING, names.get(1));
    
    // UTF8
    tsdb.config.override(
        data_store.getConfigKey(Tsdb1xUniqueIdStore.CHARACTER_SET_KEY), "UTF-8");
    uid = new Tsdb1xUniqueIdStore(data_store);
    
    names = uid.getNames(UniqueIdType.METRIC, 
        Lists.newArrayList(METRIC_BYTES, UNI_BYTES), null).join();
    assertEquals(2, names.size());
    assertEquals(METRIC_STRING, names.get(0));
    assertEquals(UNI_STRING, names.get(1));
    
    // all nulls
    names = uid.getNames(UniqueIdType.TAGK, 
        Lists.newArrayList(NSUI_TAGK, new byte[] { 0, 0, 5 }), null).join();
    assertEquals(2, names.size());
    assertNull(names.get(0));
    assertNull(names.get(1));
  }
  
  @Test
  public void getNamesIllegalArguments() throws Exception {
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(data_store);
    
    try {
      uid.getNames(null, Lists.newArrayList(METRIC_BYTES, METRIC_B_BYTES), null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      uid.getNames(UniqueIdType.METRIC, null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      uid.getNames(UniqueIdType.METRIC, Lists.newArrayList(), null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      uid.getNames(UniqueIdType.METRIC, Lists.newArrayList(METRIC_BYTES, null), null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void getNamesExceptionInOneOrMore() throws Exception {
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(data_store);
    // one throws exception
    Deferred<List<String>> deferred = uid.getNames(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_BYTES, TAGV_BYTES_EX), null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
  }
  
  @Test
  public void getNamesTracing() throws Exception {
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(data_store);
    // span tests. Only trace on debug.
    trace = new MockTrace();
    uid.getNames(UniqueIdType.METRIC, 
        Lists.newArrayList(METRIC_BYTES, METRIC_B_BYTES), trace.newSpan("UT").start()).join();
    assertEquals(0, trace.spans.size());
    
    trace = new MockTrace(true);
    uid.getNames(UniqueIdType.METRIC, 
        Lists.newArrayList(METRIC_BYTES, METRIC_B_BYTES), trace.newSpan("UT").start()).join();
    verifySpan(Tsdb1xUniqueIdStore.class.getName() + ".getNames");
  }
  
  @Test
  public void getNamesTraceException() throws Exception {
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(data_store);
    trace = new MockTrace(true);
    Deferred<List<String>> deferred = uid.getNames(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_BYTES, TAGV_BYTES_EX), trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    verifySpan(Tsdb1xUniqueIdStore.class.getName() + ".getNames", 
        UnitTestException.class);
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void getNamesClientReturnsException() throws Exception {
    // see what happens if we just return an exception
    Tsdb1xHBaseDataStore store = badClient();
    when(store.client().get(any(List.class)))
      .thenReturn(Deferred.fromResult(new UnitTestException()));
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(store);
    
    Deferred<List<String>> deferred = uid.getNames(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_BYTES, TAGV_B_BYTES), null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
    
    store = badClient();
    when(store.client().get(any(List.class)))
      .thenReturn(Deferred.fromResult(new UnitTestException()));
    uid = new Tsdb1xUniqueIdStore(store);
    // with trace
    trace = new MockTrace(true);
    deferred = uid.getNames(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_BYTES, TAGV_B_BYTES), trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
    verifySpan(Tsdb1xUniqueIdStore.class.getName() + ".getNames", 
        UnitTestException.class);
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void getNamesClientThrowsException() throws Exception {
    // see what happens if we just return an exception
    Tsdb1xHBaseDataStore store = badClient();
    when(store.client().get(any(List.class)))
      .thenThrow(new UnitTestException());
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(store);
    
    Deferred<List<String>> deferred = uid.getNames(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_BYTES, TAGV_B_BYTES), null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
    
    store = badClient();
    when(store.client().get(any(List.class)))
      .thenThrow(new UnitTestException());
    uid = new Tsdb1xUniqueIdStore(store);
    // with trace
    trace = new MockTrace(true);
    deferred = uid.getNames(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_BYTES, TAGV_B_BYTES), trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
    verifySpan(Tsdb1xUniqueIdStore.class.getName() + ".getNames", 
        UnitTestException.class);
  }

  @Test
  public void getId() throws Exception {
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(data_store);
    assertArrayEquals(METRIC_BYTES, uid.getId(UniqueIdType.METRIC, METRIC_STRING, null).join());
    assertArrayEquals(TAGK_BYTES, uid.getId(UniqueIdType.TAGK, TAGK_STRING, null).join());
    assertArrayEquals(TAGV_BYTES, uid.getId(UniqueIdType.TAGV, TAGV_STRING, null).join());
    
    // this shouldn't encode properly so it won't match the string in the DB!!
    assertNull(uid.getId(UniqueIdType.METRIC, UNI_STRING, null).join());
    
    // now try it with UTF8
    tsdb.config.override(
        data_store.getConfigKey(Tsdb1xUniqueIdStore.CHARACTER_SET_KEY), "UTF-8");
    uid = new Tsdb1xUniqueIdStore(data_store);
    
    assertArrayEquals(METRIC_BYTES, uid.getId(UniqueIdType.METRIC, METRIC_STRING, null).join());
    assertArrayEquals(TAGK_BYTES, uid.getId(UniqueIdType.TAGK, TAGK_STRING, null).join());
    assertArrayEquals(TAGV_BYTES, uid.getId(UniqueIdType.TAGV, TAGV_STRING, null).join());
    assertArrayEquals(UNI_BYTES, uid.getId(UniqueIdType.METRIC, UNI_STRING, null).join());
    
    // not present
    assertNull(uid.getId(UniqueIdType.METRIC, NSUN_METRIC, null).join());
    assertNull(uid.getId(UniqueIdType.TAGK, NSUN_TAGK, null).join());
    assertNull(uid.getId(UniqueIdType.TAGV, NSUN_TAGV, null).join());
  }
  
  @Test
  public void getIdIllegalArguments() throws Exception {
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(data_store);
    
    try {
      uid.getId(null, METRIC_STRING, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      uid.getId(UniqueIdType.METRIC, null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      uid.getId(UniqueIdType.METRIC, "", null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void getIdExceptionFromGet() throws Exception {
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(data_store);
    // exception
    Deferred<byte[]> deferred = uid.getId(UniqueIdType.METRIC, METRIC_STRING_EX, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { 
      assertTrue(e.getCause() instanceof UnitTestException);
    }
  }
  
  @Test
  public void getIdTracing() throws Exception {
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(data_store);
    // span tests. Only trace on debug.
    trace = new MockTrace();
    assertArrayEquals(METRIC_BYTES, uid.getId(UniqueIdType.METRIC, METRIC_STRING, 
        trace.newSpan("UT").start()).join());
    assertEquals(0, trace.spans.size());
    
    trace = new MockTrace(true);
    assertArrayEquals(METRIC_BYTES, uid.getId(UniqueIdType.METRIC, METRIC_STRING, 
        trace.newSpan("UT").start()).join());
    verifySpan(Tsdb1xUniqueIdStore.class.getName() + ".getId");
  }
  
  @Test
  public void getIdTraceException() throws Exception {
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(data_store);
    trace = new MockTrace(true);
    try {
      uid.getId(UniqueIdType.METRIC, METRIC_STRING_EX, trace.newSpan("UT").start())
        .join();
      fail("Expected StorageException");
    } catch (StorageException e) { 
      verifySpan(Tsdb1xUniqueIdStore.class.getName() + ".getId", UnitTestException.class);
    }
  }
  
  @Test
  public void getIdClientReturnsException() throws Exception {
    // see what happens if we just return an exception
    Tsdb1xHBaseDataStore store = badClient();
    when(store.client().get(any(GetRequest.class)))
      .thenReturn(Deferred.fromError(new UnitTestException()));
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(store);
    
    Deferred<byte[]> deferred = uid.getId(UniqueIdType.TAGV, TAGV_STRING, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
    
    store = badClient();
    when(store.client().get(any(GetRequest.class)))
      .thenReturn(Deferred.fromError(new UnitTestException()));
    uid = new Tsdb1xUniqueIdStore(store);
    // with trace
    trace = new MockTrace(true);
    deferred = uid.getId(UniqueIdType.TAGV, TAGV_STRING,
         trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
    verifySpan(Tsdb1xUniqueIdStore.class.getName() + ".getId", 
        UnitTestException.class);
  }
  
  @Test
  public void getIdClientThrowsException() throws Exception {
    // see what happens if we just return an exception
    Tsdb1xHBaseDataStore store = badClient();
    when(store.client().get(any(GetRequest.class)))
      .thenThrow(new UnitTestException());
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(store);
    
    Deferred<byte[]> deferred = uid.getId(UniqueIdType.TAGV, TAGV_STRING, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
    
    store = badClient();
    when(store.client().get(any(GetRequest.class)))
      .thenThrow(new UnitTestException());
    uid = new Tsdb1xUniqueIdStore(store);
    // with trace
    trace = new MockTrace(true);
    deferred = uid.getId(UniqueIdType.TAGV, TAGV_STRING,
        trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
    verifySpan(Tsdb1xUniqueIdStore.class.getName() + ".getId", 
        UnitTestException.class);
  }
  
  @Test
  public void getIds() throws Exception {
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(data_store);
    List<byte[]> ids = uid.getIds(UniqueIdType.METRIC, 
        Lists.newArrayList(METRIC_STRING, NSUN_METRIC), null).join();
    assertEquals(2, ids.size());
    assertArrayEquals(METRIC_BYTES, ids.get(0));
    assertNull(ids.get(1));
    
    ids = uid.getIds(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_STRING, TAGV_B_STRING), null).join();
    assertEquals(2, ids.size());
    assertArrayEquals(TAGV_BYTES, ids.get(0));
    assertArrayEquals(TAGV_B_BYTES, ids.get(1));
    
    // test the order
    ids = uid.getIds(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_B_STRING, TAGV_STRING), null).join();
    assertEquals(2, ids.size());
    assertArrayEquals(TAGV_B_BYTES, ids.get(0));
    assertArrayEquals(TAGV_BYTES, ids.get(1));
    
    // UTF8
    tsdb.config.override(
        data_store.getConfigKey(Tsdb1xUniqueIdStore.CHARACTER_SET_KEY), "UTF-8");
    uid = new Tsdb1xUniqueIdStore(data_store);
    
    ids = uid.getIds(UniqueIdType.METRIC, 
        Lists.newArrayList(METRIC_STRING, UNI_STRING), null).join();
    assertEquals(2, ids.size());
    assertArrayEquals(METRIC_BYTES, ids.get(0));
    assertArrayEquals(UNI_BYTES, ids.get(1));
    
    // all nulls
    ids = uid.getIds(UniqueIdType.TAGK, 
        Lists.newArrayList("foo", NSUN_METRIC), null).join();
    assertEquals(2, ids.size());
    assertNull(ids.get(0));
    assertNull(ids.get(1));
  }
  
  @Test
  public void getIdsIllegalArguments() throws Exception {
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(data_store);
    
    try {
      uid.getIds(null, Lists.newArrayList(METRIC_STRING, METRIC_B_STRING), null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      uid.getIds(UniqueIdType.METRIC, null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      uid.getIds(UniqueIdType.METRIC, Lists.newArrayList(), null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      uid.getIds(UniqueIdType.METRIC, Lists.newArrayList(METRIC_STRING, null), null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      uid.getIds(UniqueIdType.METRIC, Lists.newArrayList(METRIC_STRING, ""), null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void getIdsExceptionInOneOrMore() throws Exception {
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(data_store);
    // one throws exception
    Deferred<List<byte[]>> deferred = uid.getIds(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_STRING, TAGV_STRING_EX), null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
  }
  
  @Test
  public void getIdsTracing() throws Exception {
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(data_store);
    // span tests. Only trace on debug.
    trace = new MockTrace();
    uid.getIds(UniqueIdType.METRIC, 
        Lists.newArrayList(METRIC_STRING, METRIC_B_STRING), trace.newSpan("UT").start()).join();
    assertEquals(0, trace.spans.size());
    
    trace = new MockTrace(true);
    uid.getIds(UniqueIdType.METRIC, 
        Lists.newArrayList(METRIC_STRING, METRIC_B_STRING), trace.newSpan("UT").start()).join();
    verifySpan(Tsdb1xUniqueIdStore.class.getName() + ".getIds");
  }
  
  @Test
  public void getIdsTraceException() throws Exception {
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(data_store);
    trace = new MockTrace(true);
    Deferred<List<byte[]>> deferred = uid.getIds(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_STRING, TAGV_STRING_EX), trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    verifySpan(Tsdb1xUniqueIdStore.class.getName() + ".getIds", 
        UnitTestException.class);
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void getIdsClientReturnsException() throws Exception {
    // see what happens if we just return an exception
    Tsdb1xHBaseDataStore store = badClient();
    when(store.client().get(any(List.class)))
      .thenReturn(Deferred.fromResult(new UnitTestException()));
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(store);
    
    Deferred<List<byte[]>> deferred = uid.getIds(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_STRING, TAGV_B_STRING), null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
    
    store = badClient();
    when(store.client().get(any(List.class)))
      .thenReturn(Deferred.fromResult(new UnitTestException()));
    uid = new Tsdb1xUniqueIdStore(store);
    // with trace
    trace = new MockTrace(true);
    deferred = uid.getIds(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_STRING, TAGV_B_STRING), trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
    verifySpan(Tsdb1xUniqueIdStore.class.getName() + ".getIds", 
        UnitTestException.class);
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void getIdsClientThrowsException() throws Exception {
    // see what happens if we just return an exception
    Tsdb1xHBaseDataStore store = badClient();
    when(store.client().get(any(List.class)))
      .thenThrow(new UnitTestException());
    Tsdb1xUniqueIdStore uid = new Tsdb1xUniqueIdStore(store);
    
    Deferred<List<byte[]>> deferred = uid.getIds(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_STRING, TAGV_B_STRING), null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
    
    store = badClient();
    when(store.client().get(any(List.class)))
      .thenThrow(new UnitTestException());
    uid = new Tsdb1xUniqueIdStore(store);
    // with trace
    trace = new MockTrace(true);
    deferred = uid.getIds(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_STRING, TAGV_B_STRING), trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
    verifySpan(Tsdb1xUniqueIdStore.class.getName() + ".getIds", 
        UnitTestException.class);
  }
  
//  // The table contains IDs encoded on 2 bytes but the instance wants 3.
//  @Test(expected=IllegalStateException.class)
//  public void getIdMisconfiguredWidth() {
//    uid = new UniqueId(client, table, METRIC, 3);
//    final byte[] id = { 'a', 0x42 };
//    final byte[] byte_name = { 'f', 'o', 'o' };
//
//    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
//    kvs.add(new KeyValue(byte_name, ID, METRIC_ARRAY, id));
//    when(client.get(anyGet()))
//      .thenReturn(Deferred.fromResult(kvs));
//
//    uid.getId("foo");
//  }
//
//  @Test(expected=NoSuchUniqueName.class)
//  public void getIdForNonexistentName() {
//    uid = new UniqueId(client, table, METRIC, 3);
//
//    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
//      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
//    // Watch this! ______,^   I'm writing C++ in Java!
//
//    uid.getId("foo");
//  }
//
//  @Test
//  public void getOrCreateIdWithExistingId() {
//    uid = new UniqueId(client, table, METRIC, 3);
//    final byte[] id = { 0, 'a', 0x42 };
//    final byte[] byte_name = { 'f', 'o', 'o' };
//
//    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
//    kvs.add(new KeyValue(byte_name, ID, METRIC_ARRAY, id));
//    when(client.get(anyGet()))
//      .thenReturn(Deferred.fromResult(kvs));
//
//    assertArrayEquals(id, uid.getOrCreateId("foo"));
//    // Should be a cache hit ...
//    assertArrayEquals(id, uid.getOrCreateId("foo"));
//    assertEquals(1, uid.cacheHits());
//    assertEquals(1, uid.cacheMisses());
//    assertEquals(2, uid.cacheSize());
//
//    // ... so verify there was only one HBase Get.
//    verify(client).get(anyGet());
//  }
//
//  @Test  // Test the creation of an ID with no problem.
//  public void getOrCreateIdAssignFilterOK() {
//    uid = new UniqueId(client, table, METRIC, 3);
//    final byte[] id = { 0, 0, 5 };
//    final Config config = mock(Config.class);
//    when(config.enable_realtime_uid()).thenReturn(false);
//    final TSDB tsdb = mock(TSDB.class);
//    when(tsdb.getConfig()).thenReturn(config);
//    uid.setTSDB(tsdb);
//    final UniqueIdFilterPlugin filter = mock(UniqueIdFilterPlugin.class);
//    when(filter.fillterUIDAssignments()).thenReturn(true);
//    when(filter.allowUIDAssignment(any(UniqueIdType.class), anyString(), 
//        anyString(), anyMapOf(String.class, String.class)))
//      .thenReturn(Deferred.fromResult(true));
//    when(tsdb.getUidFilter()).thenReturn(filter);
//    
//    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
//      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
//    // Watch this! ______,^   I'm writing C++ in Java!
//
//    when(client.atomicIncrement(incrementForRow(MAXID)))
//      .thenReturn(Deferred.fromResult(5L));
//
//    when(client.compareAndSet(anyPut(), emptyArray()))
//      .thenReturn(Deferred.fromResult(true))
//      .thenReturn(Deferred.fromResult(true));
//
//    assertArrayEquals(id, uid.getOrCreateId("foo"));
//    // Should be a cache hit since we created that entry.
//    assertArrayEquals(id, uid.getOrCreateId("foo"));
//    // Should be a cache hit too for the same reason.
//    assertEquals("foo", uid.getName(id));
//
//    verify(client).get(anyGet()); // Initial Get.
//    verify(client).atomicIncrement(incrementForRow(MAXID));
//    // Reverse + forward mappings.
//    verify(client, times(2)).compareAndSet(anyPut(), emptyArray());
//    verify(filter, times(1)).allowUIDAssignment(any(UniqueIdType.class), anyString(), 
//        anyString(), anyMapOf(String.class, String.class));
//  }
//
//  @Test (expected = FailedToAssignUniqueIdException.class)
//  public void getOrCreateIdAssignFilterBlocked() {
//    uid = new UniqueId(client, table, METRIC, 3);
//    final Config config = mock(Config.class);
//    when(config.enable_realtime_uid()).thenReturn(false);
//    final TSDB tsdb = mock(TSDB.class);
//    when(tsdb.getConfig()).thenReturn(config);
//    uid.setTSDB(tsdb);
//    final UniqueIdFilterPlugin filter = mock(UniqueIdFilterPlugin.class);
//    when(filter.fillterUIDAssignments()).thenReturn(true);
//    when(filter.allowUIDAssignment(any(UniqueIdType.class), anyString(), 
//        anyString(), anyMapOf(String.class, String.class)))
//      .thenReturn(Deferred.fromResult(false));
//    when(tsdb.getUidFilter()).thenReturn(filter);
//    
//    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
//            .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
//    // Watch this! ______,^   I'm writing C++ in Java!
//
//    when(client.atomicIncrement(incrementForRow(MAXID)))
//            .thenReturn(Deferred.fromResult(5L));
//
//    when(client.compareAndSet(anyPut(), emptyArray()))
//            .thenReturn(Deferred.fromResult(true))
//            .thenReturn(Deferred.fromResult(true));
//
//    uid.getOrCreateId("foo");
//  }
//
//  @Test(expected = RuntimeException.class)
//  public void getOrCreateIdAssignFilterReturnException() {
//    uid = new UniqueId(client, table, METRIC, 3);
//    final Config config = mock(Config.class);
//    when(config.enable_realtime_uid()).thenReturn(false);
//    final TSDB tsdb = mock(TSDB.class);
//    when(tsdb.getConfig()).thenReturn(config);
//    uid.setTSDB(tsdb);
//    final UniqueIdFilterPlugin filter = mock(UniqueIdFilterPlugin.class);
//    when(filter.fillterUIDAssignments()).thenReturn(true);
//    when(filter.allowUIDAssignment(any(UniqueIdType.class), anyString(), 
//        anyString(), anyMapOf(String.class, String.class)))
//      .thenReturn(Deferred.<Boolean>fromError(new UnitTestException()));
//    when(tsdb.getUidFilter()).thenReturn(filter);
//
//    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
//            .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
//    // Watch this! ______,^   I'm writing C++ in Java!
//
//    when(client.atomicIncrement(incrementForRow(MAXID)))
//            .thenReturn(Deferred.fromResult(5L));
//
//    when(client.compareAndSet(anyPut(), emptyArray()))
//            .thenReturn(Deferred.fromResult(true))
//            .thenReturn(Deferred.fromResult(true));
//
//    uid.getOrCreateId("foo");
//  }
//  
//  @Test(expected = RuntimeException.class)
//  public void getOrCreateIdAssignFilterThrowsException() {
//    uid = new UniqueId(client, table, METRIC, 3);
//    final Config config = mock(Config.class);
//    when(config.enable_realtime_uid()).thenReturn(false);
//    final TSDB tsdb = mock(TSDB.class);
//    when(tsdb.getConfig()).thenReturn(config);
//    uid.setTSDB(tsdb);
//    final UniqueIdFilterPlugin filter = mock(UniqueIdFilterPlugin.class);
//    when(filter.fillterUIDAssignments()).thenReturn(true);
//    when(filter.allowUIDAssignment(any(UniqueIdType.class), anyString(), 
//        anyString(), anyMapOf(String.class, String.class)))
//      .thenThrow(new UnitTestException());
//    when(tsdb.getUidFilter()).thenReturn(filter);
//
//    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
//            .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
//    // Watch this! ______,^   I'm writing C++ in Java!
//
//    when(client.atomicIncrement(incrementForRow(MAXID)))
//            .thenReturn(Deferred.fromResult(5L));
//
//    when(client.compareAndSet(anyPut(), emptyArray()))
//            .thenReturn(Deferred.fromResult(true))
//            .thenReturn(Deferred.fromResult(true));
//
//    uid.getOrCreateId("foo");
//  }
//
//  @Test
//  public void getOrCreateIdAsyncAssignFilterOK() throws Exception {
//    uid = new UniqueId(client, table, METRIC, 3);
//    final byte[] id = { 0, 0, 5 };
//    final Config config = mock(Config.class);
//    when(config.enable_realtime_uid()).thenReturn(false);
//    final TSDB tsdb = mock(TSDB.class);
//    when(tsdb.getConfig()).thenReturn(config);
//    uid.setTSDB(tsdb);
//    final UniqueIdFilterPlugin filter = mock(UniqueIdFilterPlugin.class);
//    when(filter.fillterUIDAssignments()).thenReturn(true);
//    when(filter.allowUIDAssignment(any(UniqueIdType.class), anyString(), 
//        anyString(), anyMapOf(String.class, String.class)))
//      .thenReturn(Deferred.fromResult(true));
//    when(tsdb.getUidFilter()).thenReturn(filter);
//    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
//      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
//    // Watch this! ______,^   I'm writing C++ in Java!
//
//    when(client.atomicIncrement(incrementForRow(MAXID)))
//      .thenReturn(Deferred.fromResult(5L));
//
//    when(client.compareAndSet(anyPut(), emptyArray()))
//      .thenReturn(Deferred.fromResult(true))
//      .thenReturn(Deferred.fromResult(true));
//
//    assertArrayEquals(id, uid.getOrCreateIdAsync("foo").join());
//    // Should be a cache hit since we created that entry.
//    assertArrayEquals(id, uid.getOrCreateIdAsync("foo").join());
//    // Should be a cache hit too for the same reason.
//    assertEquals("foo", uid.getName(id));
//    
//    verify(client).get(anyGet()); // Initial Get.
//    verify(client).atomicIncrement(incrementForRow(MAXID));
//    // Reverse + forward mappings.
//    verify(client, times(2)).compareAndSet(anyPut(), emptyArray());
//    verify(filter, times(1)).allowUIDAssignment(any(UniqueIdType.class), anyString(), 
//        anyString(), anyMapOf(String.class, String.class));
//  }
//  
//  @Test (expected = FailedToAssignUniqueIdException.class)
//  public void getOrCreateIdAsyncAssignFilterBlocked() throws Exception {
//    uid = new UniqueId(client, table, METRIC, 3);
//    final Config config = mock(Config.class);
//    when(config.enable_realtime_uid()).thenReturn(false);
//    final TSDB tsdb = mock(TSDB.class);
//    when(tsdb.getConfig()).thenReturn(config);
//    uid.setTSDB(tsdb);
//    final UniqueIdFilterPlugin filter = mock(UniqueIdFilterPlugin.class);
//    when(filter.fillterUIDAssignments()).thenReturn(true);
//    when(filter.allowUIDAssignment(any(UniqueIdType.class), anyString(), 
//        anyString(), anyMapOf(String.class, String.class)))
//      .thenReturn(Deferred.fromResult(false));
//    when(tsdb.getUidFilter()).thenReturn(filter);
//    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
//      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
//
//    when(client.atomicIncrement(incrementForRow(MAXID)))
//      .thenReturn(Deferred.fromResult(5L));
//
//    when(client.compareAndSet(anyPut(), emptyArray()))
//      .thenReturn(Deferred.fromResult(true))
//      .thenReturn(Deferred.fromResult(true));
//
//    uid.getOrCreateIdAsync("foo").join();
//  }
//  
//  @Test (expected = UnitTestException.class)
//  public void getOrCreateIdAsyncAssignFilterReturnException() throws Exception {
//    uid = new UniqueId(client, table, METRIC, 3);
//    final Config config = mock(Config.class);
//    when(config.enable_realtime_uid()).thenReturn(false);
//    final TSDB tsdb = mock(TSDB.class);
//    when(tsdb.getConfig()).thenReturn(config);
//    uid.setTSDB(tsdb);
//    final UniqueIdFilterPlugin filter = mock(UniqueIdFilterPlugin.class);
//    when(filter.fillterUIDAssignments()).thenReturn(true);
//    when(filter.allowUIDAssignment(any(UniqueIdType.class), anyString(), 
//        anyString(), anyMapOf(String.class, String.class)))
//      .thenReturn(Deferred.<Boolean>fromError(new UnitTestException()));
//    when(tsdb.getUidFilter()).thenReturn(filter);
//    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
//      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
//
//    when(client.atomicIncrement(incrementForRow(MAXID)))
//      .thenReturn(Deferred.fromResult(5L));
//
//    when(client.compareAndSet(anyPut(), emptyArray()))
//      .thenReturn(Deferred.fromResult(true))
//      .thenReturn(Deferred.fromResult(true));
//
//    uid.getOrCreateIdAsync("foo").join();
//  }
//  
//  @Test (expected = UnitTestException.class)
//  public void getOrCreateIdAsyncAssignFilterThrowsException() throws Exception {
//    uid = new UniqueId(client, table, METRIC, 3);
//    final Config config = mock(Config.class);
//    when(config.enable_realtime_uid()).thenReturn(false);
//    final TSDB tsdb = mock(TSDB.class);
//    when(tsdb.getConfig()).thenReturn(config);
//    uid.setTSDB(tsdb);
//    final UniqueIdFilterPlugin filter = mock(UniqueIdFilterPlugin.class);
//    when(filter.fillterUIDAssignments()).thenReturn(true);
//    when(filter.allowUIDAssignment(any(UniqueIdType.class), anyString(), 
//        anyString(),anyMapOf(String.class, String.class)))
//      .thenThrow(new UnitTestException());
//    when(tsdb.getUidFilter()).thenReturn(filter);
//    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
//      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
//
//    when(client.atomicIncrement(incrementForRow(MAXID)))
//      .thenReturn(Deferred.fromResult(5L));
//
//    when(client.compareAndSet(anyPut(), emptyArray()))
//      .thenReturn(Deferred.fromResult(true))
//      .thenReturn(Deferred.fromResult(true));
//
//    uid.getOrCreateIdAsync("foo").join();
//  }
//  
//  @Test  // Test the creation of an ID when unable to increment MAXID
//  public void getOrCreateIdUnableToIncrementMaxId() throws Exception {
//    PowerMockito.mockStatic(Thread.class);
//
//    uid = new UniqueId(client, table, METRIC, 3);
//
//    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
//      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
//    // Watch this! ______,^   I'm writing C++ in Java!
//
//    HBaseException hbe = fakeHBaseException();
//    when(client.atomicIncrement(incrementForRow(MAXID)))
//      .thenThrow(hbe);
//    PowerMockito.doNothing().when(Thread.class); Thread.sleep(anyInt());
//
//    try {
//      uid.getOrCreateId("foo");
//      fail("HBaseException should have been thrown!");
//    } catch (HBaseException e) {
//      assertSame(hbe, e);
//    }
//  }
//
//  @Test  // Test the creation of an ID with a race condition.
//  public void getOrCreateIdAssignIdWithRaceCondition() {
//    // Simulate a race between client A and client B.
//    // A does a Get and sees that there's no ID for this name.
//    // B does a Get and sees that there's no ID too, and B actually goes
//    // through the entire process to create the ID.
//    // Then A attempts to go through the process and should discover that the
//    // ID has already been assigned.
//
//    uid = new UniqueId(client, table, METRIC, 3); // Used by client A.
//    HBaseClient client_b = mock(HBaseClient.class); // For client B.
//    final UniqueId uid_b = new UniqueId(client_b, table, METRIC, 3);
//
//    final byte[] id = { 0, 0, 5 };
//    final byte[] byte_name = { 'f', 'o', 'o' };
//    final ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
//    kvs.add(new KeyValue(byte_name, ID, METRIC_ARRAY, id));
//    
//    final Deferred<ArrayList<KeyValue>> d = 
//      PowerMockito.spy(new Deferred<ArrayList<KeyValue>>());
//    when(client.get(anyGet()))
//      .thenReturn(d)
//      .thenReturn(Deferred.fromResult(kvs));
//
//    final Answer<byte[]> the_race = new Answer<byte[]>() {
//      public byte[] answer(final InvocationOnMock unused_invocation) throws Exception {
//        // While answering A's first Get, B doest a full getOrCreateId.
//        assertArrayEquals(id, uid_b.getOrCreateId("foo"));
//        d.callback(null);
//        return (byte[]) ((Deferred) d).join();
//      }
//    };
//
//    // Start the race when answering A's first Get.
//    try {
//      PowerMockito.doAnswer(the_race).when(d).joinUninterruptibly();
//    } catch (Exception e) {
//      fail("Should never happen: " + e);
//    }
//
//    when(client_b.get(anyGet())) // null => ID doesn't exist.
//      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
//    // Watch this! ______,^ I'm writing C++ in Java!
//
//    when(client_b.atomicIncrement(incrementForRow(MAXID)))
//      .thenReturn(Deferred.fromResult(5L));
//
//    when(client_b.compareAndSet(anyPut(), emptyArray()))
//      .thenReturn(Deferred.fromResult(true))
//      .thenReturn(Deferred.fromResult(true));
//
//    // Now that B is finished, A proceeds and allocates a UID that will be
//    // wasted, and creates the reverse mapping, but fails at creating the
//    // forward mapping.
//    when(client.atomicIncrement(incrementForRow(MAXID)))
//      .thenReturn(Deferred.fromResult(6L));
//
//    when(client.compareAndSet(anyPut(), emptyArray()))
//      .thenReturn(Deferred.fromResult(true)) // Orphan reverse mapping.
//      .thenReturn(Deferred.fromResult(false)); // Already CAS'ed by A.
//
//    // Start the execution.
//    assertArrayEquals(id, uid.getOrCreateId("foo"));
//
//    // Verify the order of execution too.
//    final InOrder order = inOrder(client, client_b);
//    order.verify(client).get(anyGet()); // 1st Get for A.
//    order.verify(client_b).get(anyGet()); // 1st Get for B.
//    order.verify(client_b).atomicIncrement(incrementForRow(MAXID));
//    order.verify(client_b, times(2)).compareAndSet(anyPut(), // both mappings.
//                                                   emptyArray());
//    order.verify(client).atomicIncrement(incrementForRow(MAXID));
//    order.verify(client, times(2)).compareAndSet(anyPut(), // both mappings.
//                                                 emptyArray());
//    order.verify(client).get(anyGet()); // A retries and gets it.
//  }
//
//  @Test
//  // Test the creation of an ID when all possible IDs are already in use
//  public void getOrCreateIdWithOverflow() {
//    uid = new UniqueId(client, table, METRIC, 1);  // IDs are only on 1 byte.
//
//    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
//      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
//    // Watch this! ______,^   I'm writing C++ in Java!
//
//    // Update once HBASE-2292 is fixed:
//    when(client.atomicIncrement(incrementForRow(MAXID)))
//      .thenReturn(Deferred.fromResult(256L));
//
//    try {
//      final byte[] id = uid.getOrCreateId("foo");
//      fail("IllegalArgumentException should have been thrown but instead "
//           + " this was returned id=" + Arrays.toString(id));
//    } catch (IllegalStateException e) {
//      // OK.
//    }
//
//    verify(client, times(1)).get(anyGet());  // Initial Get.
//    verify(client).atomicIncrement(incrementForRow(MAXID));
//  }
//
//  @Test  // ICV throws an exception, we can't get an ID.
//  public void getOrCreateIdWithICVFailure() {
//    uid = new UniqueId(client, table, METRIC, 3);
//    final Config config = mock(Config.class);
//    when(config.enable_realtime_uid()).thenReturn(false);
//    final TSDB tsdb = mock(TSDB.class);
//    when(tsdb.getConfig()).thenReturn(config);
//    uid.setTSDB(tsdb);
//    
//    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
//      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
//    // Watch this! ______,^   I'm writing C++ in Java!
//
//    // Update once HBASE-2292 is fixed:
//    HBaseException hbe = fakeHBaseException();
//    when(client.atomicIncrement(incrementForRow(MAXID)))
//      .thenReturn(Deferred.<Long>fromError(hbe))
//      .thenReturn(Deferred.fromResult(5L));
//
//    when(client.compareAndSet(anyPut(), emptyArray()))
//      .thenReturn(Deferred.fromResult(true))
//      .thenReturn(Deferred.fromResult(true));
//
//    final byte[] id = { 0, 0, 5 };
//    assertArrayEquals(id, uid.getOrCreateId("foo"));
//    verify(client, times(1)).get(anyGet()); // Initial Get.
//    // First increment (failed) + retry.
//    verify(client, times(2)).atomicIncrement(incrementForRow(MAXID));
//    // Reverse + forward mappings.
//    verify(client, times(2)).compareAndSet(anyPut(), emptyArray());
//  }
//
//  @Test  // Test that the reverse mapping is created before the forward one.
//  public void getOrCreateIdPutsReverseMappingFirst() {
//    uid = new UniqueId(client, table, METRIC, 3);
//    final Config config = mock(Config.class);
//    when(config.enable_realtime_uid()).thenReturn(false);
//    final TSDB tsdb = mock(TSDB.class);
//    when(tsdb.getConfig()).thenReturn(config);
//    uid.setTSDB(tsdb);
//    
//    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
//      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
//    // Watch this! ______,^   I'm writing C++ in Java!
//
//    when(client.atomicIncrement(incrementForRow(MAXID)))
//      .thenReturn(Deferred.fromResult(6L));
//
//    when(client.compareAndSet(anyPut(), emptyArray()))
//      .thenReturn(Deferred.fromResult(true))
//      .thenReturn(Deferred.fromResult(true));
//
//    final byte[] id = { 0, 0, 6 };
//    final byte[] row = { 'f', 'o', 'o' };
//    assertArrayEquals(id, uid.getOrCreateId("foo"));
//
//    final InOrder order = inOrder(client);
//    order.verify(client).get(anyGet());            // Initial Get.
//    order.verify(client).atomicIncrement(incrementForRow(MAXID));
//    order.verify(client).compareAndSet(putForRow(id), emptyArray());
//    order.verify(client).compareAndSet(putForRow(row), emptyArray());
//  }
//  
//  @Test
//  public void getOrCreateIdRandom() {
//    PowerMockito.mockStatic(RandomUniqueId.class);
//    uid = new UniqueId(client, table, METRIC, 3, true);
//    final long id = 42L;
//    final byte[] id_array = { 0, 0, 0x2A };
//
//    when(RandomUniqueId.getRandomUID()).thenReturn(id);
//    when(client.get(any(GetRequest.class)))
//      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
//    
//    when(client.compareAndSet(any(PutRequest.class), any(byte[].class)))
//      .thenReturn(Deferred.fromResult(true))
//      .thenReturn(Deferred.fromResult(true));
//
//    assertArrayEquals(id_array, uid.getOrCreateId("foo"));
//    // Should be a cache hit ...
//    assertArrayEquals(id_array, uid.getOrCreateId("foo"));
//    assertEquals(1, uid.cacheHits());
//    assertEquals(1, uid.cacheMisses());
//    assertEquals(2, uid.cacheSize());
//    assertEquals(0, uid.randomIdCollisions());
//    // ... so verify there was only one HBase Get.
//    verify(client).get(any(GetRequest.class));
//  }
//  
//  @Test
//  public void getOrCreateIdRandomCollision() {
//    PowerMockito.mockStatic(RandomUniqueId.class);
//    uid = new UniqueId(client, table, METRIC, 3, true);
//    final long id = 42L;
//    final byte[] id_array = { 0, 0, 0x2A };
//
//    when(RandomUniqueId.getRandomUID()).thenReturn(24L).thenReturn(id);
//    
//    when(client.get(any(GetRequest.class)))
//      .thenReturn(Deferred.fromResult((ArrayList<KeyValue>)null));
//    
//    when(client.compareAndSet(anyPut(), any(byte[].class)))
//      .thenReturn(Deferred.fromResult(false))
//      .thenReturn(Deferred.fromResult(true))
//      .thenReturn(Deferred.fromResult(true));
//
//    assertArrayEquals(id_array, uid.getOrCreateId("foo"));
//    // Should be a cache hit ...
//    assertArrayEquals(id_array, uid.getOrCreateId("foo"));
//    assertEquals(1, uid.cacheHits());
//    assertEquals(1, uid.cacheMisses());
//    assertEquals(2, uid.cacheSize());
//    assertEquals(1, uid.randomIdCollisions());
//
//    // ... so verify there was only one HBase Get.
//    verify(client).get(anyGet());
//  }
//  
//  @Test
//  public void getOrCreateIdRandomCollisionTooManyAttempts() {
//    PowerMockito.mockStatic(RandomUniqueId.class);
//    uid = new UniqueId(client, table, METRIC, 3, true);
//    final long id = 42L;
//
//    when(RandomUniqueId.getRandomUID()).thenReturn(24L).thenReturn(id);
//    
//    when(client.get(any(GetRequest.class)))
//      .thenReturn(Deferred.fromResult((ArrayList<KeyValue>)null));
//    
//    when(client.compareAndSet(any(PutRequest.class), any(byte[].class)))
//      .thenReturn(Deferred.fromResult(false))
//      .thenReturn(Deferred.fromResult(false))
//      .thenReturn(Deferred.fromResult(false))
//      .thenReturn(Deferred.fromResult(false))
//      .thenReturn(Deferred.fromResult(false))
//      .thenReturn(Deferred.fromResult(false))
//      .thenReturn(Deferred.fromResult(false))
//      .thenReturn(Deferred.fromResult(false))
//      .thenReturn(Deferred.fromResult(false))
//      .thenReturn(Deferred.fromResult(false));
//
//    try {
//      final byte[] assigned_id = uid.getOrCreateId("foo");
//      fail("FailedToAssignUniqueIdException should have been thrown but instead "
//           + " this was returned id=" + Arrays.toString(assigned_id));
//    } catch (FailedToAssignUniqueIdException e) {
//      // OK
//    }
//    assertEquals(0, uid.cacheHits());
//    assertEquals(1, uid.cacheMisses());
//    assertEquals(0, uid.cacheSize());
//    assertEquals(9, uid.randomIdCollisions());
//
//    // ... so verify there was only one HBase Get.
//    verify(client).get(any(GetRequest.class));
//  }
//  
//  @Test
//  public void getOrCreateIdRandomWithRaceCondition() {
//    PowerMockito.mockStatic(RandomUniqueId.class);
//    uid = new UniqueId(client, table, METRIC, 3, true);
//    final long id = 24L;
//    final byte[] id_array = { 0, 0, 0x2A };
//    final byte[] byte_name = { 'f', 'o', 'o' };
//    
//    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
//    kvs.add(new KeyValue(byte_name, ID, METRIC_ARRAY, id_array));
//    
//    when(RandomUniqueId.getRandomUID()).thenReturn(id);
//    
//    when(client.get(any(GetRequest.class)))
//      .thenReturn(Deferred.fromResult((ArrayList<KeyValue>)null))
//      .thenReturn(Deferred.fromResult(kvs));
//    
//    when(client.compareAndSet(any(PutRequest.class), any(byte[].class)))
//      .thenReturn(Deferred.fromResult(true))
//      .thenReturn(Deferred.fromResult(false));
//
//    assertArrayEquals(id_array, uid.getOrCreateId("foo"));
//    assertEquals(0, uid.cacheHits());
//    assertEquals(2, uid.cacheMisses());
//    assertEquals(2, uid.cacheSize());
//    assertEquals(1, uid.randomIdCollisions());
//
//    // ... so verify there was only one HBase Get.
//    verify(client, times(2)).get(any(GetRequest.class));
//  }
//  
//  @Test
//  public void suggestWithNoMatch() {
//    uid = new UniqueId(client, table, METRIC, 3);
//
//    final Scanner fake_scanner = mock(Scanner.class);
//    when(client.newScanner(table))
//      .thenReturn(fake_scanner);
//
//    when(fake_scanner.nextRows())
//      .thenReturn(Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));
//    // Watch this! ______,^   I'm writing C++ in Java!
//
//    final List<String> suggestions = uid.suggest("nomatch");
//    assertEquals(0, suggestions.size());  // No results.
//
//    verify(fake_scanner).setStartKey("nomatch".getBytes());
//    verify(fake_scanner).setStopKey("nomatci".getBytes());
//    verify(fake_scanner).setFamily(ID);
//    verify(fake_scanner).setQualifier(METRIC_ARRAY);
//  }
//  
//  @Test
//  public void suggestWithMatches() {
//    uid = new UniqueId(client, table, METRIC, 3);
//
//    final Scanner fake_scanner = mock(Scanner.class);
//    when(client.newScanner(table))
//      .thenReturn(fake_scanner);
//
//    final ArrayList<ArrayList<KeyValue>> rows = new ArrayList<ArrayList<KeyValue>>(2);
//    final byte[] foo_bar_id = { 0, 0, 1 };
//    {
//      ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
//      row.add(new KeyValue("foo.bar".getBytes(), ID, METRIC_ARRAY, foo_bar_id));
//      rows.add(row);
//      row = new ArrayList<KeyValue>(1);
//      row.add(new KeyValue("foo.baz".getBytes(), ID, METRIC_ARRAY,
//                           new byte[] { 0, 0, 2 }));
//      rows.add(row);
//    }
//    when(fake_scanner.nextRows())
//      .thenReturn(Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(rows))
//      .thenReturn(Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));
//    // Watch this! ______,^   I'm writing C++ in Java!
//
//    final List<String> suggestions = uid.suggest("foo");
//    final ArrayList<String> expected = new ArrayList<String>(2);
//    expected.add("foo.bar");
//    expected.add("foo.baz");
//    assertEquals(expected, suggestions);
//    // Verify that we cached the forward + backwards mapping for both results
//    // we "discovered" as a result of the scan.
//    assertEquals(4, uid.cacheSize());
//    assertEquals(0, uid.cacheHits());
//
//    // Verify that the cached results are usable.
//    // Should be a cache hit ...
//    assertArrayEquals(foo_bar_id, uid.getOrCreateId("foo.bar"));
//    assertEquals(1, uid.cacheHits());
//    // ... so verify there was no HBase Get.
//    verify(client, never()).get(anyGet());
//  }
//
//  @Test
//  public void uidToString() {
//    assertEquals("01", UniqueId.uidToString(new byte[] { 1 }));
//  }
//  
//  @Test
//  public void uidToString2() {
//    assertEquals("0A0B", UniqueId.uidToString(new byte[] { 10, 11 }));
//  }
//  
//  @Test
//  public void uidToString3() {
//    assertEquals("1A1B", UniqueId.uidToString(new byte[] { 26, 27 }));
//  }
//  
//  @Test
//  public void uidToStringZeros() {
//    assertEquals("00", UniqueId.uidToString(new byte[] { 0 }));
//  }
//  
//  @Test
//  public void uidToString255() {
//    assertEquals("FF", UniqueId.uidToString(new byte[] { (byte) 255 }));
//  }
//  
//  @Test (expected = NullPointerException.class)
//  public void uidToStringNull() {
//    UniqueId.uidToString(null);
//  }
//  
//  @Test
//  public void stringToUid() {
//    assertArrayEquals(new byte[] { 0x0a, 0x0b }, UniqueId.stringToUid("0A0B"));
//  }
//  
//  @Test
//  public void stringToUidNormalize() {
//    assertArrayEquals(new byte[] { (byte) 171 }, UniqueId.stringToUid("AB"));
//  }
//  
//  @Test
//  public void stringToUidCase() {
//    assertArrayEquals(new byte[] { (byte) 11 }, UniqueId.stringToUid("B"));
//  }
//  
//  @Test
//  public void stringToUidWidth() {
//    assertArrayEquals(new byte[] { (byte) 0, (byte) 42, (byte) 12 }, 
//        UniqueId.stringToUid("2A0C", (short)3));
//  }
//  
//  @Test
//  public void stringToUidWidth2() {
//    assertArrayEquals(new byte[] { (byte) 0, (byte) 0, (byte) 0 }, 
//        UniqueId.stringToUid("0", (short)3));
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void stringToUidNull() {
//    UniqueId.stringToUid(null);
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void stringToUidEmpty() {
//    UniqueId.stringToUid("");
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void stringToUidNotHex() {
//    UniqueId.stringToUid("HelloWorld");
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void stringToUidNotHex2() {
//    UniqueId.stringToUid(" ");
//  }
//  
//  @Test
//  public void getTSUIDFromKey() {
//    final byte[] tsuid = UniqueId.getTSUIDFromKey(new byte[] 
//      { 0, 0, 1, 1, 1, 1, 1, 0, 0, 2, 0, 0, 3 }, (short)3, (short)4);
//    assertArrayEquals(new byte[] { 0, 0, 1, 0, 0, 2, 0, 0, 3 }, 
//        tsuid);
//  }
//  
//  @Test
//  public void getTSUIDFromKeySalted() {
//    PowerMockito.mockStatic(Const.class);
//    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
//    
//    final byte[] expected = { 0, 0, 1, 0, 0, 2, 0, 0, 3 };
//    byte[] tsuid = UniqueId.getTSUIDFromKey(new byte[] 
//      { 0, 0, 0, 1, 1, 1, 1, 1, 0, 0, 2, 0, 0, 3 }, (short)3, (short)4);
//    assertArrayEquals(expected, tsuid);
//    
//    tsuid = UniqueId.getTSUIDFromKey(new byte[] 
//      { 1, 0, 0, 1, 1, 1, 1, 1, 0, 0, 2, 0, 0, 3 }, (short)3, (short)4);
//    assertArrayEquals(expected, tsuid);
//    
//    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(4);
//    tsuid = UniqueId.getTSUIDFromKey(new byte[] 
//      { 1, 2, 3, 4, 0, 0, 1, 1, 1, 1, 1, 0, 0, 2, 0, 0, 3 }, (short)3, (short)4);
//    assertArrayEquals(expected, tsuid);
//    
//    tsuid = UniqueId.getTSUIDFromKey(new byte[] 
//      { 4, 3, 2, 1, 0, 0, 1, 1, 1, 1, 1, 0, 0, 2, 0, 0, 3 }, (short)3, (short)4);
//    assertArrayEquals(expected, tsuid);
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void getTSUIDFromKeyMissingTags() {
//    UniqueId.getTSUIDFromKey(new byte[] 
//      { 0, 0, 1, 1, 1, 1, 1 }, (short)3, (short)4);
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void getTSUIDFromKeyMissingTagsSalted() {
//    PowerMockito.mockStatic(Const.class);
//    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1); 
//    
//    UniqueId.getTSUIDFromKey(new byte[] 
//      { 0, 0, 0, 1, 1, 1, 1, 1 }, (short)3, (short)4);
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void getTSUIDFromKeyMissingSalt() {
//    PowerMockito.mockStatic(Const.class);
//    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
//    
//    UniqueId.getTSUIDFromKey(new byte[] 
//        { 0, 0, 1, 1, 1, 1, 1, 0, 0, 2, 0, 0, 3 }, (short)3, (short)4);
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void getTSUIDFromKeySaltButShouldntBe() {
//    UniqueId.getTSUIDFromKey(new byte[] 
//        { 1, 0, 0, 1, 1, 1, 1, 1, 0, 0, 2, 0, 0, 3 }, (short)3, (short)4);
//  }
//  
//  @Test
//  public void getTagPairsFromTSUIDString() {
//    List<byte[]> tags = UniqueId.getTagPairsFromTSUID(
//        "000000000001000002000003000004");
//    assertNotNull(tags);
//    assertEquals(2, tags.size());
//    assertArrayEquals(new byte[] { 0, 0, 1, 0, 0, 2 }, tags.get(0));
//    assertArrayEquals(new byte[] { 0, 0, 3, 0, 0, 4 }, tags.get(1));
//  }
//  
//  @Test
//  public void getTagPairsFromTSUIDStringNonStandardWidth() {
//    PowerMockito.mockStatic(TSDB.class);
//    when(TSDB.metrics_width()).thenReturn((short)3);
//    when(TSDB.tagk_width()).thenReturn((short)4);
//    when(TSDB.tagv_width()).thenReturn((short)3);
//    
//    List<byte[]> tags = UniqueId.getTagPairsFromTSUID(
//        "0000000000000100000200000003000004");
//    assertNotNull(tags);
//    assertEquals(2, tags.size());
//    assertArrayEquals(new byte[] { 0, 0, 0, 1, 0, 0, 2 }, tags.get(0));
//    assertArrayEquals(new byte[] { 0, 0, 0, 3, 0, 0, 4 }, tags.get(1));
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void getTagPairsFromTSUIDStringMissingTags() {
//    UniqueId.getTagPairsFromTSUID("123456");
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void getTagPairsFromTSUIDStringMissingMetric() {
//    UniqueId.getTagPairsFromTSUID("000001000002");
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void getTagPairsFromTSUIDStringOddNumberOfCharacters() {
//    UniqueId.getTagPairsFromTSUID("0000080000010000020");
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void getTagPairsFromTSUIDStringMissingTagv() {
//    UniqueId.getTagPairsFromTSUID("000008000001");
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void getTagPairsFromTSUIDStringNull() {
//    UniqueId.getTagPairsFromTSUID((String)null);
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void getTagPairsFromTSUIDStringEmpty() {
//    UniqueId.getTagPairsFromTSUID("");
//  }
//  
//  @Test
//  public void getTagPairsFromTSUIDBytes() {
//    List<byte[]> tags = UniqueId.getTagPairsFromTSUID(
//        new byte[] { 0, 0, 0, 0, 0, 1, 0, 0, 2, 0, 0, 3, 0, 0, 4 });
//    assertNotNull(tags);
//    assertEquals(2, tags.size());
//    assertArrayEquals(new byte[] { 0, 0, 1, 0, 0, 2 }, tags.get(0));
//    assertArrayEquals(new byte[] { 0, 0, 3, 0, 0, 4 }, tags.get(1));
//  }
//  
//  @Test
//  public void getTagPairsFromTSUIDBytesNonStandardWidth() {
//    PowerMockito.mockStatic(TSDB.class);
//    when(TSDB.metrics_width()).thenReturn((short)3);
//    when(TSDB.tagk_width()).thenReturn((short)4);
//    when(TSDB.tagv_width()).thenReturn((short)3);
//    
//    List<byte[]> tags = UniqueId.getTagPairsFromTSUID(
//        new byte[] { 0, 0, 0, 0, 0, 0, 1, 0, 0, 2, 0, 0, 0, 3, 0, 0, 4 });
//    assertNotNull(tags);
//    assertEquals(2, tags.size());
//    assertArrayEquals(new byte[] { 0, 0, 0, 1, 0, 0, 2 }, tags.get(0));
//    assertArrayEquals(new byte[] { 0, 0, 0, 3, 0, 0, 4 }, tags.get(1));
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void getTagPairsFromTSUIDBytesMissingTags() {
//    UniqueId.getTagPairsFromTSUID(new byte[] { 0, 0, 1 });
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void getTagPairsFromTSUIDBytesMissingMetric() {
//    UniqueId.getTagPairsFromTSUID(new byte[] { 0, 0, 1, 0, 0, 2 });
//  }
//
//  @Test (expected = IllegalArgumentException.class)
//  public void getTagPairsFromTSUIDBytesMissingTagv() {
//    UniqueId.getTagPairsFromTSUID(new byte[] { 0, 0, 8, 0, 0, 2 });
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void getTagPairsFromTSUIDBytesNull() {
//    UniqueId.getTagPairsFromTSUID((byte[])null);
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void getTagPairsFromTSUIDBytesEmpty() {
//    UniqueId.getTagPairsFromTSUID(new byte[0]);
//  }
//  
//  @Test
//  public void getTagFromTSUID() {
//    List<byte[]> tags = UniqueId.getTagsFromTSUID(
//        "000000000001000002000003000004");
//    assertNotNull(tags);
//    assertEquals(4, tags.size());
//    assertArrayEquals(new byte[] { 0, 0, 1 }, tags.get(0));
//    assertArrayEquals(new byte[] { 0, 0, 2 }, tags.get(1));
//    assertArrayEquals(new byte[] { 0, 0, 3 }, tags.get(2));
//    assertArrayEquals(new byte[] { 0, 0, 4 }, tags.get(3));
//  }
//  
//  @Test
//  public void getTagFromTSUIDNonStandardWidth() {
//    PowerMockito.mockStatic(TSDB.class);
//    when(TSDB.metrics_width()).thenReturn((short)3);
//    when(TSDB.tagk_width()).thenReturn((short)4);
//    when(TSDB.tagv_width()).thenReturn((short)3);
//    
//    List<byte[]> tags = UniqueId.getTagsFromTSUID(
//        "0000000000000100000200000003000004");
//    assertNotNull(tags);
//    assertEquals(4, tags.size());
//    assertArrayEquals(new byte[] { 0, 0, 0, 1 }, tags.get(0));
//    assertArrayEquals(new byte[] { 0, 0, 2 }, tags.get(1));
//    assertArrayEquals(new byte[] { 0, 0, 0, 3 }, tags.get(2));
//    assertArrayEquals(new byte[] { 0, 0, 4 }, tags.get(3));
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void getTagFromTSUIDMissingTags() {
//    UniqueId.getTagsFromTSUID("123456");
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void getTagFromTSUIDMissingMetric() {
//    UniqueId.getTagsFromTSUID("000001000002");
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void getTagFromTSUIDOddNumberOfCharacters() {
//    UniqueId.getTagsFromTSUID("0000080000010000020");
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void getTagFromTSUIDMissingTagv() {
//    UniqueId.getTagsFromTSUID("000008000001");
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void getTagFromTSUIDNull() {
//    UniqueId.getTagsFromTSUID(null);
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void getTagFromTSUIDEmpty() {
//    UniqueId.getTagsFromTSUID("");
//  }
//  
//  @Test
//  public void getUsedUIDs() throws Exception {
//    final ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(3);
//    final byte[] metrics = { 'm', 'e', 't', 'r', 'i', 'c', 's' };
//    final byte[] tagk = { 't', 'a', 'g', 'k' };
//    final byte[] tagv = { 't', 'a', 'g', 'v' };
//    kvs.add(new KeyValue(MAXID, ID, metrics, Bytes.fromLong(64L)));
//    kvs.add(new KeyValue(MAXID, ID, tagk, Bytes.fromLong(42L)));
//    kvs.add(new KeyValue(MAXID, ID, tagv, Bytes.fromLong(1024L)));
//    final TSDB tsdb = mock(TSDB.class);
//    when(tsdb.getClient()).thenReturn(client);
//    when(tsdb.uidTable()).thenReturn(new byte[] { 'u', 'i', 'd' });
//    when(client.get(anyGet()))
//      .thenReturn(Deferred.fromResult(kvs));
//    
//    final byte[][] kinds = { metrics, tagk, tagv };
//    final Map<String, Long> uids = UniqueId.getUsedUIDs(tsdb, kinds)
//      .joinUninterruptibly();
//    assertNotNull(uids);
//    assertEquals(3, uids.size());
//    assertEquals(64L, uids.get("metrics").longValue());
//    assertEquals(42L, uids.get("tagk").longValue());
//    assertEquals(1024L, uids.get("tagv").longValue());
//  }
//  
//  @Test
//  public void getUsedUIDsEmptyRow() throws Exception {
//    final ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(0);
//    final byte[] metrics = { 'm', 'e', 't', 'r', 'i', 'c', 's' };
//    final byte[] tagk = { 't', 'a', 'g', 'k' };
//    final byte[] tagv = { 't', 'a', 'g', 'v' };
//    final TSDB tsdb = mock(TSDB.class);
//    when(tsdb.getClient()).thenReturn(client);
//    when(tsdb.uidTable()).thenReturn(new byte[] { 'u', 'i', 'd' });
//    when(client.get(anyGet()))
//      .thenReturn(Deferred.fromResult(kvs));
//    
//    final byte[][] kinds = { metrics, tagk, tagv };
//    final Map<String, Long> uids = UniqueId.getUsedUIDs(tsdb, kinds)
//      .joinUninterruptibly();
//    assertNotNull(uids);
//    assertEquals(3, uids.size());
//    assertEquals(0L, uids.get("metrics").longValue());
//    assertEquals(0L, uids.get("tagk").longValue());
//    assertEquals(0L, uids.get("tagv").longValue());
//  }
//  
//  @Test
//  public void uidToLong() throws Exception {
//    assertEquals(42, UniqueId.uidToLong(new byte[] { 0, 0, 0x2A }, (short)3));
//  }
//
//  @Test
//  public void uidToLongFromString() throws Exception {
//    assertEquals(42L, UniqueId.uidToLong("00002A", (short) 3));
//  }
//
//  @Test (expected = IllegalArgumentException.class)
//  public void uidToLongTooLong() throws Exception {
//    UniqueId.uidToLong(new byte[] { 0, 0, 0, 0x2A }, (short)3);
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void uidToLongTooShort() throws Exception {
//    UniqueId.uidToLong(new byte[] { 0, 0x2A }, (short)3);
//  }
//  
//  @Test (expected = NullPointerException.class)
//  public void uidToLongNull() throws Exception {
//    UniqueId.uidToLong((byte[])null, (short)3);
//  }
//  
//  @Test
//  public void longToUID() throws Exception {
//    assertArrayEquals(new byte[] { 0, 0, 0x2A }, 
//        UniqueId.longToUID(42L, (short)3));
//  }
//  
//  @Test (expected = IllegalStateException.class)
//  public void longToUIDTooBig() throws Exception {
//    UniqueId.longToUID(257, (short)1);
//  }
//
//  @Test
//  public void rename() throws Exception {
//    uid = new UniqueId(client, table, METRIC, 3);
//    final byte[] foo_id = { 0, 'a', 0x42 };
//    final byte[] foo_name = { 'f', 'o', 'o' };
//
//    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
//    kvs.add(new KeyValue(foo_name, ID, METRIC_ARRAY, foo_id));
//    when(client.get(anyGet()))
//        .thenReturn(Deferred.fromResult(kvs))
//        .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
//    when(client.put(anyPut())).thenAnswer(answerTrue());
//    when(client.delete(anyDelete())).thenAnswer(answerTrue());
//
//    uid.rename("foo", "bar");
//  }
//
//  @Test (expected = IllegalArgumentException.class)
//  public void renameNewNameExists() throws Exception {
//    uid = new UniqueId(client, table, METRIC, 3);
//    final byte[] foo_id = { 0, 'a', 0x42 };
//    final byte[] foo_name = { 'f', 'o', 'o' };
//    final byte[] bar_id = { 1, 'b', 0x43 };
//    final byte[] bar_name = { 'b', 'a', 'r' };
//
//    ArrayList<KeyValue> foo_kvs = new ArrayList<KeyValue>(1);
//    ArrayList<KeyValue> bar_kvs = new ArrayList<KeyValue>(1);
//    foo_kvs.add(new KeyValue(foo_name, ID, METRIC_ARRAY, foo_id));
//    bar_kvs.add(new KeyValue(bar_name, ID, METRIC_ARRAY, bar_id));
//    when(client.get(anyGet()))
//        .thenReturn(Deferred.fromResult(foo_kvs))
//        .thenReturn(Deferred.fromResult(bar_kvs));
//    when(client.put(anyPut())).thenAnswer(answerTrue());
//    when(client.delete(anyDelete())).thenAnswer(answerTrue());
//
//    uid.rename("foo", "bar");
//  }
//
//  @Test (expected = IllegalStateException.class)
//  public void renameRaceCondition() throws Exception {
//    // Simulate a race between client A(default) and client B.
//    // A and B rename same UID to different name.
//    // B waits till A start to invoke PutRequest to start.
//
//    uid = new UniqueId(client, table, METRIC, 3);
//    HBaseClient client_b = mock(HBaseClient.class);
//    final UniqueId uid_b = new UniqueId(client_b, table, METRIC, 3);
//
//    final byte[] foo_id = { 0, 'a', 0x42 };
//    final byte[] foo_name = { 'f', 'o', 'o' };
//
//    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
//    kvs.add(new KeyValue(foo_name, ID, METRIC_ARRAY, foo_id));
//
//    when(client_b.get(anyGet()))
//        .thenReturn(Deferred.fromResult(kvs))
//        .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
//    when(client_b.put(anyPut())).thenAnswer(answerTrue());
//    when(client_b.delete(anyDelete())).thenAnswer(answerTrue());
//
//    final Answer<Deferred<Boolean>> the_race = new Answer<Deferred<Boolean>>() {
//      public Deferred<Boolean> answer(final InvocationOnMock inv) throws Exception {
//        uid_b.rename("foo", "xyz");
//        return Deferred.fromResult(true);
//      }
//    };
//
//    when(client.get(anyGet()))
//        .thenReturn(Deferred.fromResult(kvs))
//        .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
//    when(client.put(anyPut())).thenAnswer(the_race);
//    when(client.delete(anyDelete())).thenAnswer(answerTrue());
//
//    uid.rename("foo", "bar");
//  }
//
//  @Test
//  public void deleteCached() throws Exception {
//    setupStorage();
//    uid = new UniqueId(client, table, METRIC, 3);
//    uid.setTSDB(tsdb);
//    assertArrayEquals(UID, uid.getId("sys.cpu.user"));
//    assertEquals("sys.cpu.user", uid.getName(UID));
//    
//    uid.deleteAsync("sys.cpu.user").join();
//    try {
//      uid.getId("sys.cpu.user");
//      fail("Expected a NoSuchUniqueName");
//    } catch (NoSuchUniqueName nsun) { }
//    
//    try {
//      uid.getName(UID);
//      fail("Expected a NoSuchUniqueId");
//    } catch (NoSuchUniqueId nsui) { }
//    
//    uid = new UniqueId(client, table, TAGK, 3);
//    uid.setTSDB(tsdb);
//    assertArrayEquals(UID, uid.getId("host"));
//    assertEquals("host", uid.getName(UID));
//    
//    uid = new UniqueId(client, table, TAGV, 3);
//    uid.setTSDB(tsdb);
//    assertArrayEquals(UID, uid.getId("web01"));
//    assertEquals("web01", uid.getName(UID));
//  }
//  
//  @Test
//  public void deleteNotCached() throws Exception {
//    setupStorage();
//    uid = new UniqueId(client, table, METRIC, 3);
//    uid.setTSDB(tsdb);
//    uid.deleteAsync("sys.cpu.user").join();
//    try {
//      uid.getId("sys.cpu.user");
//      fail("Expected a NoSuchUniqueName");
//    } catch (NoSuchUniqueName nsun) { }
//    
//    try {
//      uid.getName(UID);
//      fail("Expected a NoSuchUniqueId");
//    } catch (NoSuchUniqueId nsui) { }
//    
//    uid = new UniqueId(client, table, TAGK, 3);
//    uid.setTSDB(tsdb);
//    assertArrayEquals(UID, uid.getId("host"));
//    assertEquals("host", uid.getName(UID));
//    
//    uid = new UniqueId(client, table, TAGV, 3);
//    uid.setTSDB(tsdb);
//    assertArrayEquals(UID, uid.getId("web01"));
//    assertEquals("web01", uid.getName(UID));
//  }
//  
//  @Test
//  public void deleteFailForwardDelete() throws Exception {
//    setupStorage();
//    uid = new UniqueId(client, table, METRIC, 3);
//    uid.setTSDB(tsdb);
//    assertArrayEquals(UID, uid.getId("sys.cpu.user"));
//    assertEquals("sys.cpu.user", uid.getName(UID));
//    
//    storage.throwException("sys.cpu.user".getBytes(), fakeHBaseException());
//    try {
//      uid.deleteAsync("sys.cpu.user").join();
//      fail("Expected HBaseException");
//    } catch (HBaseException e) { }
//    catch (Exception e) { }
//    storage.clearExceptions();
//    try {
//      uid.getName(UID);
//      fail("Expected a NoSuchUniqueId");
//    } catch (NoSuchUniqueId nsui) { }
//    assertArrayEquals(UID, uid.getId("sys.cpu.user"));
//    // now it pollutes the cache
//    assertEquals("sys.cpu.user", uid.getName(UID));
//  }
//  
//  @Test
//  public void deleteFailReverseDelete() throws Exception {
//    setupStorage();
//    storage.throwException(UID, fakeHBaseException());
//    uid = new UniqueId(client, table, METRIC, 3);
//    uid.setTSDB(tsdb);
//    try {
//      uid.deleteAsync("sys.cpu.user").join();
//      fail("Expected HBaseException");
//    } catch (HBaseException e) { }
//    catch (Exception e) { }
//    storage.clearExceptions();
//    try {
//      uid.getId("sys.cpu.user");
//      fail("Expected a NoSuchUniqueName");
//    } catch (NoSuchUniqueName nsun) { }
//    assertEquals("sys.cpu.user", uid.getName(UID));
//  }
//  
//  @Test
//  public void deleteNoSuchUniqueName() throws Exception {
//    setupStorage();
//    uid = new UniqueId(client, table, METRIC, 3);
//    uid.setTSDB(tsdb);
//    storage.flushRow(table, "sys.cpu.user".getBytes());
//    try {
//      uid.deleteAsync("sys.cpu.user").join();
//      fail("Expected NoSuchUniqueName");
//    } catch (NoSuchUniqueName e) { }
//    assertEquals("sys.cpu.user", uid.getName(UID));
//  }
//  
//  @Test
//  public void useLru() throws Exception {
//    config.overrideConfig("tsd.uid.lru.enable", "true");
//    uid = new UniqueId(tsdb, table, METRIC, 3, false);
//    final byte[] id = { 0, 'a', 0x42 };
//    final byte[] byte_name = { 'f', 'o', 'o' };
//
//    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
//    kvs.add(new KeyValue(id, ID, METRIC_ARRAY, byte_name));
//    when(client.get(anyGet()))
//      .thenReturn(Deferred.fromResult(kvs));
//
//    assertEquals("foo", uid.getName(id));
//    // Should be a cache hit ...
//    assertEquals("foo", uid.getName(id));
//
//    assertEquals(1, uid.cacheHits());
//    assertEquals(1, uid.cacheMisses());
//    assertEquals(2, uid.cacheSize());
//
//    // ... so verify there was only one HBase Get.
//    verify(client).get(anyGet());
//    assertNotNull(uid.lruNameCache());
//    assertNotNull(uid.lruIdCache());
//  }
//  
//  @Test
//  public void useLruLimit() throws Exception {
//    config.overrideConfig("tsd.uid.lru.name.size", "2");
//    config.overrideConfig("tsd.uid.lru.id.size", "2");
//    config.overrideConfig("tsd.uid.lru.enable", "true");
//    uid = new UniqueId(tsdb, table, METRIC, 3, false);
//    byte[] id = { 0, 'a', 0x42 };
//    byte[] byte_name = { 'f', 'o', 'o' };
//
//    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
//    kvs.add(new KeyValue(id, ID, METRIC_ARRAY, byte_name));
//    when(client.get(anyGet()))
//      .thenReturn(Deferred.fromResult(kvs));
//
//    assertEquals("foo", uid.getName(id));
//    // Should be a cache hit ...
//    assertEquals("foo", uid.getName(id));
//
//    assertEquals(1, uid.cacheHits());
//    assertEquals(1, uid.cacheMisses());
//    assertEquals(2, uid.cacheSize());
//
//    // ... so verify there was only one HBase Get.
//    verify(client).get(anyGet());
//    
//    id = new byte[] { 0, 0, 1 };
//    byte_name = new byte[] { 'b', 'a', 'r' };
//    
//    kvs = new ArrayList<KeyValue>(1);
//    kvs.add(new KeyValue(id, ID, METRIC_ARRAY, byte_name));
//    when(client.get(anyGet()))
//      .thenReturn(Deferred.fromResult(kvs));
//    
//    assertEquals("bar", uid.getName(id));
//    assertEquals(1, uid.cacheHits());
//    assertEquals(2, uid.cacheMisses());
//    assertEquals(4, uid.cacheSize());
//    verify(client, times(2)).get(anyGet());
//    
//    // now one should be bumped out
//    id = new byte[] { 0, 0, 2 };
//    byte_name = new byte[] { 'd', 'o', 'g' };
//    
//    kvs = new ArrayList<KeyValue>(1);
//    kvs.add(new KeyValue(id, ID, METRIC_ARRAY, byte_name));
//    when(client.get(anyGet()))
//      .thenReturn(Deferred.fromResult(kvs));
//    
//    assertEquals("dog", uid.getName(id));
//    assertEquals(1, uid.cacheHits());
//    assertEquals(3, uid.cacheMisses());
//    assertEquals(4, uid.cacheSize());
//    verify(client, times(3)).get(anyGet());
//  }
//
//  @Test
//  public void useModeRWGetName() throws Exception {
//    when(tsdb.getMode()).thenReturn(OperationMode.READWRITE);
//    config.overrideConfig("tsd.uid.use_mode", "true");
//    uid = new UniqueId(tsdb, table, METRIC, 3, false);
//    final byte[] id = { 0, 'a', 0x42 };
//    final byte[] byte_name = { 'f', 'o', 'o' };
//
//    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
//    kvs.add(new KeyValue(id, ID, METRIC_ARRAY, byte_name));
//    when(client.get(anyGet()))
//      .thenReturn(Deferred.fromResult(kvs));
//
//    assertEquals("foo", uid.getName(id));
//    // Should be a cache hit ...
//    assertEquals("foo", uid.getName(id));
//
//    assertEquals(1, uid.cacheHits());
//    assertEquals(1, uid.cacheMisses());
//    assertEquals(2, uid.cacheSize());
//
//    // ... so verify there was only one HBase Get.
//    verify(client).get(anyGet());
//  }
//  
//  @Test
//  public void useModeROGetName() throws Exception {
//    when(tsdb.getMode()).thenReturn(OperationMode.READONLY);
//    config.overrideConfig("tsd.uid.use_mode", "true");
//    uid = new UniqueId(tsdb, table, METRIC, 3, false);
//    final byte[] id = { 0, 'a', 0x42 };
//    final byte[] byte_name = { 'f', 'o', 'o' };
//
//    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
//    kvs.add(new KeyValue(id, ID, METRIC_ARRAY, byte_name));
//    when(client.get(anyGet()))
//      .thenReturn(Deferred.fromResult(kvs));
//
//    assertEquals("foo", uid.getName(id));
//    // Should be a cache hit ...
//    assertEquals("foo", uid.getName(id));
//
//    assertEquals(1, uid.cacheHits());
//    assertEquals(1, uid.cacheMisses());
//    assertEquals(1, uid.cacheSize());
//
//    // ... so verify there was only one HBase Get.
//    verify(client).get(anyGet());
//    assertTrue(uid.nameCache().isEmpty());
//    assertEquals(1, uid.idCache().size());
//  }
//  
//  @Test
//  public void useModeWOGetName() throws Exception {
//    when(tsdb.getMode()).thenReturn(OperationMode.WRITEONLY);
//    config.overrideConfig("tsd.uid.use_mode", "true");
//    uid = new UniqueId(tsdb, table, METRIC, 3, false);
//    final byte[] id = { 0, 'a', 0x42 };
//    final byte[] byte_name = { 'f', 'o', 'o' };
//
//    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
//    kvs.add(new KeyValue(id, ID, METRIC_ARRAY, byte_name));
//    when(client.get(anyGet()))
//      .thenReturn(Deferred.fromResult(kvs))
//      .thenReturn(Deferred.fromResult(kvs));
//
//    assertEquals("foo", uid.getName(id));
//    // NOT a cache hit since we didn't cache the first result.
//    assertEquals("foo", uid.getName(id));
//
//    assertEquals(0, uid.cacheHits());
//    assertEquals(2, uid.cacheMisses());
//    assertEquals(0, uid.cacheSize());
//
//    // 2 hbase hits this time
//    verify(client, times(2)).get(anyGet());
//    assertTrue(uid.nameCache().isEmpty());
//    assertTrue(uid.idCache().isEmpty());
//  }
//  
//  @Test
//  public void useModeRWGetId() throws Exception {
//    when(tsdb.getMode()).thenReturn(OperationMode.READWRITE);
//    config.overrideConfig("tsd.uid.use_mode", "true");
//    uid = new UniqueId(tsdb, table, METRIC, 3, false);
//    final byte[] id = { 0, 'a', 0x42 };
//    final byte[] byte_name = { 'f', 'o', 'o' };
//
//    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
//    kvs.add(new KeyValue(byte_name, ID, METRIC_ARRAY, id));
//    when(client.get(anyGet()))
//      .thenReturn(Deferred.fromResult(kvs));
//
//    assertArrayEquals(id, uid.getId("foo"));
//    // Should be a cache hit ...
//    assertArrayEquals(id, uid.getId("foo"));
//
//    assertEquals(1, uid.cacheHits());
//    assertEquals(1, uid.cacheMisses());
//    assertEquals(2, uid.cacheSize());
//
//    // ... so verify there was only one HBase Get.
//    verify(client).get(anyGet());
//  }
//  
//  @Test
//  public void useModeROGetId() throws Exception {
//    when(tsdb.getMode()).thenReturn(OperationMode.READONLY);
//    config.overrideConfig("tsd.uid.use_mode", "true");
//    uid = new UniqueId(tsdb, table, METRIC, 3, false);
//    final byte[] id = { 0, 'a', 0x42 };
//    final byte[] byte_name = { 'f', 'o', 'o' };
//
//    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
//    kvs.add(new KeyValue(byte_name, ID, METRIC_ARRAY, id));
//    when(client.get(anyGet()))
//      .thenReturn(Deferred.fromResult(kvs))
//      .thenReturn(Deferred.fromResult(kvs));
//
//    assertArrayEquals(id, uid.getId("foo"));
//    // NOT a cache hit since we didn't cache the first time.
//    assertArrayEquals(id, uid.getId("foo"));
//
//    assertEquals(0, uid.cacheHits());
//    assertEquals(2, uid.cacheMisses());
//    assertEquals(0, uid.cacheSize());
//
//    // two HBase hits
//    verify(client, times(2)).get(anyGet());
//    assertTrue(uid.nameCache().isEmpty());
//    assertTrue(uid.idCache().isEmpty());
//  }
//  
//  @Test
//  public void useModeWOGetId() throws Exception {
//    when(tsdb.getMode()).thenReturn(OperationMode.WRITEONLY);
//    config.overrideConfig("tsd.uid.use_mode", "true");
//    uid = new UniqueId(tsdb, table, METRIC, 3, false);
//    final byte[] id = { 0, 'a', 0x42 };
//    final byte[] byte_name = { 'f', 'o', 'o' };
//
//    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
//    kvs.add(new KeyValue(byte_name, ID, METRIC_ARRAY, id));
//    when(client.get(anyGet()))
//      .thenReturn(Deferred.fromResult(kvs))
//      .thenReturn(Deferred.fromResult(kvs));
//
//    assertArrayEquals(id, uid.getId("foo"));
//    // Should be a cache hit ...
//    assertArrayEquals(id, uid.getId("foo"));
//
//    assertEquals(1, uid.cacheHits());
//    assertEquals(1, uid.cacheMisses());
//    assertEquals(1, uid.cacheSize());
//
//    // ... so verify there was only one HBase Get.
//    verify(client, times(1)).get(anyGet());
//    assertEquals(1, uid.nameCache().size());
//    assertTrue(uid.idCache().isEmpty());
//  }
//  
  // ----------------- //
  // Helper functions. //
  // ----------------- //

  private static byte[] emptyArray() {
    return eq(HBaseClient.EMPTY_ARRAY);
  }

  private static GetRequest anyGet() {
    return any(GetRequest.class);
  }

  private static AtomicIncrementRequest incrementForRow(final byte[] row) {
    return argThat(new ArgumentMatcher<AtomicIncrementRequest>() {
      public boolean matches(Object incr) {
        return Arrays.equals(((AtomicIncrementRequest) incr).key(), row);
      }
      public void describeTo(org.hamcrest.Description description) {
        description.appendText("AtomicIncrementRequest for row "
                               + Arrays.toString(row));
      }
    });
  }

  private static PutRequest anyPut() {
    return any(PutRequest.class);
  }
  
  private static DeleteRequest anyDelete() {
    return any(DeleteRequest.class);
  }

  private static Answer<Deferred<Boolean>> answerTrue() {
    return new Answer<Deferred<Boolean>>() {
      public Deferred<Boolean> answer(final InvocationOnMock inv) {
        return Deferred.fromResult(true);
      }
    };
  }

  @SuppressWarnings("unchecked")
  private static Callback<byte[], ArrayList<KeyValue>> anyByteCB() {
    return any(Callback.class);
  }

  private static PutRequest putForRow(final byte[] row) {
    return argThat(new ArgumentMatcher<PutRequest>() {
      public boolean matches(Object put) {
        return Arrays.equals(((PutRequest) put).key(), row);
      }
      public void describeTo(org.hamcrest.Description description) {
        description.appendText("PutRequest for row " + Arrays.toString(row));
      }
    });
  }

  private static HBaseException fakeHBaseException() {
    final HBaseException hbe = mock(HBaseException.class);
    when(hbe.getStackTrace())
      // Truncate the stack trace because otherwise it's gigantic.
      .thenReturn(Arrays.copyOf(new RuntimeException().getStackTrace(), 3));
    when(hbe.getMessage())
      .thenReturn("fake exception");
    return hbe;
  }

  private static final byte[] MAXID = { 0 };

  private static void resetConfig() {
    final UnitTestConfiguration c = tsdb.config;
    if (c.hasProperty(data_store.getConfigKey(Tsdb1xUniqueIdStore.CHARACTER_SET_KEY))) {
      c.override(data_store.getConfigKey(Tsdb1xUniqueIdStore.CHARACTER_SET_KEY), 
          Tsdb1xUniqueIdStore.CHARACTER_SET_DEFAULT);
    }
  }
  
  static void verifySpan(final String name) {
    assertEquals(1, trace.spans.size());
    assertEquals(name, trace.spans.get(0).id);
    assertEquals("OK", trace.spans.get(0).tags.get("status"));
  }
  
  static void verifySpan(final String name, final Class<?> ex) {
    verifySpan(name, ex, 1);
  }
  
  static void verifySpan(final String name, final Class<?> ex, final int size) {
    assertEquals(size, trace.spans.size());
    assertEquals(name, trace.spans.get(size - 1).id);
    assertEquals("Error", trace.spans.get(0).tags.get("status"));
    assertTrue(ex.isInstance(trace.spans.get(0).exceptions.get("Exception")));
  }
  
  static Tsdb1xHBaseDataStore badClient() {
    final HBaseClient local_client = mock(HBaseClient.class);
    final Tsdb1xHBaseDataStore data_store = mock(Tsdb1xHBaseDataStore.class);
    when(data_store.client()).thenReturn(local_client);
    when(data_store.tsdb()).thenReturn(tsdb);
    when(data_store.uidTable()).thenReturn(UID_TABLE);
    when(data_store.getConfigKey(anyString())).thenReturn("foo");
    return data_store;
  }
}
