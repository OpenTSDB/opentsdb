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
package net.opentsdb.uid;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

import net.opentsdb.configuration.UnitTestConfiguration;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.stats.MockTrace;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.StorageException;

public class TestLRUUniqueId {
  private static final String DEFAULT_ID = "default";
  private static final byte[] UID1 = new byte[] { 0, 0, 1 };
  private static final byte[] UID2 = new byte[] { 0, 0, 2 };
  private static final byte[] UID3 = new byte[] { 0, 0, 3 };
  private static final byte[] UID4 = new byte[] { 0, 0, 4 };
  
  private static final String STRING1 = "sys.cpu.user";
  private static final String STRING2 = "host";
  private static final String STRING3 = "web01";
  private static final String STRING4 = "web02";
  
  private static MockTSDB tsdb;
  private MockTrace trace;
  private UniqueIdStore store;

  @BeforeClass
  public static void beforeClass() throws Exception {
    tsdb = new MockTSDB();
  }
  
  @Before
  public void before() throws Exception {
    resetConfig();
    store = mock(UniqueIdStore.class);
    
  }
  
  @Test
  public void ctor() throws Exception {
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    assertEquals(UniqueIdType.METRIC, lru.type());
  }
  
  @Test
  public void getName() throws Exception {
    when(store.getName(any(UniqueIdType.class), any(byte[].class), 
        any(Span.class)))
      .thenReturn(Deferred.fromResult(STRING1));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    assertEquals(STRING1, lru.getName(UID1, null).join());
    assertEquals(STRING1, lru.getName(UID1, null).join());
    assertEquals(STRING1, lru.getName(UID1, null).join());
    assertEquals(1, lru.nameCache().size());
    assertEquals(1, lru.idCache().size());
    verify(store, times(1)).getName(UniqueIdType.METRIC, UID1, null);
    
    trace = new MockTrace();
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    assertEquals(STRING1, lru.getName(UID1, trace.newSpan("UT").start()).join());
    assertEquals(STRING1, lru.getName(UID1, trace.newSpan("UT").start()).join());
    assertEquals(STRING1, lru.getName(UID1, trace.newSpan("UT").start()).join());
    assertEquals(0, trace.spans.size());
    
    trace = new MockTrace(true);
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    assertEquals(STRING1, lru.getName(UID1, trace.newSpan("UT").start()).join());
    assertEquals(STRING1, lru.getName(UID1, trace.newSpan("UT").start()).join());
    assertEquals(STRING1, lru.getName(UID1, trace.newSpan("UT").start()).join());
    assertEquals(3, trace.spans.size());
    assertEquals(LRUUniqueId.class.getName() + ".getName", trace.spans.get(0).id);
    assertEquals("OK", trace.spans.get(0).tags.get("status"));
    assertEquals("false", trace.spans.get(0).tags.get("fromCache"));
    assertEquals("true", trace.spans.get(1).tags.get("fromCache"));
    assertEquals("true", trace.spans.get(2).tags.get("fromCache"));
  }
  
  @Test
  public void getNameNull() throws Exception {
    when(store.getName(any(UniqueIdType.class), any(byte[].class), 
        any(Span.class)))
      .thenReturn(Deferred.fromResult(null));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    assertNull(lru.getName(UID1, null).join());
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    verify(store, times(1)).getName(UniqueIdType.METRIC, UID1, null);
  }
  
  @Test
  public void getNameIllegalArgumentException() throws Exception {
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    try {
      lru.getName(null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      lru.getName(new byte[0], null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void getNameModes() throws Exception {
    // read-write
    when(store.getName(any(UniqueIdType.class), any(byte[].class), 
        any(Span.class)))
      .thenReturn(Deferred.fromResult(STRING1));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    assertEquals(STRING1, lru.getName(UID1, null).join());
    assertEquals(STRING1, lru.getName(UID1, null).join());
    assertEquals(STRING1, lru.getName(UID1, null).join());
    verify(store, times(1)).getName(UniqueIdType.METRIC, UID1, null);
    assertEquals(1, lru.nameCache().size());
    assertEquals(1, lru.idCache().size());
    
    // write only
    tsdb.config.override("tsd.uid." + DEFAULT_ID + ".metric.mode", "w");
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    assertEquals(STRING1, lru.getName(UID1, null).join());
    assertEquals(STRING1, lru.getName(UID1, null).join());
    assertEquals(STRING1, lru.getName(UID1, null).join());
    verify(store, times(4)).getName(UniqueIdType.METRIC, UID1, null);
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    
    // read only
    tsdb.config.override("tsd.uid." + DEFAULT_ID + ".metric.mode", "r");
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    assertEquals(STRING1, lru.getName(UID1, null).join());
    assertEquals(STRING1, lru.getName(UID1, null).join());
    assertEquals(STRING1, lru.getName(UID1, null).join());
    verify(store, times(5)).getName(UniqueIdType.METRIC, UID1, null);
    assertEquals(0, lru.nameCache().size());
    assertEquals(1, lru.idCache().size());
  }
  
  @Test
  public void getNameExceptionReturned() throws Exception {
    when(store.getName(any(UniqueIdType.class), any(byte[].class), 
        any(Span.class)))
      .thenReturn(Deferred.fromError(new StorageException("Boo!")));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    Deferred<String> deferred = lru.getName(UID1, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    verify(store, times(1)).getName(UniqueIdType.METRIC, UID1, null);
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    
    trace = new MockTrace(true);
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    deferred = lru.getName(UID1, trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    verify(store, times(2)).getName(eq(UniqueIdType.METRIC), eq(UID1), 
        any(Span.class));
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    assertEquals(1, trace.spans.size());
    assertEquals("Error", trace.spans.get(0).tags.get("status"));
  }
  
  @Test
  public void getNameExceptionThrown() throws Exception {
    when(store.getName(any(UniqueIdType.class), any(byte[].class), 
        any(Span.class)))
      .thenThrow(new StorageException("Boo!"));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    Deferred<String> deferred = lru.getName(UID1, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    verify(store, times(1)).getName(UniqueIdType.METRIC, UID1, null);
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    
    trace = new MockTrace(true);
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    deferred = lru.getName(UID1, trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    verify(store, times(2)).getName(eq(UniqueIdType.METRIC), eq(UID1), 
        any(Span.class));
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    assertEquals(1, trace.spans.size());
    assertEquals("Error", trace.spans.get(0).tags.get("status"));
  }
  
  @Test
  public void getNames() throws Exception {
    when(store.getNames(any(UniqueIdType.class), any(List.class), 
        any(Span.class)))
      .thenReturn(Deferred.fromResult(Lists.newArrayList(STRING1, STRING2)));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    
    List<String> names = lru.getNames(Lists.newArrayList(UID1, UID2), null).join();
    assertEquals(STRING1, names.get(0));
    assertEquals(STRING2, names.get(1));
    verify(store, times(1)).getNames(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(2, lru.nameCache().size());
    assertEquals(2, lru.idCache().size());
    
    names = lru.getNames(Lists.newArrayList(UID1, UID2), null).join();
    assertEquals(STRING1, names.get(0));
    assertEquals(STRING2, names.get(1));
    verify(store, times(1)).getNames(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(2, lru.nameCache().size());
    assertEquals(2, lru.idCache().size());
    
    trace = new MockTrace();
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    names = lru.getNames(Lists.newArrayList(UID1, UID2), 
        trace.newSpan("UT").start()).join();
    assertEquals(0, trace.spans.size());
    
    trace = new MockTrace(true);
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    names = lru.getNames(Lists.newArrayList(UID1, UID2), 
        trace.newSpan("UT").start()).join();
    names = lru.getNames(Lists.newArrayList(UID1, UID2), 
        trace.newSpan("UT").start()).join();
    assertEquals(2, trace.spans.size());
    assertEquals(LRUUniqueId.class.getName() + ".getNames", trace.spans.get(0).id);
    assertEquals("OK", trace.spans.get(0).tags.get("status"));
    assertEquals("false", trace.spans.get(0).tags.get("fromCache"));
    assertEquals("true", trace.spans.get(1).tags.get("fromCache"));
  }
  
  @Test
  public void getNamesSameIDs() throws Exception {
    when(store.getNames(any(UniqueIdType.class), any(List.class), 
        any(Span.class)))
      .thenReturn(Deferred.fromResult(Lists.newArrayList(STRING1, STRING1, STRING1)));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    
    List<String> names = lru.getNames(Lists.newArrayList(UID1, UID1, UID1), null).join();
    assertEquals(STRING1, names.get(0));
    assertEquals(STRING1, names.get(1));
    assertEquals(STRING1, names.get(2));
    verify(store, times(1)).getNames(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(1, lru.nameCache().size());
    assertEquals(1, lru.idCache().size());
  }
  
  @Test
  public void getNamesIllegalArgumentException() throws Exception {
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    try {
      lru.getNames(null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      lru.getNames(Lists.newArrayList(), null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void getNamesPartialCacheHit() throws Exception {
    when(store.getNames(any(UniqueIdType.class), any(List.class), 
        any(Span.class)))
      .thenReturn(Deferred.fromResult(Lists.newArrayList(STRING2)));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    lru.idCache().put(UniqueId.uidToString(UID1), STRING1);
    
    List<String> names = lru.getNames(Lists.newArrayList(UID1, UID2), null).join();
    assertEquals(2, names.size());
    assertEquals(STRING1, names.get(0));
    assertEquals(STRING2, names.get(1));
    verify(store, times(1)).getNames(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(1, lru.nameCache().size());
    assertEquals(2, lru.idCache().size());
    
    // fully satisfied from cache
    names = lru.getNames(Lists.newArrayList(UID1, UID2), null).join();
    assertEquals(2, names.size());
    assertEquals(STRING1, names.get(0));
    assertEquals(STRING2, names.get(1));
    verify(store, times(1)).getNames(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(1, lru.nameCache().size());
    assertEquals(2, lru.idCache().size());
    
    // staggered
    when(store.getNames(any(UniqueIdType.class), any(List.class), 
        any(Span.class)))
      .thenReturn(Deferred.fromResult(Lists.newArrayList(STRING1, STRING3)));
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    lru.idCache().put(UniqueId.uidToString(UID2), STRING2);
    lru.idCache().put(UniqueId.uidToString(UID4), STRING4);
    
    names = lru.getNames(Lists.newArrayList(UID1, UID2, UID3, UID4), null).join();
    assertEquals(4, names.size());
    assertEquals(STRING1, names.get(0));
    assertEquals(STRING2, names.get(1));
    assertEquals(STRING3, names.get(2));
    assertEquals(STRING4, names.get(3));
    
    // diff order
    when(store.getNames(any(UniqueIdType.class), any(List.class), 
        any(Span.class)))
      .thenReturn(Deferred.fromResult(Lists.newArrayList(STRING1, STRING3)));
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    lru.idCache().put(UniqueId.uidToString(UID2), STRING2);
    lru.idCache().put(UniqueId.uidToString(UID4), STRING4);
    
    names = lru.getNames(Lists.newArrayList(UID2, UID4, UID1, UID3), null).join();
    assertEquals(4, names.size());
    assertEquals(STRING2, names.get(0));
    assertEquals(STRING4, names.get(1));
    assertEquals(STRING1, names.get(2));
    assertEquals(STRING3, names.get(3));
  }
  
  @Test
  public void getNamesNulls() throws Exception {
    when(store.getNames(any(UniqueIdType.class), any(List.class), 
        any(Span.class)))
      .thenReturn(Deferred.fromResult(Lists.newArrayList(STRING1, null)));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    List<String> names = lru.getNames(Lists.newArrayList(UID1, UID2), null).join();
    assertEquals(STRING1, names.get(0));
    assertNull(names.get(1));
    verify(store, times(1)).getNames(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(1, lru.nameCache().size());
    assertEquals(1, lru.idCache().size());
  }
  
  @Test
  public void getNamesModes() throws Exception {
    when(store.getNames(any(UniqueIdType.class), any(List.class), 
        any(Span.class)))
      .thenReturn(Deferred.fromResult(Lists.newArrayList(STRING1, STRING2)));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    
    // read-write
    List<String> names = lru.getNames(Lists.newArrayList(UID1, UID2), null).join();
    assertEquals(STRING1, names.get(0));
    assertEquals(STRING2, names.get(1));
    verify(store, times(1)).getNames(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(2, lru.nameCache().size());
    assertEquals(2, lru.idCache().size());
    
    // write only
    tsdb.config.override("tsd.uid." + DEFAULT_ID + ".metric.mode", "w");
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    names = lru.getNames(Lists.newArrayList(UID1, UID2), null).join();
    assertEquals(STRING1, names.get(0));
    assertEquals(STRING2, names.get(1));
    verify(store, times(2)).getNames(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    
    // read only
    tsdb.config.override("tsd.uid." + DEFAULT_ID + ".metric.mode", "r");
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    names = lru.getNames(Lists.newArrayList(UID1, UID2), null).join();
    assertEquals(STRING1, names.get(0));
    assertEquals(STRING2, names.get(1));
    verify(store, times(3)).getNames(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(0, lru.nameCache().size());
    assertEquals(2, lru.idCache().size());
  }
  
  @Test
  public void getNamesExceptionReturned() throws Exception {
    when(store.getNames(any(UniqueIdType.class), any(List.class), 
        any(Span.class)))
      .thenReturn(Deferred.fromError(new StorageException("Boo!")));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    
    Deferred<List<String>> deferred = 
        lru.getNames(Lists.newArrayList(UID1, UID2), null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    verify(store, times(1)).getNames(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    
    trace = new MockTrace(true);
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    deferred = lru.getNames(Lists.newArrayList(UID1, UID2), 
        trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    verify(store, times(2)).getNames(eq(UniqueIdType.METRIC), 
        any(List.class), any(Span.class));
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    assertEquals(1, trace.spans.size());
    assertEquals("Error", trace.spans.get(0).tags.get("status"));
  }
  
  @Test
  public void getNamesExceptionThrown() throws Exception {
    when(store.getNames(any(UniqueIdType.class), any(List.class), 
        any(Span.class)))
      .thenThrow(new StorageException("Boo!"));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    
    // read-write
    Deferred<List<String>> deferred = 
        lru.getNames(Lists.newArrayList(UID1, UID2), null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    verify(store, times(1)).getNames(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    
    trace = new MockTrace(true);
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    
    deferred = lru.getNames(Lists.newArrayList(UID1, UID2), 
        trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    verify(store, times(2)).getNames(eq(UniqueIdType.METRIC), 
        any(List.class), any(Span.class));
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    assertEquals(1, trace.spans.size());
    assertEquals("Error", trace.spans.get(0).tags.get("status"));
  }
  
  @Test
  public void getId() throws Exception {
    when(store.getId(any(UniqueIdType.class), anyString(), 
        any(Span.class)))
      .thenReturn(Deferred.fromResult(UID1));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    assertArrayEquals(UID1, lru.getId(STRING1, null).join());
    assertArrayEquals(UID1, lru.getId(STRING1, null).join());
    assertArrayEquals(UID1, lru.getId(STRING1, null).join());
    assertEquals(1, lru.nameCache().size());
    assertEquals(1, lru.idCache().size());
    verify(store, times(1)).getId(UniqueIdType.METRIC, STRING1, null);
    
    trace = new MockTrace();
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    assertArrayEquals(UID1, lru.getId(STRING1, trace.newSpan("UT").start()).join());
    assertArrayEquals(UID1, lru.getId(STRING1, trace.newSpan("UT").start()).join());
    assertArrayEquals(UID1, lru.getId(STRING1, trace.newSpan("UT").start()).join());
    assertEquals(0, trace.spans.size());
    
    trace = new MockTrace(true);
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    assertArrayEquals(UID1, lru.getId(STRING1, trace.newSpan("UT").start()).join());
    assertArrayEquals(UID1, lru.getId(STRING1, trace.newSpan("UT").start()).join());
    assertArrayEquals(UID1, lru.getId(STRING1, trace.newSpan("UT").start()).join());
    assertEquals(3, trace.spans.size());
    assertEquals(LRUUniqueId.class.getName() + ".getId", trace.spans.get(0).id);
    assertEquals("OK", trace.spans.get(0).tags.get("status"));
    assertEquals("false", trace.spans.get(0).tags.get("fromCache"));
    assertEquals("true", trace.spans.get(1).tags.get("fromCache"));
    assertEquals("true", trace.spans.get(2).tags.get("fromCache"));
  }
  
  @Test
  public void getIdNull() throws Exception {
    when(store.getId(any(UniqueIdType.class), anyString(), 
        any(Span.class)))
      .thenReturn(Deferred.fromResult(null));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    assertNull(lru.getId(STRING1, null).join());
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    verify(store, times(1)).getId(UniqueIdType.METRIC, STRING1, null);
  }
  
  @Test
  public void getIdIllegalArgumentException() throws Exception {
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    try {
      lru.getId(null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      lru.getId("", null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void getIdModes() throws Exception {
    // read-write
    when(store.getId(any(UniqueIdType.class), anyString(), 
        any(Span.class)))
      .thenReturn(Deferred.fromResult(UID1));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    assertArrayEquals(UID1, lru.getId(STRING1, null).join());
    assertArrayEquals(UID1, lru.getId(STRING1, null).join());
    assertArrayEquals(UID1, lru.getId(STRING1, null).join());
    assertEquals(1, lru.nameCache().size());
    assertEquals(1, lru.idCache().size());
    verify(store, times(1)).getId(UniqueIdType.METRIC, STRING1, null);
    
    // write only
    tsdb.config.override("tsd.uid." + DEFAULT_ID + ".metric.mode", "w");
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    assertArrayEquals(UID1, lru.getId(STRING1, null).join());
    assertArrayEquals(UID1, lru.getId(STRING1, null).join());
    assertArrayEquals(UID1, lru.getId(STRING1, null).join());
    assertEquals(1, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    verify(store, times(2)).getId(UniqueIdType.METRIC, STRING1, null);
    
    // read only
    tsdb.config.override("tsd.uid." + DEFAULT_ID + ".metric.mode", "r");
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    assertArrayEquals(UID1, lru.getId(STRING1, null).join());
    assertArrayEquals(UID1, lru.getId(STRING1, null).join());
    assertArrayEquals(UID1, lru.getId(STRING1, null).join());
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    verify(store, times(5)).getId(UniqueIdType.METRIC, STRING1, null);
  }

  @Test
  public void getIdExceptionReturned() throws Exception {
    when(store.getId(any(UniqueIdType.class), anyString(), 
        any(Span.class)))
      .thenReturn(Deferred.fromError(new StorageException("Boo!")));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    
    Deferred<byte[]> deferred = lru.getId(STRING1, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    verify(store, times(1)).getId(UniqueIdType.METRIC, STRING1, null);
    
    trace = new MockTrace(true);
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    deferred = lru.getId(STRING1, trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    verify(store, times(1)).getId(UniqueIdType.METRIC, STRING1, null);
    assertEquals(1, trace.spans.size());
    assertEquals("Error", trace.spans.get(0).tags.get("status"));
  }
  
  @Test
  public void getIdExceptionThrown() throws Exception {
    when(store.getId(any(UniqueIdType.class), anyString(), 
        any(Span.class)))
      .thenThrow(new StorageException("Boo!"));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    
    Deferred<byte[]> deferred = lru.getId(STRING1, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    verify(store, times(1)).getId(UniqueIdType.METRIC, STRING1, null);
    
    trace = new MockTrace(true);
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    deferred = lru.getId(STRING1, trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    verify(store, times(1)).getId(UniqueIdType.METRIC, STRING1, null);
    assertEquals(1, trace.spans.size());
    assertEquals("Error", trace.spans.get(0).tags.get("status"));
  }
  
  @Test
  public void getIds() throws Exception {
    when(store.getIds(any(UniqueIdType.class), any(List.class), 
        any(Span.class)))
      .thenReturn(Deferred.fromResult(Lists.newArrayList(UID1, UID2)));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    
    List<byte[]> ids = lru.getIds(Lists.newArrayList(STRING1, STRING2), null).join();
    assertArrayEquals(UID1, ids.get(0));
    assertArrayEquals(UID2, ids.get(1));
    verify(store, times(1)).getIds(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(2, lru.nameCache().size());
    assertEquals(2, lru.idCache().size());
    
    trace = new MockTrace();
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    ids = lru.getIds(Lists.newArrayList(STRING1, STRING2), 
        trace.newSpan("UT").start()).join();
    assertEquals(0, trace.spans.size());
    
    trace = new MockTrace(true);
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    ids = lru.getIds(Lists.newArrayList(STRING1, STRING2), 
        trace.newSpan("UT").start()).join();
    ids = lru.getIds(Lists.newArrayList(STRING1, STRING2), 
        trace.newSpan("UT").start()).join();
    assertEquals(2, trace.spans.size());
    assertEquals(LRUUniqueId.class.getName() + ".getIds", trace.spans.get(0).id);
    assertEquals("OK", trace.spans.get(0).tags.get("status"));
    assertEquals("false", trace.spans.get(0).tags.get("fromCache"));
    assertEquals("true", trace.spans.get(1).tags.get("fromCache"));
  }
  
  @Test
  public void getIdsSameNames() throws Exception {
    when(store.getIds(any(UniqueIdType.class), any(List.class), 
        any(Span.class)))
      .thenReturn(Deferred.fromResult(Lists.newArrayList(UID1, UID1, UID1)));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    
    List<byte[]> ids = lru.getIds(Lists.newArrayList(STRING1, STRING1, STRING1), null).join();
    assertArrayEquals(UID1, ids.get(0));
    assertArrayEquals(UID1, ids.get(1));
    assertArrayEquals(UID1, ids.get(2));
    verify(store, times(1)).getIds(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(1, lru.nameCache().size());
    assertEquals(1, lru.idCache().size());
  }
  
  @Test
  public void getIdsPartialCacheIt() throws Exception {
    when(store.getIds(any(UniqueIdType.class), any(List.class), 
        any(Span.class)))
      .thenReturn(Deferred.fromResult(Lists.newArrayList(UID2)));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    lru.nameCache().put(STRING1, UID1);
    
    List<byte[]> ids = lru.getIds(Lists.newArrayList(STRING1, STRING2), null).join();
    assertArrayEquals(UID1, ids.get(0));
    assertArrayEquals(UID2, ids.get(1));
    verify(store, times(1)).getIds(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(2, lru.nameCache().size());
    assertEquals(1, lru.idCache().size());
    
    // fully satisfied from cache
    ids = lru.getIds(Lists.newArrayList(STRING1, STRING2), null).join();
    assertArrayEquals(UID1, ids.get(0));
    assertArrayEquals(UID2, ids.get(1));
    verify(store, times(1)).getIds(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(2, lru.nameCache().size());
    assertEquals(1, lru.idCache().size());
    
    // staggered
    when(store.getIds(any(UniqueIdType.class), any(List.class), 
        any(Span.class)))
      .thenReturn(Deferred.fromResult(Lists.newArrayList(UID1, UID3)));
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    lru.nameCache().put(STRING2, UID2);
    lru.nameCache().put(STRING4, UID4);
    
    ids = lru.getIds(Lists.newArrayList(STRING1, STRING2, STRING3, STRING4), null).join();
    assertEquals(4, ids.size());
    assertArrayEquals(UID1, ids.get(0));
    assertArrayEquals(UID2, ids.get(1));
    assertArrayEquals(UID3, ids.get(2));
    assertArrayEquals(UID4, ids.get(3));
    verify(store, times(2)).getIds(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(4, lru.nameCache().size());
    assertEquals(2, lru.idCache().size());
    
    // diff order
    when(store.getIds(any(UniqueIdType.class), any(List.class), 
        any(Span.class)))
      .thenReturn(Deferred.fromResult(Lists.newArrayList(UID3, UID1)));
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    lru.nameCache().put(STRING2, UID2);
    lru.nameCache().put(STRING4, UID4);
    
    ids = lru.getIds(Lists.newArrayList(STRING2, STRING4, STRING3, STRING1), null).join();
    assertEquals(4, ids.size());
    assertArrayEquals(UID2, ids.get(0));
    assertArrayEquals(UID4, ids.get(1));
    assertArrayEquals(UID3, ids.get(2));
    assertArrayEquals(UID1, ids.get(3));
    verify(store, times(3)).getIds(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(4, lru.nameCache().size());
    assertEquals(2, lru.idCache().size());
  }
  
  @Test
  public void getIdsWithNulls() throws Exception {
    when(store.getIds(any(UniqueIdType.class), any(List.class), 
        any(Span.class)))
      .thenReturn(Deferred.fromResult(Lists.newArrayList(UID1, null)));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    
    List<byte[]> ids = lru.getIds(Lists.newArrayList(STRING1, STRING2), null).join();
    assertArrayEquals(UID1, ids.get(0));
    assertNull(ids.get(1));
    verify(store, times(1)).getIds(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(1, lru.nameCache().size());
    assertEquals(1, lru.idCache().size());
  }
  
  @Test
  public void getIdsModes() throws Exception {
    when(store.getIds(any(UniqueIdType.class), any(List.class), 
        any(Span.class)))
      .thenReturn(Deferred.fromResult(Lists.newArrayList(UID1, UID2)));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    
    List<byte[]> ids = lru.getIds(Lists.newArrayList(STRING1, STRING2), null).join();
    assertArrayEquals(UID1, ids.get(0));
    assertArrayEquals(UID2, ids.get(1));
    verify(store, times(1)).getIds(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(2, lru.nameCache().size());
    assertEquals(2, lru.idCache().size());
    
    // write only
    tsdb.config.override("tsd.uid." + DEFAULT_ID + ".metric.mode", "w");
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    ids = lru.getIds(Lists.newArrayList(STRING1, STRING2), null).join();
    assertArrayEquals(UID1, ids.get(0));
    assertArrayEquals(UID2, ids.get(1));
    verify(store, times(2)).getIds(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(2, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    
    // read only
    tsdb.config.override("tsd.uid." + DEFAULT_ID + ".metric.mode", "r");
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    ids = lru.getIds(Lists.newArrayList(STRING1, STRING2), null).join();
    assertArrayEquals(UID1, ids.get(0));
    assertArrayEquals(UID2, ids.get(1));
    verify(store, times(3)).getIds(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
  }

  @Test
  public void getIdsExceptionReturned() throws Exception {
    when(store.getIds(any(UniqueIdType.class), any(List.class), 
        any(Span.class)))
      .thenReturn(Deferred.fromError(new StorageException("Boo!")));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    
    Deferred<List<byte[]>> deferred = lru.getIds(
        Lists.newArrayList(STRING1, STRING2), null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    verify(store, times(1)).getIds(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    
    trace = new MockTrace(true);
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    deferred = lru.getIds(Lists.newArrayList(STRING1, STRING2), 
        trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    verify(store, times(2)).getIds(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    assertEquals(1, trace.spans.size());
    assertEquals("Error", trace.spans.get(0).tags.get("status"));
  }
  
  @Test
  public void getIdsExceptionThrown() throws Exception {
    when(store.getIds(any(UniqueIdType.class), any(List.class), 
        any(Span.class)))
      .thenThrow(new StorageException("Boo!"));
    LRUUniqueId lru = new LRUUniqueId(tsdb, DEFAULT_ID, 
        UniqueIdType.METRIC, store);
    
    Deferred<List<byte[]>> deferred = lru.getIds(
        Lists.newArrayList(STRING1, STRING2), null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    verify(store, times(1)).getIds(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    
    trace = new MockTrace(true);
    lru = new LRUUniqueId(tsdb, DEFAULT_ID, UniqueIdType.METRIC, store);
    deferred = lru.getIds(Lists.newArrayList(STRING1, STRING2), 
        trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    verify(store, times(2)).getIds(eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
    assertEquals(0, lru.nameCache().size());
    assertEquals(0, lru.idCache().size());
    assertEquals(1, trace.spans.size());
    assertEquals("Error", trace.spans.get(0).tags.get("status"));
  }
  
  private static void resetConfig() {
    final UnitTestConfiguration c = tsdb.config;
    if (c.hasProperty("tsd.uid." + DEFAULT_ID + ".metric.mode")) {
      c.override("tsd.uid." + DEFAULT_ID + ".metric.mode", "rw");
    }
    if (c.hasProperty("tsd.uid." + DEFAULT_ID + ".record_stats")) {
      c.override("tsd.uid." + DEFAULT_ID + ".record_stats", "true");
    }
    if (c.hasProperty("tsd.uid." + DEFAULT_ID + ".lru.name.size")) {
      c.override("tsd.uid." + DEFAULT_ID + ".lru.name.size", "10");
    }
    if (c.hasProperty("tsd.uid." + DEFAULT_ID + ".lru.id.size")) {
      c.override("tsd.uid." + DEFAULT_ID + ".lru.id.size", "8");
    }
  }
}
