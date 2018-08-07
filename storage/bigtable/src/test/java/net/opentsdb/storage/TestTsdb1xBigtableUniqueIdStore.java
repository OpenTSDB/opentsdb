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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.Random;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.TimeoutException;

import io.netty.util.Timer;
import net.opentsdb.core.Const;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.MockTSDB.FakeTaskTimer;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesDatumStringId;
import net.opentsdb.data.TimeSeriesDatumId;
import net.opentsdb.data.TimeSeriesDatumStringId;
import net.opentsdb.stats.MockTrace;
import net.opentsdb.stats.Span;
import net.opentsdb.stats.Span.SpanBuilder;
import net.opentsdb.auth.AuthState;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.UnitTestConfiguration;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.TagVFilter;
import net.opentsdb.storage.WriteStatus.WriteState;
import net.opentsdb.storage.schemas.tsdb1x.ResolvedQueryFilter;
import net.opentsdb.uid.IdOrError;
import net.opentsdb.uid.RandomUniqueId;
import net.opentsdb.uid.UniqueIdAssignmentAuthorizer;
import net.opentsdb.uid.UniqueIdFactory;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.UnitTestException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.yaml.snakeyaml.tokens.Token.ID;

@RunWith(PowerMockRunner.class)
// "Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
// because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
                  "ch.qos.*", "org.slf4j.*",
                  "com.sum.*", "org.xml.*"})
@PrepareForTest({ ExecutorService.class, BigtableSession.class, 
  Tsdb1xBigtableQueryNode.class, CredentialOptions.class,
  Tsdb1xBigtableUniqueIdStore.class, RandomUniqueId.class })
public class TestTsdb1xBigtableUniqueIdStore extends UTBase {
  
  private static final String UNI_STRING = "\u00a5123";
  private static final byte[] UNI_BYTES = new byte[] { 0, 0, 6 };
  private static final String ASSIGNED_ID_NAME = "foo";
  private static final byte[] ASSIGNED_ID = { 0, 'a', 0x42 };
  
  private static final String UNASSIGNED_ID_NAME = "new.metric";
  private static final byte[] UNASSIGNED_ID = new byte[] { 0, 0, 0x2B };
  
  private static final String ASSIGNED_TAGV_NAME = "db01";
  private static final byte[] ASSIGNED_TAGV = new byte[] { 0, 0, 8 };
  private static final String UNASSIGNED_TAGV_NAME = "tyrion";
  private static final byte[] UNASSIGNED_TAGV = new byte[] { 0, 0, 0x2B };
  
  private static TimeSeriesDatumStringId ASSIGNED_DATUM_ID;
  private static TimeSeriesDatumStringId UNASSIGNED_DATUM_ID;
  
  private UniqueIdAssignmentAuthorizer filter;
  private FakeTaskTimer timer;
  
  @BeforeClass
  public static void beforeClassLocal() throws Exception {
    // UTF-8 Encoding
    storage.addColumn(MockBigtable.UID_TABLE, 
        UNI_STRING.getBytes(Const.UTF8_CHARSET), 
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY, 
        Tsdb1xBigtableUniqueIdStore.METRICS_QUAL, 
        UNI_BYTES);
    storage.addColumn(MockBigtable.UID_TABLE, 
        UNI_BYTES, 
        Tsdb1xBigtableUniqueIdStore.NAME_FAMILY, 
        Tsdb1xBigtableUniqueIdStore.METRICS_QUAL, 
        UNI_STRING.getBytes(Const.UTF8_CHARSET));
    
    ASSIGNED_DATUM_ID = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric(ASSIGNED_ID_NAME)
        .addTags("host", ASSIGNED_TAGV_NAME)
        .addTags("owner", UNASSIGNED_TAGV_NAME)
        .build();
    
    UNASSIGNED_DATUM_ID = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric(UNASSIGNED_ID_NAME)
        .addTags("host", ASSIGNED_TAGV_NAME)
        .addTags("owner", UNASSIGNED_TAGV_NAME)
        .build();
  }

  @Before
  public void before() throws Exception {
    resetConfig();
  }
  
  @Test
  public void ctorDefaults() throws Exception {
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    assertFalse(uid.assign_and_retry);
    assertFalse(uid.randomize_metric_ids);
    assertFalse(uid.randomize_tagk_ids);
    assertFalse(uid.randomize_tagv_ids);
    
    assertEquals(Tsdb1xBigtableUniqueIdStore.DEFAULT_ATTEMPTS_ASSIGN_ID, 
        uid.max_attempts_assign);
    assertEquals(Tsdb1xBigtableUniqueIdStore.DEFAULT_ATTEMPTS_ASSIGN_RANDOM_ID, 
        uid.max_attempts_assign_random);
    
    assertEquals(Charset.forName("ISO-8859-1"), 
        uid.characterSet(UniqueIdType.METRIC));
    assertEquals(Charset.forName("ISO-8859-1"), 
        uid.characterSet(UniqueIdType.TAGK));
    assertEquals(Charset.forName("ISO-8859-1"), 
        uid.characterSet(UniqueIdType.TAGV));
    
    assertEquals(UniqueIdType.values().length, uid.pending_assignments.size());
    
    try {
      new Tsdb1xBigtableUniqueIdStore(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void ctorOverrides() throws Exception {
    MockTSDB tsdb = new MockTSDB();
    Tsdb1xBigtableDataStore data_store = mock(Tsdb1xBigtableDataStore.class);
    when(data_store.tsdb()).thenReturn(tsdb);
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    
    // now we're assured they're registered, override
    tsdb.config.override(Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), 
        Tsdb1xBigtableUniqueIdStore.CONFIG_PREFIX.get(UniqueIdType.METRIC) + 
        Tsdb1xBigtableUniqueIdStore.CHARACTER_SET_KEY), 
        "UTF8");
    tsdb.config.override(Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), 
        Tsdb1xBigtableUniqueIdStore.CONFIG_PREFIX.get(UniqueIdType.TAGK) + 
        Tsdb1xBigtableUniqueIdStore.CHARACTER_SET_KEY), 
        "UTF8");
    tsdb.config.override(Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), 
        Tsdb1xBigtableUniqueIdStore.CONFIG_PREFIX.get(UniqueIdType.TAGV) + 
        Tsdb1xBigtableUniqueIdStore.CHARACTER_SET_KEY), 
        "UTF8");
    
    tsdb.config.override(Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), 
        Tsdb1xBigtableUniqueIdStore.CONFIG_PREFIX.get(UniqueIdType.METRIC) + 
        Tsdb1xBigtableUniqueIdStore.RANDOM_ASSIGNMENT_KEY), 
        "true");
    tsdb.config.override(Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), 
        Tsdb1xBigtableUniqueIdStore.CONFIG_PREFIX.get(UniqueIdType.TAGK) + 
        Tsdb1xBigtableUniqueIdStore.RANDOM_ASSIGNMENT_KEY), 
        "true");
    tsdb.config.override(Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), 
        Tsdb1xBigtableUniqueIdStore.CONFIG_PREFIX.get(UniqueIdType.TAGV) + 
        Tsdb1xBigtableUniqueIdStore.RANDOM_ASSIGNMENT_KEY), 
        "true");
    
    tsdb.config.override(Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), 
        Tsdb1xBigtableUniqueIdStore.ASSIGN_AND_RETRY_KEY), 
        "true");
    tsdb.config.override(Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), 
        Tsdb1xBigtableUniqueIdStore.ATTEMPTS_KEY), 
        "42");
    tsdb.config.override(Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), 
        Tsdb1xBigtableUniqueIdStore.RANDOM_ATTEMPTS_KEY), 
        "128");
    
    uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    
    assertTrue(uid.assign_and_retry);
    assertTrue(uid.randomize_metric_ids);
    assertTrue(uid.randomize_tagk_ids);
    assertTrue(uid.randomize_tagv_ids);
    
    assertEquals(42, 
        uid.max_attempts_assign);
    assertEquals(128, 
        uid.max_attempts_assign_random);
    
    assertEquals(Charset.forName("UTF8"), 
        uid.characterSet(UniqueIdType.METRIC));
    assertEquals(Charset.forName("UTF8"), 
        uid.characterSet(UniqueIdType.TAGK));
    assertEquals(Charset.forName("UTF8"), 
        uid.characterSet(UniqueIdType.TAGV));
    
    assertNull(uid.authorizer);
    assertEquals(UniqueIdType.values().length, uid.pending_assignments.size());
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
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    assertEquals(METRIC_STRING, uid.getName(UniqueIdType.METRIC, METRIC_BYTES, null).join());
    assertEquals(TAGK_STRING, uid.getName(UniqueIdType.TAGK, TAGK_BYTES, null).join());
    assertEquals(TAGV_STRING, uid.getName(UniqueIdType.TAGV, TAGV_BYTES, null).join());
    
    // this shouldn't decode properly!!
    assertNotEquals(UNI_STRING, uid.getName(UniqueIdType.METRIC, UNI_BYTES, null).join());
    
    // now try it with UTF8
    tsdb.config.override(
        Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), 
            Tsdb1xBigtableUniqueIdStore.CONFIG_PREFIX.get(UniqueIdType.METRIC) + 
            Tsdb1xBigtableUniqueIdStore.CHARACTER_SET_KEY), "UTF-8");
    uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    
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
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    
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
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    // exception
    Deferred<String> deferred = uid.getName(UniqueIdType.METRIC, 
        METRIC_BYTES_EX, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause().getCause() instanceof UnitTestException);
    }
  }
  
  @Test
  public void getNameTracing() throws Exception {
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    // span tests. Only trace on debug.
    trace = new MockTrace();
    assertEquals(METRIC_STRING, uid.getName(UniqueIdType.METRIC, METRIC_BYTES, 
        trace.newSpan("UT").start()).join());
    assertEquals(0, trace.spans.size());
    
    trace = new MockTrace(true);
    assertEquals(METRIC_STRING, uid.getName(UniqueIdType.METRIC, METRIC_BYTES, 
        trace.newSpan("UT").start()).join());
    verifySpan(Tsdb1xBigtableUniqueIdStore.class.getName() + ".getName");
  }
  
  @Test
  public void getNameTraceException() throws Exception {
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    trace = new MockTrace(true);
    try {
      uid.getName(UniqueIdType.METRIC, METRIC_BYTES_EX, trace.newSpan("UT").start())
        .join();
      fail("Expected StorageException");
    } catch (StorageException e) { 
      verifySpan(Tsdb1xBigtableUniqueIdStore.class.getName() + ".getName", 
          ExecutionException.class);
    }
  }
  
  @Test
  public void getNameClientThrowsException() throws Exception {
    // see what happens if we just return an exception
    Tsdb1xBigtableDataStore store = badClient();
    when(store.executor().readRowsAsync(any(ReadRowsRequest.class)))
      .thenThrow(new UnitTestException());
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(store);
    
    Deferred<String> deferred = uid.getName(UniqueIdType.TAGV, TAGV_BYTES, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
    
    store = badClient();
    when(store.executor().readRowsAsync(any(ReadRowsRequest.class)))
      .thenThrow(new UnitTestException());
    uid = new Tsdb1xBigtableUniqueIdStore(store);
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
    verifySpan(Tsdb1xBigtableUniqueIdStore.class.getName() + ".getName", 
        UnitTestException.class);
  }
  
  @Test
  public void getNames() throws Exception {
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
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
        Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), 
            Tsdb1xBigtableUniqueIdStore.CONFIG_PREFIX.get(UniqueIdType.METRIC) + 
            Tsdb1xBigtableUniqueIdStore.CHARACTER_SET_KEY), "UTF-8");
    uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    
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
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    
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
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    // one throws exception
    Deferred<List<String>> deferred = uid.getNames(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_BYTES, TAGV_BYTES_EX), null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause().getCause() instanceof UnitTestException);
    }
  }
  
  @Test
  public void getNamesTracing() throws Exception {
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    // span tests. Only trace on debug.
    trace = new MockTrace();
    uid.getNames(UniqueIdType.METRIC, 
        Lists.newArrayList(METRIC_BYTES, METRIC_B_BYTES), trace.newSpan("UT").start()).join();
    assertEquals(0, trace.spans.size());
    
    trace = new MockTrace(true);
    uid.getNames(UniqueIdType.METRIC, 
        Lists.newArrayList(METRIC_BYTES, METRIC_B_BYTES), trace.newSpan("UT").start()).join();
    verifySpan(Tsdb1xBigtableUniqueIdStore.class.getName() + ".getNames");
  }
  
  @Test
  public void getNamesTraceException() throws Exception {
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    trace = new MockTrace(true);
    Deferred<List<String>> deferred = uid.getNames(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_BYTES, TAGV_BYTES_EX), trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    verifySpan(Tsdb1xBigtableUniqueIdStore.class.getName() + ".getNames", 
        ExecutionException.class);
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void getNamesClientThrowsException() throws Exception {
    // see what happens if we just return an exception
    Tsdb1xBigtableDataStore store = badClient();
    when(store.executor().readRowsAsync(any(ReadRowsRequest.class)))
      .thenThrow(new UnitTestException());
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(store);
    
    Deferred<List<String>> deferred = uid.getNames(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_BYTES, TAGV_B_BYTES), null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
    
    store = badClient();
    when(store.executor().readRowsAsync(any(ReadRowsRequest.class)))
      .thenThrow(new UnitTestException());
    uid = new Tsdb1xBigtableUniqueIdStore(store);
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
    verifySpan(Tsdb1xBigtableUniqueIdStore.class.getName() + ".getNames", 
        UnitTestException.class);
  }

  @Test
  public void getId() throws Exception {
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    assertArrayEquals(METRIC_BYTES, uid.getId(UniqueIdType.METRIC, METRIC_STRING, null).join());
    assertArrayEquals(TAGK_BYTES, uid.getId(UniqueIdType.TAGK, TAGK_STRING, null).join());
    assertArrayEquals(TAGV_BYTES, uid.getId(UniqueIdType.TAGV, TAGV_STRING, null).join());
    
    // this shouldn't encode properly so it won't match the string in the DB!!
    assertNull(uid.getId(UniqueIdType.METRIC, UNI_STRING, null).join());
    
    // now try it with UTF8
    tsdb.config.override(
        Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), 
            Tsdb1xBigtableUniqueIdStore.CONFIG_PREFIX.get(UniqueIdType.METRIC) + 
            Tsdb1xBigtableUniqueIdStore.CHARACTER_SET_KEY), "UTF-8");
    uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    
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
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    
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
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    // exception
    Deferred<byte[]> deferred = uid.getId(UniqueIdType.METRIC, METRIC_STRING_EX, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { 
      assertTrue(e.getCause().getCause() instanceof UnitTestException);
    }
  }
  
  @Test
  public void getIdTracing() throws Exception {
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    // span tests. Only trace on debug.
    trace = new MockTrace();
    assertArrayEquals(METRIC_BYTES, uid.getId(UniqueIdType.METRIC, METRIC_STRING, 
        trace.newSpan("UT").start()).join());
    assertEquals(0, trace.spans.size());
    
    trace = new MockTrace(true);
    assertArrayEquals(METRIC_BYTES, uid.getId(UniqueIdType.METRIC, METRIC_STRING, 
        trace.newSpan("UT").start()).join());
    verifySpan(Tsdb1xBigtableUniqueIdStore.class.getName() + ".getId");
  }
  
  @Test
  public void getIdTraceException() throws Exception {
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    trace = new MockTrace(true);
    try {
      uid.getId(UniqueIdType.METRIC, METRIC_STRING_EX, trace.newSpan("UT").start())
        .join();
      fail("Expected StorageException");
    } catch (StorageException e) { 
      verifySpan(Tsdb1xBigtableUniqueIdStore.class.getName() + ".getId", 
          ExecutionException.class);
    }
  }
  
  @Test
  public void getIdClientThrowsException() throws Exception {
    // see what happens if we just return an exception
    Tsdb1xBigtableDataStore store = badClient();
    when(store.executor().readRowsAsync(any(ReadRowsRequest.class)))
      .thenThrow(new UnitTestException());
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(store);
    
    Deferred<byte[]> deferred = uid.getId(UniqueIdType.TAGV, TAGV_STRING, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
    
    store = badClient();
    when(store.executor().readRowsAsync(any(ReadRowsRequest.class)))
      .thenThrow(new UnitTestException());
    uid = new Tsdb1xBigtableUniqueIdStore(store);
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
    verifySpan(Tsdb1xBigtableUniqueIdStore.class.getName() + ".getId", 
        UnitTestException.class);
  }
  
  @Test
  public void getIds() throws Exception {
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
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
        Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), 
            Tsdb1xBigtableUniqueIdStore.CONFIG_PREFIX.get(UniqueIdType.METRIC) + 
            Tsdb1xBigtableUniqueIdStore.CHARACTER_SET_KEY), "UTF-8");
    uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    
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
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    
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
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    // one throws exception
    Deferred<List<byte[]>> deferred = uid.getIds(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_STRING, TAGV_STRING_EX), null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause().getCause() instanceof UnitTestException);
    }
  }
  
  @Test
  public void getIdsTracing() throws Exception {
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    // span tests. Only trace on debug.
    trace = new MockTrace();
    uid.getIds(UniqueIdType.METRIC, 
        Lists.newArrayList(METRIC_STRING, METRIC_B_STRING), trace.newSpan("UT").start()).join();
    assertEquals(0, trace.spans.size());
    
    trace = new MockTrace(true);
    uid.getIds(UniqueIdType.METRIC, 
        Lists.newArrayList(METRIC_STRING, METRIC_B_STRING), trace.newSpan("UT").start()).join();
    verifySpan(Tsdb1xBigtableUniqueIdStore.class.getName() + ".getIds");
  }
  
  @Test
  public void getIdsTraceException() throws Exception {
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    trace = new MockTrace(true);
    Deferred<List<byte[]>> deferred = uid.getIds(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_STRING, TAGV_STRING_EX), trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    verifySpan(Tsdb1xBigtableUniqueIdStore.class.getName() + ".getIds", 
        ExecutionException.class);
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void getIdsClientThrowsException() throws Exception {
    // see what happens if we just return an exception
    Tsdb1xBigtableDataStore store = badClient();
    when(store.executor().readRowsAsync(any(ReadRowsRequest.class)))
      .thenThrow(new UnitTestException());
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(store);
    
    Deferred<List<byte[]>> deferred = uid.getIds(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_STRING, TAGV_B_STRING), null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) {
      assertTrue(e.getCause() instanceof UnitTestException);
    }
    
    store = badClient();
    when(store.executor().readRowsAsync(any(ReadRowsRequest.class)))
      .thenThrow(new UnitTestException());
    uid = new Tsdb1xBigtableUniqueIdStore(store);
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
    verifySpan(Tsdb1xBigtableUniqueIdStore.class.getName() + ".getIds", 
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
  
  @Test
  public void getOrCreateIdWithExistingId() throws Exception {
    resetAssignmentState();
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    IdOrError result = uid.getOrCreateId(null, 
        UniqueIdType.METRIC, 
        ASSIGNED_ID_NAME, 
        ASSIGNED_DATUM_ID, 
        null).join();
    assertArrayEquals(ASSIGNED_ID, result.id());
    assertNull(result.error());
    assertTrue(uid.pending_assignments.get(UniqueIdType.METRIC).isEmpty());
  }

  @Test  // Test the creation of an ID with no problem.
  public void getOrCreateIdAssignFilterOK() throws Exception {
    resetAssignmentState();
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    IdOrError result = uid.getOrCreateId(null, 
        UniqueIdType.METRIC, 
        UNASSIGNED_ID_NAME, 
        UNASSIGNED_DATUM_ID, 
        null).join();
    assertArrayEquals(UNASSIGNED_ID, result.id());
    assertNull(result.error());
    assertArrayEquals(UNASSIGNED_ID, storage.getColumn(data_store.uidTable(), 
        UNASSIGNED_ID_NAME.getBytes(Const.UTF8_CHARSET), 
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY, 
        Tsdb1xBigtableUniqueIdStore.METRICS_QUAL));
    assertArrayEquals(UNASSIGNED_ID_NAME.getBytes(Const.UTF8_CHARSET), 
        storage.getColumn(data_store.uidTable(), 
            UNASSIGNED_ID, 
            Tsdb1xBigtableUniqueIdStore.NAME_FAMILY, 
            Tsdb1xBigtableUniqueIdStore.METRICS_QUAL));
    assertTrue(uid.pending_assignments.get(UniqueIdType.METRIC).isEmpty());
  }

  @Test
  public void getOrCreateIdAssignFilterBlocked() throws Exception {
    resetAssignmentState();
    when(filter.allowUIDAssignment(any(AuthState.class), any(UniqueIdType.class), anyString(), 
        any(TimeSeriesDatumId.class)))
      .thenReturn(Deferred.fromResult("Nope!"));
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    IdOrError result = uid.getOrCreateId(null, 
        UniqueIdType.METRIC, 
        UNASSIGNED_ID_NAME, 
        UNASSIGNED_DATUM_ID, 
        null).join();
    assertNull(result.id());
    assertEquals("Nope!", result.error());
    assertEquals(WriteState.REJECTED, result.state());
    assertNull(storage.getColumn(data_store.uidTable(), 
        UNASSIGNED_ID_NAME.getBytes(Const.UTF8_CHARSET), 
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY, 
        Tsdb1xBigtableUniqueIdStore.METRICS_QUAL));
    assertNull(storage.getColumn(data_store.uidTable(), 
            UNASSIGNED_ID, 
            Tsdb1xBigtableUniqueIdStore.NAME_FAMILY, 
            Tsdb1xBigtableUniqueIdStore.METRICS_QUAL));
    assertTrue(uid.pending_assignments.get(UniqueIdType.METRIC).isEmpty());
  }

  @Test
  public void getOrCreateIdAssignFilterReturnException() throws Exception{
    resetAssignmentState();
    when(filter.allowUIDAssignment(any(AuthState.class), any(UniqueIdType.class), anyString(), 
        any(TimeSeriesDatumId.class))).thenAnswer(new Answer<Deferred<String>>() {
          @Override
          public Deferred<String> answer(InvocationOnMock invocation)
              throws Throwable {
            return Deferred.fromError(new UnitTestException());
          }
        });
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    Deferred<IdOrError> deferred = uid.getOrCreateId(null, 
        UniqueIdType.METRIC, 
        UNASSIGNED_ID_NAME, 
        UNASSIGNED_DATUM_ID, 
        null);
    try {
      deferred.join();
      fail("Expected UnitTestException");
    } catch (UnitTestException e) { }
    assertNull(storage.getColumn(data_store.uidTable(), 
        UNASSIGNED_ID_NAME.getBytes(Const.UTF8_CHARSET), 
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY, 
        Tsdb1xBigtableUniqueIdStore.METRICS_QUAL));
    assertNull(storage.getColumn(data_store.uidTable(), 
            UNASSIGNED_ID, 
            Tsdb1xBigtableUniqueIdStore.NAME_FAMILY, 
            Tsdb1xBigtableUniqueIdStore.METRICS_QUAL));
    assertTrue(uid.pending_assignments.get(UniqueIdType.METRIC).isEmpty());
  }
  
  @Test
  public void getOrCreateIdAssignFilterThrowsException() throws Exception {
    resetAssignmentState();
    when(filter.allowUIDAssignment(any(AuthState.class), any(UniqueIdType.class), anyString(), 
        any(TimeSeriesDatumId.class))).thenThrow(new UnitTestException());
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    Deferred<IdOrError> deferred = uid.getOrCreateId(null, 
        UniqueIdType.METRIC, 
        UNASSIGNED_ID_NAME, 
        UNASSIGNED_DATUM_ID, 
        null);
    try {
      deferred.join();
      fail("Expected UnitTestException");
    } catch (UnitTestException e) { }
    assertNull(storage.getColumn(data_store.uidTable(), 
        UNASSIGNED_ID_NAME.getBytes(Const.UTF8_CHARSET), 
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY, 
        Tsdb1xBigtableUniqueIdStore.METRICS_QUAL));
    assertNull(storage.getColumn(data_store.uidTable(), 
            UNASSIGNED_ID, 
            Tsdb1xBigtableUniqueIdStore.NAME_FAMILY, 
            Tsdb1xBigtableUniqueIdStore.METRICS_QUAL));
    assertTrue(uid.pending_assignments.get(UniqueIdType.METRIC).isEmpty());
  }

  @Test  // Test the creation of an ID when unable to increment MAXID
  public void getOrCreateIdUnableToIncrementMaxId() throws Exception {
    resetAssignmentState();
    storage.addColumn(MockBigtable.UID_TABLE, 
        Tsdb1xBigtableUniqueIdStore.MAXID_ROW,
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY,
        Tsdb1xBigtableUniqueIdStore.METRICS_QUAL, 
        new byte[] { 0, 0, 0, 0, 0, (byte) 255, (byte) 255, (byte) 255 });
    
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    Deferred<IdOrError> deferred = uid.getOrCreateId(null, 
        UniqueIdType.METRIC, 
        UNASSIGNED_ID_NAME, 
        UNASSIGNED_DATUM_ID, 
        null);
    try {
      deferred.join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    assertNull(storage.getColumn(data_store.uidTable(), 
        UNASSIGNED_ID_NAME.getBytes(Const.UTF8_CHARSET), 
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY, 
        Tsdb1xBigtableUniqueIdStore.METRICS_QUAL));
    assertNull(storage.getColumn(data_store.uidTable(), 
            UNASSIGNED_ID, 
            Tsdb1xBigtableUniqueIdStore.NAME_FAMILY, 
            Tsdb1xBigtableUniqueIdStore.METRICS_QUAL));
    assertTrue(uid.pending_assignments.get(UniqueIdType.METRIC).isEmpty());
  }
  
  @Test  // Failure due to negative id.
  public void getOrCreateIdUnableToIncrementRolledID() throws Exception {
    resetAssignmentState();
    storage.addColumn(MockBigtable.UID_TABLE, 
        Tsdb1xBigtableUniqueIdStore.MAXID_ROW,
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY,
        Tsdb1xBigtableUniqueIdStore.METRICS_QUAL, 
        Bytes.fromLong(Long.MAX_VALUE));
    
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    Deferred<IdOrError> deferred = uid.getOrCreateId(null, 
        UniqueIdType.METRIC, 
        UNASSIGNED_ID_NAME, 
        UNASSIGNED_DATUM_ID, 
        null);
    try {
      deferred.join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    assertNull(storage.getColumn(data_store.uidTable(), 
        UNASSIGNED_ID_NAME.getBytes(Const.UTF8_CHARSET), 
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY, 
        Tsdb1xBigtableUniqueIdStore.METRICS_QUAL));
    assertNull(storage.getColumn(data_store.uidTable(), 
            UNASSIGNED_ID, 
            Tsdb1xBigtableUniqueIdStore.NAME_FAMILY, 
            Tsdb1xBigtableUniqueIdStore.METRICS_QUAL));
    assertTrue(uid.pending_assignments.get(UniqueIdType.METRIC).isEmpty());
  }
  
  @Test  // Failure due to negative id.
  public void getOrCreateIdUnableToIncrementCorruptId() throws Exception {
    resetAssignmentState();
    storage.addColumn(MockBigtable.UID_TABLE, 
        Tsdb1xBigtableUniqueIdStore.MAXID_ROW,
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY,
        Tsdb1xBigtableUniqueIdStore.METRICS_QUAL, 
        new byte[] { 0, 0 });
    
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    Deferred<IdOrError> deferred = uid.getOrCreateId(null, 
        UniqueIdType.METRIC, 
        UNASSIGNED_ID_NAME, 
        UNASSIGNED_DATUM_ID, 
        null);
    try {
      deferred.join();
      // TODO - fix this in asynchbase.
      fail("Expected StorageException");
    } catch (StorageException e) { }
    assertNull(storage.getColumn(data_store.uidTable(), 
        UNASSIGNED_ID_NAME.getBytes(Const.UTF8_CHARSET), 
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY, 
        Tsdb1xBigtableUniqueIdStore.METRICS_QUAL));
    assertNull(storage.getColumn(data_store.uidTable(), 
            UNASSIGNED_ID, 
            Tsdb1xBigtableUniqueIdStore.NAME_FAMILY, 
            Tsdb1xBigtableUniqueIdStore.METRICS_QUAL));
    assertTrue(uid.pending_assignments.get(UniqueIdType.METRIC).isEmpty());
  }
  
  @Test  // Test the creation of an ID with a race condition.
  public void getOrCreateIdAssignIdWithRaceConditionReverseMap() throws Exception {
    resetAssignmentState();
    Tsdb1xBigtableDataStore data_store_a = mock(Tsdb1xBigtableDataStore.class);
    AsyncExecutor executor = mock(AsyncExecutor.class);
    when(data_store_a.executor()).thenReturn(executor);
    when(data_store_a.schema()).thenReturn(schema);
    when(data_store_a.tsdb()).thenReturn(tsdb);
    when(data_store_a.uidTable()).thenReturn(MockBigtable.UID_TABLE);
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store_a);
    
    when(executor.readRowsAsync(any(ReadRowsRequest.class)))
      .thenReturn(mockGetResponse(null));
    
    when(executor.readModifyWriteRowAsync(any(ReadModifyWriteRowRequest.class)))
      .thenReturn(mockAtomicIncrement(5L))
      .thenReturn(mockAtomicIncrement(6L));
    
    when(executor.checkAndMutateRowAsync(any(CheckAndMutateRowRequest.class)))
      .thenReturn(mockCAS(false))
      .thenReturn(mockCAS(true))
      .thenReturn(mockCAS(true));
    
    Deferred<IdOrError> deferred = uid.getOrCreateId(null, 
        UniqueIdType.METRIC, 
        UNASSIGNED_ID_NAME, 
        UNASSIGNED_DATUM_ID, 
        null);
    try {
      deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();
    
    IdOrError result = deferred.join();
    assertArrayEquals(new byte[] { 0, 0, 6 }, result.id());
    assertTrue(uid.pending_assignments.get(UniqueIdType.METRIC).isEmpty());
  }
  
  @Test  // Test the creation of an ID with a race condition.
  public void getOrCreateIdAssignIdWithRaceConditionForwardMap() throws Exception {
    resetAssignmentState();
    Tsdb1xBigtableDataStore data_store_a = mock(Tsdb1xBigtableDataStore.class);
    AsyncExecutor executor = mock(AsyncExecutor.class);
    when(data_store_a.executor()).thenReturn(executor);
    when(data_store_a.schema()).thenReturn(schema);
    when(data_store_a.tsdb()).thenReturn(tsdb);
    when(data_store_a.uidTable()).thenReturn(MockBigtable.UID_TABLE);
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store_a);
    
    when(executor.readRowsAsync(any(ReadRowsRequest.class)))
      .thenReturn(mockGetResponse(null))
      .thenReturn(mockGetResponse(new byte[] { 0, 0, 5 }));
    
    when(executor.readModifyWriteRowAsync(any(ReadModifyWriteRowRequest.class)))
      .thenReturn(mockAtomicIncrement(5L));
    
    when(executor.checkAndMutateRowAsync(any(CheckAndMutateRowRequest.class)))
      .thenReturn(mockCAS(true))
      .thenReturn(mockCAS(false));
    
    IdOrError result = uid.getOrCreateId(null, 
        UniqueIdType.METRIC, 
        UNASSIGNED_ID_NAME, 
        UNASSIGNED_DATUM_ID, 
        null).join();
    assertArrayEquals(new byte[] { 0, 0, 5 }, result.id());
    assertTrue(uid.pending_assignments.get(UniqueIdType.METRIC).isEmpty());
  }
  
  @Test  // Test the creation of an ID with a race condition.
  public void getOrCreateIdTooManyAttempts() throws Exception {
    resetAssignmentState();
    Tsdb1xBigtableDataStore data_store_a = mock(Tsdb1xBigtableDataStore.class);
    AsyncExecutor executor = mock(AsyncExecutor.class);
    when(data_store_a.executor()).thenReturn(executor);
    when(data_store_a.schema()).thenReturn(schema);
    when(data_store_a.tsdb()).thenReturn(tsdb);
    when(data_store_a.uidTable()).thenReturn(MockBigtable.UID_TABLE);
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store_a);
    
    when(executor.readRowsAsync(any(ReadRowsRequest.class)))
      .thenReturn(mockGetResponse(null));
    
    when(executor.readModifyWriteRowAsync(any(ReadModifyWriteRowRequest.class)))
      .thenReturn(mockAtomicIncrement(5L))
      .thenReturn(mockAtomicIncrement(6L))
      .thenReturn(mockAtomicIncrement(7L))
      .thenReturn(mockAtomicIncrement(8L));
    
    when(executor.checkAndMutateRowAsync(any(CheckAndMutateRowRequest.class)))
      .thenReturn(mockCAS(false))
      .thenReturn(mockCAS(false))
      .thenReturn(mockCAS(false))
      .thenReturn(mockCAS(false));
    
    Deferred<IdOrError> deferred = uid.getOrCreateId(null, 
        UniqueIdType.METRIC, 
        UNASSIGNED_ID_NAME, 
        UNASSIGNED_DATUM_ID, 
        null);
    
    try {
      deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();
    
    IdOrError result = deferred.join();
    assertNull(result.id());
    assertEquals(WriteState.RETRY, result.state());
    assertNotNull(result.error());
    assertTrue(uid.pending_assignments.get(UniqueIdType.METRIC).isEmpty());
  }
  
  @Test  // Test the creation of an ID with a race condition.
  public void getOrCreateIdIncException() throws Exception {
    resetAssignmentState();
    Tsdb1xBigtableDataStore data_store_a = mock(Tsdb1xBigtableDataStore.class);
    AsyncExecutor executor = mock(AsyncExecutor.class);
    when(data_store_a.executor()).thenReturn(executor);
    when(data_store_a.schema()).thenReturn(schema);
    when(data_store_a.tsdb()).thenReturn(tsdb);
    when(data_store_a.uidTable()).thenReturn(MockBigtable.UID_TABLE);
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store_a);
    
    when(executor.readRowsAsync(any(ReadRowsRequest.class)))
      .thenReturn(mockGetResponse(null));
    
    when(executor.readModifyWriteRowAsync(any(ReadModifyWriteRowRequest.class)))
      .thenReturn(mockAtomicIncrement(new UnitTestException()));
    
    when(executor.checkAndMutateRowAsync(any(CheckAndMutateRowRequest.class)))
      .thenReturn(mockCAS(false));
    
    Deferred<IdOrError> deferred = uid.getOrCreateId(null, 
        UniqueIdType.METRIC, 
        UNASSIGNED_ID_NAME, 
        UNASSIGNED_DATUM_ID, 
        null);
    try {
      deferred.join(1);
      fail("Expected ExecutionException e");
    } catch (ExecutionException e) { }
    assertTrue(uid.pending_assignments.get(UniqueIdType.METRIC).isEmpty());
  }
  
  @Test  // Test the creation of an ID with a race condition.
  public void getOrCreateIdCASException() throws Exception {
    resetAssignmentState();
    Tsdb1xBigtableDataStore data_store_a = mock(Tsdb1xBigtableDataStore.class);
    AsyncExecutor executor = mock(AsyncExecutor.class);
    when(data_store_a.executor()).thenReturn(executor);
    when(data_store_a.schema()).thenReturn(schema);
    when(data_store_a.tsdb()).thenReturn(tsdb);
    when(data_store_a.uidTable()).thenReturn(MockBigtable.UID_TABLE);
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store_a);
    
    when(executor.readRowsAsync(any(ReadRowsRequest.class)))
      .thenReturn(mockGetResponse(null));
    
    when(executor.readModifyWriteRowAsync(any(ReadModifyWriteRowRequest.class)))
      .thenReturn(mockAtomicIncrement(5L));
    
    when(executor.checkAndMutateRowAsync(any(CheckAndMutateRowRequest.class)))
      .thenReturn(mockCAS(new UnitTestException()));
    
    Deferred<IdOrError> deferred = uid.getOrCreateId(null, 
        UniqueIdType.METRIC, 
        UNASSIGNED_ID_NAME, 
        UNASSIGNED_DATUM_ID, 
        null);
    try {
      deferred.join(1);
      fail("Expected ExecutionException");
    } catch (ExecutionException e) { }
    assertTrue(uid.pending_assignments.get(UniqueIdType.METRIC).isEmpty());
  }
  
  @Test
  public void getOrCreateIdRandom() throws Exception {
    resetAssignmentState();
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    Whitebox.setInternalState(uid, "randomize_metric_ids", true);
    IdOrError result = uid.getOrCreateId(null, 
        UniqueIdType.METRIC, 
        UNASSIGNED_ID_NAME, 
        UNASSIGNED_DATUM_ID,
        null).join();
    assertTrue(Bytes.memcmp(UNASSIGNED_ID, result.id()) != 0);
    assertEquals(3, result.id().length);
    assertNull(result.error());

    assertArrayEquals(result.id(), storage.getColumn(data_store.uidTable(), 
        UNASSIGNED_ID_NAME.getBytes(Const.UTF8_CHARSET), 
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY, 
        Tsdb1xBigtableUniqueIdStore.METRICS_QUAL));
    assertArrayEquals(UNASSIGNED_ID_NAME.getBytes(Const.UTF8_CHARSET), 
        storage.getColumn(data_store.uidTable(), 
            result.id(), 
            Tsdb1xBigtableUniqueIdStore.NAME_FAMILY, 
            Tsdb1xBigtableUniqueIdStore.METRICS_QUAL));
    assertTrue(uid.pending_assignments.get(UniqueIdType.METRIC).isEmpty());
  }
  
  @Test
  public void getOrCreateIdRandomCollision() throws Exception {
    resetAssignmentState();
    
    PowerMockito.mockStatic(RandomUniqueId.class);
    when(RandomUniqueId.getRandomUID(anyInt()))
      .thenReturn(24898L)
      .thenReturn(42L);
    
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    Whitebox.setInternalState(uid, "randomize_metric_ids", true);
    
    Deferred<IdOrError> deferred = uid.getOrCreateId(null, 
        UniqueIdType.METRIC, 
        UNASSIGNED_ID_NAME, 
        UNASSIGNED_DATUM_ID,
        null);
    
    try {
      deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();
    
    IdOrError result = deferred.join();
    assertTrue(Bytes.memcmp(UNASSIGNED_ID, result.id()) != 0);
    assertEquals(3, result.id().length);
    assertNull(result.error());

    assertArrayEquals(result.id(), storage.getColumn(data_store.uidTable(), 
        UNASSIGNED_ID_NAME.getBytes(Const.UTF8_CHARSET), 
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY, 
        Tsdb1xBigtableUniqueIdStore.METRICS_QUAL));
    assertArrayEquals(UNASSIGNED_ID_NAME.getBytes(Const.UTF8_CHARSET), 
        storage.getColumn(data_store.uidTable(), 
            result.id(), 
            Tsdb1xBigtableUniqueIdStore.NAME_FAMILY, 
            Tsdb1xBigtableUniqueIdStore.METRICS_QUAL));
    assertTrue(uid.pending_assignments.get(UniqueIdType.METRIC).isEmpty());
  }
  
  @Test
  public void getOrCreateIdRandomCollisionTooManyAttempts() throws Exception {
    resetAssignmentState();
    
    PowerMockito.mockStatic(RandomUniqueId.class);
    when(RandomUniqueId.getRandomUID(anyInt()))
      .thenReturn(24898L)
      .thenReturn(24898L)
      .thenReturn(24898L);
    
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    Whitebox.setInternalState(uid, "randomize_metric_ids", true);
    Whitebox.setInternalState(uid, "max_attempts_assign_random", (short) 3); 
    Deferred<IdOrError> deferred = uid.getOrCreateId(null, 
        UniqueIdType.METRIC, 
        UNASSIGNED_ID_NAME, 
        UNASSIGNED_DATUM_ID,
        null);
    
    try {
      deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();
    
    IdOrError result = deferred.join();
    assertNull(result.id());
    assertEquals(WriteState.RETRY, result.state());
    assertNotNull(result.error());

    assertNull(storage.getColumn(data_store.uidTable(), 
        UNASSIGNED_ID_NAME.getBytes(Const.UTF8_CHARSET), 
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY, 
        Tsdb1xBigtableUniqueIdStore.METRICS_QUAL));
    assertTrue(uid.pending_assignments.get(UniqueIdType.METRIC).isEmpty());
  }
  
  @Test
  public void getOrCreateIdRandomWithRaceConditionReverseMap() throws Exception {
    resetAssignmentState();
    
    PowerMockito.mockStatic(RandomUniqueId.class);
    when(RandomUniqueId.getRandomUID(anyInt()))
      .thenReturn(1L)
      .thenReturn(42L);
    
    resetAssignmentState();
    Tsdb1xBigtableDataStore data_store_a = mock(Tsdb1xBigtableDataStore.class);
    AsyncExecutor executor = mock(AsyncExecutor.class);
    when(data_store_a.executor()).thenReturn(executor);
    when(data_store_a.schema()).thenReturn(schema);
    when(data_store_a.tsdb()).thenReturn(tsdb);
    when(data_store_a.uidTable()).thenReturn(MockBigtable.UID_TABLE);
    
    when(executor.readRowsAsync(any(ReadRowsRequest.class)))
      .thenReturn(mockGetResponse(null));
    
    when(executor.checkAndMutateRowAsync(any(CheckAndMutateRowRequest.class)))
      .thenReturn(mockCAS(false))
      .thenReturn(mockCAS(true))
      .thenReturn(mockCAS(true));
    
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store_a);
    Whitebox.setInternalState(uid, "randomize_metric_ids", true);
    
    Deferred<IdOrError> deferred = uid.getOrCreateId(null, 
        UniqueIdType.METRIC, 
        UNASSIGNED_ID_NAME, 
        UNASSIGNED_DATUM_ID,
        null);
    
    try {
      deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();
    
    IdOrError result = deferred.join();
    assertTrue(Bytes.memcmp(UNASSIGNED_ID, result.id()) != 0);
    assertEquals(3, result.id().length);
    assertNull(result.error());
    assertTrue(uid.pending_assignments.get(UniqueIdType.METRIC).isEmpty());
  }
  
  @Test
  public void getOrCreateIdRandomWithRaceConditionForwardMap() throws Exception {
    resetAssignmentState();
    
    PowerMockito.mockStatic(RandomUniqueId.class);
    when(RandomUniqueId.getRandomUID(anyInt()))
      .thenReturn(1L);
    
    resetAssignmentState();
    Tsdb1xBigtableDataStore data_store_a = mock(Tsdb1xBigtableDataStore.class);
    AsyncExecutor executor = mock(AsyncExecutor.class);
    when(data_store_a.executor()).thenReturn(executor);
    when(data_store_a.schema()).thenReturn(schema);
    when(data_store_a.tsdb()).thenReturn(tsdb);
    when(data_store_a.uidTable()).thenReturn(MockBigtable.UID_TABLE);
    
    when(executor.readRowsAsync(any(ReadRowsRequest.class)))
      .thenReturn(mockGetResponse(null))
      .thenReturn(mockGetResponse(new byte[] { 0, 0, 1 }));
    
    when(executor.checkAndMutateRowAsync(any(CheckAndMutateRowRequest.class)))
      .thenReturn(mockCAS(true))
      .thenReturn(mockCAS(false))
      .thenReturn(mockCAS(true));
    
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store_a);
    Whitebox.setInternalState(uid, "randomize_metric_ids", true);
    
    IdOrError result = uid.getOrCreateId(null, 
        UniqueIdType.METRIC, 
        UNASSIGNED_ID_NAME, 
        UNASSIGNED_DATUM_ID,
        null).join();
    assertArrayEquals(new byte[] { 0, 0, 1 }, result.id());
    assertEquals(3, result.id().length);
    assertNull(result.error());
    assertTrue(uid.pending_assignments.get(UniqueIdType.METRIC).isEmpty());
  }
  
  @Test
  public void getOrCreateIdAlreadyWaiting() throws Exception {
    resetAssignmentState();
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    Map<String, Deferred<IdOrError>> waiting = 
        uid.pending_assignments.get(UniqueIdType.METRIC); 
    Deferred<IdOrError> deferred = new Deferred<IdOrError>();
    waiting.put(UNASSIGNED_ID_NAME, deferred);
    
    Deferred<IdOrError> assign1 = uid.getOrCreateId(null, 
        UniqueIdType.METRIC, 
        UNASSIGNED_ID_NAME, 
        UNASSIGNED_DATUM_ID, 
        null);
    
    try {
      assign1.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    Deferred<IdOrError> assign2 = uid.getOrCreateId(null, 
        UniqueIdType.METRIC, 
        UNASSIGNED_ID_NAME, 
        UNASSIGNED_DATUM_ID, 
        null);
    try {
      assign2.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    deferred.callback(IdOrError.wrapId(UNASSIGNED_ID));
    
    IdOrError result = assign1.join();
    assertArrayEquals(UNASSIGNED_ID, result.id());
    assertNull(result.error());
    
    result = assign2.join();
    assertArrayEquals(UNASSIGNED_ID, result.id());
    assertNull(result.error());
  }
  
  @Test
  public void getOrCreateIdAssignAndRetry() throws Exception {
    resetAssignmentState();
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    Whitebox.setInternalState(uid, "assign_and_retry", true);
    IdOrError result = uid.getOrCreateId(null, 
        UniqueIdType.METRIC, 
        UNASSIGNED_ID_NAME, 
        UNASSIGNED_DATUM_ID, 
        null).join();
    assertNull(result.id());
    assertEquals(WriteState.RETRY, result.state());
    assertSame(Tsdb1xBigtableUniqueIdStore.ASSIGN_AND_RETRY, result.error());
    
    // still assigns
    assertArrayEquals(UNASSIGNED_ID, storage.getColumn(data_store.uidTable(), 
        UNASSIGNED_ID_NAME.getBytes(Const.UTF8_CHARSET), 
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY, 
        Tsdb1xBigtableUniqueIdStore.METRICS_QUAL));
    assertArrayEquals(UNASSIGNED_ID_NAME.getBytes(Const.UTF8_CHARSET), 
        storage.getColumn(data_store.uidTable(), 
            UNASSIGNED_ID, 
            Tsdb1xBigtableUniqueIdStore.NAME_FAMILY, 
            Tsdb1xBigtableUniqueIdStore.METRICS_QUAL));
    assertTrue(uid.pending_assignments.get(UniqueIdType.METRIC).isEmpty());
  }

  @Test
  public void getOrCreateIdsAssignOne() throws Exception {
    resetAssignmentState();
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    
    List<String> names = Lists.newArrayList(ASSIGNED_TAGV_NAME,
        UNASSIGNED_TAGV_NAME);
    List<IdOrError> result = uid.getOrCreateIds(null, 
        UniqueIdType.TAGV, 
        names, 
        ASSIGNED_DATUM_ID, 
        null).join();
    
    assertEquals(2, result.size());
    assertArrayEquals(ASSIGNED_TAGV, result.get(0).id());
    assertNull(result.get(0).error());
    assertArrayEquals(UNASSIGNED_TAGV, result.get(1).id());
    assertNull(result.get(1).error());
    
    assertArrayEquals(UNASSIGNED_TAGV, storage.getColumn(data_store.uidTable(), 
        UNASSIGNED_TAGV_NAME.getBytes(Const.UTF8_CHARSET), 
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY, 
        Tsdb1xBigtableUniqueIdStore.TAG_VALUE_QUAL));
    assertArrayEquals(UNASSIGNED_TAGV_NAME.getBytes(Const.UTF8_CHARSET), 
        storage.getColumn(data_store.uidTable(), 
            UNASSIGNED_TAGV, 
            Tsdb1xBigtableUniqueIdStore.NAME_FAMILY, 
            Tsdb1xBigtableUniqueIdStore.TAG_VALUE_QUAL));
    assertTrue(uid.pending_assignments.get(UniqueIdType.TAGV).isEmpty());
  }
  
  @Test
  public void getOrCreateIdsAssignAndRetry() throws Exception {
    resetAssignmentState();
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    Whitebox.setInternalState(uid, "assign_and_retry", true);
    
    List<String> names = Lists.newArrayList(ASSIGNED_TAGV_NAME,
        UNASSIGNED_TAGV_NAME);
    List<IdOrError> result = uid.getOrCreateIds(null, 
        UniqueIdType.TAGV, 
        names, 
        ASSIGNED_DATUM_ID, 
        null).join();
    
    assertEquals(2, result.size());
    assertArrayEquals(ASSIGNED_TAGV, result.get(0).id());
    assertNull(result.get(0).error());
    assertNull(result.get(1).id());
    assertEquals(WriteState.RETRY, result.get(1).state());
    assertSame(Tsdb1xBigtableUniqueIdStore.ASSIGN_AND_RETRY, result.get(1).error());
    
    assertArrayEquals(UNASSIGNED_TAGV, storage.getColumn(data_store.uidTable(), 
        UNASSIGNED_TAGV_NAME.getBytes(Const.UTF8_CHARSET), 
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY, 
        Tsdb1xBigtableUniqueIdStore.TAG_VALUE_QUAL));
    assertArrayEquals(UNASSIGNED_TAGV_NAME.getBytes(Const.UTF8_CHARSET), 
        storage.getColumn(data_store.uidTable(), 
            UNASSIGNED_TAGV, 
            Tsdb1xBigtableUniqueIdStore.NAME_FAMILY, 
            Tsdb1xBigtableUniqueIdStore.TAG_VALUE_QUAL));
    assertTrue(uid.pending_assignments.get(UniqueIdType.TAGV).isEmpty());
  }
  
  @Test
  public void getOrCreateIdsAssignException() throws Exception {
    resetAssignmentState();
    Tsdb1xBigtableUniqueIdStore uid = new Tsdb1xBigtableUniqueIdStore(data_store);
    
    List<String> names = Lists.newArrayList(TAGV_STRING_EX,
        UNASSIGNED_TAGV_NAME);
    List<IdOrError> result = uid.getOrCreateIds(null, 
        UniqueIdType.TAGV, 
        names, 
        ASSIGNED_DATUM_ID, 
        null).join();
    
    assertEquals(2, result.size());
    assertNull(result.get(0).id());
    assertNotNull(result.get(0).error());
    assertNotNull(result.get(0).exception());
    assertArrayEquals(UNASSIGNED_TAGV, result.get(1).id());
    assertNull(result.get(1).error());
    assertNull(result.get(1).exception());
    
    assertArrayEquals(UNASSIGNED_TAGV, storage.getColumn(data_store.uidTable(), 
        UNASSIGNED_TAGV_NAME.getBytes(Const.UTF8_CHARSET), 
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY, 
        Tsdb1xBigtableUniqueIdStore.TAG_VALUE_QUAL));
    assertArrayEquals(UNASSIGNED_TAGV_NAME.getBytes(Const.UTF8_CHARSET), 
        storage.getColumn(data_store.uidTable(), 
            UNASSIGNED_TAGV, 
            Tsdb1xBigtableUniqueIdStore.NAME_FAMILY, 
            Tsdb1xBigtableUniqueIdStore.TAG_VALUE_QUAL));
    assertTrue(uid.pending_assignments.get(UniqueIdType.TAGV).isEmpty());
  }
  
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

  private static ListenableFuture<List<Row>> mockGetResponse( 
      final byte[] response) {
    final List<Row> results = Lists.newArrayList();
    if (response != null) {
      results.add(Row.newBuilder()
          .addFamilies(Family.newBuilder()
              .addColumns(Column.newBuilder()
                  .addCells(Cell.newBuilder()
                      .setValue(ByteStringer.wrap(response)))))
          .build());
    }
    
    return new ListenableFuture<List<Row>>() {

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return true;
      }

      @Override
      public List<Row> get() throws InterruptedException, ExecutionException {
        if (response == null) {
          return Collections.emptyList();
        }
        return results;
      }

      @Override
      public List<Row> get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException,
          java.util.concurrent.TimeoutException {
        if (response == null) {
          return Collections.emptyList();
        }
        return results;
      }

      @Override
      public void addListener(Runnable listener, Executor executor) {
        // TODO - super mega ugly reflection because Futures$CallbackListener
        // is a private static class.
        final FutureCallback<List<Row>> callback = 
            (FutureCallback<List<Row>>) 
              Whitebox.getInternalState(listener, "callback");
        callback.onSuccess(results);
      }
      
    };
  }
  
  private static ListenableFuture<ReadModifyWriteRowResponse> mockAtomicIncrement(
      final long new_value) {
    ReadModifyWriteRowResponse response = 
        ReadModifyWriteRowResponse.newBuilder()
        .setRow(Row.newBuilder()
            .addFamilies(Family.newBuilder()
                .addColumns(Column.newBuilder()
                    .addCells(Cell.newBuilder()
                        .setValue(ByteStringer.wrap(Bytes.fromLong(new_value)))))))
        .build();
    return new ListenableFuture<ReadModifyWriteRowResponse>() {

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return true;
      }

      @Override
      public ReadModifyWriteRowResponse get()
          throws InterruptedException, ExecutionException {
        return response;
      }

      @Override
      public ReadModifyWriteRowResponse get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException,
          java.util.concurrent.TimeoutException {
        return response;
      }

      @Override
      public void addListener(Runnable listener, Executor executor) {
        // TODO - super mega ugly reflection because Futures$CallbackListener
        // is a private static class.
        final FutureCallback<ReadModifyWriteRowResponse> callback = 
            (FutureCallback<ReadModifyWriteRowResponse>) 
              Whitebox.getInternalState(listener, "callback");
        callback.onSuccess(response);
      }
      
    };
  }
  
  private static ListenableFuture<ReadModifyWriteRowResponse> mockAtomicIncrement(
      final Exception exception) {
    return new ListenableFuture<ReadModifyWriteRowResponse>() {

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return true;
      }

      @Override
      public ReadModifyWriteRowResponse get()
          throws InterruptedException, ExecutionException {
        throw new ExecutionException(exception);
      }

      @Override
      public ReadModifyWriteRowResponse get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException,
          java.util.concurrent.TimeoutException {
        throw new ExecutionException(exception);
      }

      @Override
      public void addListener(Runnable listener, Executor executor) {
        // TODO - super mega ugly reflection because Futures$CallbackListener
        // is a private static class.
        final FutureCallback<ReadModifyWriteRowResponse> callback = 
            (FutureCallback<ReadModifyWriteRowResponse>) 
              Whitebox.getInternalState(listener, "callback");
        callback.onFailure(new ExecutionException(exception));
      }
      
    };
  }
  
  private static ListenableFuture<CheckAndMutateRowResponse> mockCAS(
      final boolean satisfied) {
    CheckAndMutateRowResponse response = CheckAndMutateRowResponse.newBuilder()
        .setPredicateMatched(!satisfied) // flipped per Bigtable's implementation
        .build();
    return new ListenableFuture<CheckAndMutateRowResponse>() {

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return false;
      }

      @Override
      public CheckAndMutateRowResponse get()
          throws InterruptedException, ExecutionException {
        return response;
      }

      @Override
      public CheckAndMutateRowResponse get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException,
          java.util.concurrent.TimeoutException {
        return response;
      }

      @Override
      public void addListener(Runnable listener, Executor executor) {
        // TODO - super mega ugly reflection because Futures$CallbackListener
        // is a private static class.
        final FutureCallback<CheckAndMutateRowResponse> callback = 
            (FutureCallback<CheckAndMutateRowResponse>) 
              Whitebox.getInternalState(listener, "callback");
        callback.onSuccess(response);
      }
      
    };
  }
  
  private static ListenableFuture<CheckAndMutateRowResponse> mockCAS(
      final Exception exception) {
    return new ListenableFuture<CheckAndMutateRowResponse>() {

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return false;
      }

      @Override
      public CheckAndMutateRowResponse get()
          throws InterruptedException, ExecutionException {
        throw new ExecutionException(exception);
      }

      @Override
      public CheckAndMutateRowResponse get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException,
          java.util.concurrent.TimeoutException {
        throw new ExecutionException(exception);
      }

      @Override
      public void addListener(Runnable listener, Executor executor) {
        // TODO - super mega ugly reflection because Futures$CallbackListener
        // is a private static class.
        final FutureCallback<CheckAndMutateRowResponse> callback = 
            (FutureCallback<CheckAndMutateRowResponse>) 
              Whitebox.getInternalState(listener, "callback");
        callback.onFailure(new ExecutionException(exception));
      }
      
    };
  }
  
  private static byte[] emptyArray() {
    return eq(new byte[0]);
  }

  private static void resetConfig() {
    final UnitTestConfiguration c = tsdb.config;
    if (c.hasProperty(Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), 
        Tsdb1xBigtableUniqueIdStore.CONFIG_PREFIX.get(UniqueIdType.METRIC) + 
        Tsdb1xBigtableUniqueIdStore.CHARACTER_SET_KEY))) {
      // restore
      tsdb.config.override(
          Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), 
              Tsdb1xBigtableUniqueIdStore.CONFIG_PREFIX.get(UniqueIdType.METRIC) + 
              Tsdb1xBigtableUniqueIdStore.CHARACTER_SET_KEY), Tsdb1xBigtableUniqueIdStore.CHARACTER_SET_DEFAULT);
    }
  }
  
  private void resetAssignmentState() {
    filter = mock(UniqueIdAssignmentAuthorizer.class);
    when(filter.fillterUIDAssignments()).thenReturn(true);
    when(filter.allowUIDAssignment(any(AuthState.class), any(UniqueIdType.class), anyString(), 
        any(TimeSeriesDatumId.class)))
      .thenReturn(Deferred.fromResult(null))
      .thenReturn(Deferred.fromResult(null));
    
    timer = new FakeTaskTimer();
    tsdb.timer = timer;
    
    when(tsdb.registry.getDefaultPlugin(
        UniqueIdAssignmentAuthorizer.class)).thenReturn(filter);
    
    // clean out the UID table.
    storage.flushRow(MockBigtable.UID_TABLE, ASSIGNED_ID_NAME.getBytes(Const.UTF8_CHARSET));
    storage.flushRow(MockBigtable.UID_TABLE, ASSIGNED_ID);
    storage.flushRow(MockBigtable.UID_TABLE, ASSIGNED_TAGV_NAME.getBytes(Const.UTF8_CHARSET));
    storage.flushRow(MockBigtable.UID_TABLE, ASSIGNED_TAGV);
    storage.flushRow(MockBigtable.UID_TABLE, UNASSIGNED_ID_NAME.getBytes(Const.UTF8_CHARSET));
    storage.flushRow(MockBigtable.UID_TABLE, UNASSIGNED_ID);
    storage.flushRow(MockBigtable.UID_TABLE, UNASSIGNED_TAGV_NAME.getBytes(Const.UTF8_CHARSET));
    storage.flushRow(MockBigtable.UID_TABLE, UNASSIGNED_TAGV);
    
    storage.addColumn(MockBigtable.UID_TABLE, 
        ASSIGNED_ID_NAME.getBytes(Const.UTF8_CHARSET), 
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY, 
        Tsdb1xBigtableUniqueIdStore.METRICS_QUAL, 
        ASSIGNED_ID);
    storage.addColumn(MockBigtable.UID_TABLE, 
        ASSIGNED_ID, 
        Tsdb1xBigtableUniqueIdStore.NAME_FAMILY, 
        Tsdb1xBigtableUniqueIdStore.METRICS_QUAL, 
        ASSIGNED_ID_NAME.getBytes(Const.UTF8_CHARSET));
    
    storage.addColumn(MockBigtable.UID_TABLE, 
        ASSIGNED_TAGV_NAME.getBytes(Const.UTF8_CHARSET), 
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY, 
        Tsdb1xBigtableUniqueIdStore.TAG_VALUE_QUAL, 
        ASSIGNED_TAGV);
    storage.addColumn(MockBigtable.UID_TABLE, 
        ASSIGNED_TAGV, 
        Tsdb1xBigtableUniqueIdStore.NAME_FAMILY, 
        Tsdb1xBigtableUniqueIdStore.TAG_VALUE_QUAL, 
        ASSIGNED_TAGV_NAME.getBytes(Const.UTF8_CHARSET));
    
    // set an atomic long for metrics and tag values
    storage.addColumn(MockBigtable.UID_TABLE, 
        Tsdb1xBigtableUniqueIdStore.MAXID_ROW,
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY,
        Tsdb1xBigtableUniqueIdStore.METRICS_QUAL, 
        Bytes.fromLong(42));
    
    storage.addColumn(MockBigtable.UID_TABLE, 
        Tsdb1xBigtableUniqueIdStore.MAXID_ROW,
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY,
        Tsdb1xBigtableUniqueIdStore.TAG_VALUE_QUAL, 
        Bytes.fromLong(42));
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
  
  static Tsdb1xBigtableDataStore badClient() {
    BigtableSession session = mock(BigtableSession.class);
    BigtableDataClient client = mock(BigtableDataClient.class);
    AsyncExecutor executor = mock(AsyncExecutor.class);
    Tsdb1xBigtableDataStore data_store = mock(Tsdb1xBigtableDataStore.class);
    
    when(data_store.tableNamer()).thenReturn(table_namer);
    when(session.getDataClient()).thenReturn(client);
    when(data_store.tsdb()).thenReturn(tsdb);
    when(data_store.uidTable()).thenReturn(MockBigtable.UID_TABLE);
    when(data_store.session()).thenReturn(session);
    when(data_store.executor()).thenReturn(executor);
    return data_store;
  }
}
