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
package net.opentsdb.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.BeforeClass;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;

import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.util.ByteStringer;

import net.opentsdb.common.Const;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.MockTrace;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.SchemaBase;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xDataStoreFactory;
import net.opentsdb.uid.LRUUniqueId;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdFactory;
import net.opentsdb.uid.UniqueIdStore;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.UnitTestException;

/**
 * Base class that mocks out the various components and populates the
 * MockBase with some data.
 */
public class UTBase {
  public static final String METRIC_STRING = "sys.cpu.user";
  public static final byte[] METRIC_BYTES = new byte[] { 0, 0, 1 };
  public static final String METRIC_B_STRING = "sys.cpu.system";
  public static final byte[] METRIC_B_BYTES = new byte[] { 0, 0, 2 };
  public static final String NSUN_METRIC = "sys.cpu.nice";
  public static final byte[] NSUI_METRIC = new byte[] { 0, 0, 3 };
  public static final String METRIC_STRING_EX = "sys.cpu.idle";
  public static final byte[] METRIC_BYTES_EX = new byte[] { 0, 0, 7 };
  
  public static final String TAGK_STRING = "host";
  public static final byte[] TAGK_BYTES = new byte[] { 0, 0, 1 };
  public static final String TAGK_B_STRING = "owner";
  public static final byte[] TAGK_B_BYTES = new byte[] { 0, 0, 3 };
  public static final String NSUN_TAGK = "dc";
  public static final byte[] NSUI_TAGK = new byte[] { 0, 0, 4 };
  public static final String TAGK_STRING_EX = "colo";
  public static final byte[] TAGK_BYTES_EX = new byte[] { 0, 0, 8 };
  
  public static final String TAGV_STRING = "web01";
  public static final byte[] TAGV_BYTES = new byte[] { 0, 0, 1 };
  public static final String TAGV_B_STRING = "web02";
  public static final byte[] TAGV_B_BYTES = new byte[] { 0, 0, 2 };
  public static final String NSUN_TAGV = "web03";
  public static final byte[] NSUI_TAGV = new byte[] { 0, 0, 3 };
  public static final String TAGV_STRING_EX = "web04";
  public static final byte[] TAGV_BYTES_EX = new byte[] { 0, 0, 9 };
  
  public static final int TS_SINGLE_SERIES = 1517443200;
  public static final int TS_SINGLE_SERIES_COUNT = 16;
  public static final int TS_SINGLE_SERIES_INTERVAL = 3600;
  
  public static final int TS_DOUBLE_SERIES = 1522540800;
  public static final int TS_DOUBLE_SERIES_COUNT = 16;
  public static final int TS_DOUBLE_SERIES_INTERVAL = 3600;
  
  public static final int TS_MULTI_SERIES_EX = 1525132800;
  public static final int TS_MULTI_SERIES_EX_COUNT = 16;
  public static final int TS_MULTI_SERIES_EX_INDEX = 7;
  public static final int TS_MULTI_SERIES_INTERVAL = 3600;
  
  public static final int TS_NSUI_SERIES = 1527811200;
  public static final int TS_NSUI_SERIES_COUNT = 16;
  public static final int TS_NSUI_SERIES_INTERVAL = 3600;
  
  // GMT: Monday, January 1, 2018 12:15:00 AM
  public static final int START_TS = 1514765700;
  
  // GMT: Monday, January 1, 2018 1:15:00 AM
  public static final int END_TS = 1514769300;
  
  /** The types of series to use as a helper. */
  public static enum Series {
    /** Two metrics but one series each. */
    SINGLE_SERIES,
    
    /** Two metrics and two series each. */
    DOUBLE_SERIES,
    
    /** Two metrics, two series, and an exception is returned at a point. */
    MULTI_SERIES_EX,
    
    /** Two metrics, three series with one incorporating a non-assigned tag value ID. */
    NSUI_SERIES,
  }
  
  protected static MockTSDB tsdb;
  protected static Tsdb1xDataStoreFactory store_factory;
  protected static BigtableSession session;
  protected static BigtableDataClient client;
  protected static AsyncExecutor executor;
  protected static BulkMutation bulk_mutator;
  protected static BigtableInstanceName table_namer;
  protected static MockBigtable storage;
  protected static Tsdb1xBigtableDataStore data_store;
  protected static UniqueIdFactory uid_factory;
  protected static UniqueIdStore uid_store;
  
  protected static Schema schema;
  
  protected static MockTrace trace;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    tsdb = new MockTSDB();
    store_factory = mock(Tsdb1xDataStoreFactory.class);
    session = mock(BigtableSession.class);
    client = mock(BigtableDataClient.class);
    executor = mock(AsyncExecutor.class);
    bulk_mutator = mock(BulkMutation.class);
    uid_factory = mock(UniqueIdFactory.class);
    data_store = mock(Tsdb1xBigtableDataStore.class);
    
    PowerMockito.whenNew(BigtableSession.class).withAnyArguments()
      .thenReturn(session);
    PowerMockito.mockStatic(CredentialOptions.class);
    when(CredentialOptions.jsonCredentials(any(InputStream.class)))
      .thenReturn(mock(CredentialOptions.class));
    PowerMockito.mockStatic(Executors.class);
    when(Executors.newCachedThreadPool())
      .thenReturn(mock(ExecutorService.class));
    when(session.getDataClient()).thenReturn(client);
    PowerMockito.whenNew(FileInputStream.class).withAnyArguments()
      .thenAnswer(new Answer<FileInputStream>() {
        @Override
        public FileInputStream answer(InvocationOnMock invocation)
            throws Throwable {
          return mock(FileInputStream.class);
        }
      });
    
    when(session.createBulkMutation(any(BigtableTableName.class)))
      .thenReturn(bulk_mutator);
    when(session.createAsyncExecutor()).thenReturn(executor);
    
    table_namer = new BigtableInstanceName("UT", "UT");
    when(data_store.tableNamer()).thenReturn(table_namer);
    
    when(client.readFlatRows(any(ReadRowsRequest.class)))
      .thenAnswer(new Answer<ResultScanner>() {
      @Override
      public ResultScanner answer(InvocationOnMock invocation)
          throws Throwable {
        return mock(ResultScanner.class);
      }
    });
    
    when(tsdb.registry.getDefaultPlugin(Tsdb1xDataStoreFactory.class))
      .thenReturn(store_factory);
    when(store_factory.newInstance(any(TSDB.class), any(), any(Schema.class)))
      .thenReturn(data_store);
    when(data_store.tsdb()).thenReturn(tsdb);
    when(data_store.dataTable()).thenReturn(MockBigtable.DATA_TABLE);
    when(data_store.uidTable()).thenReturn(MockBigtable.UID_TABLE);
    when(data_store.session()).thenReturn(session);
    when(data_store.executor()).thenReturn(executor);
    when(tsdb.registry.getSharedObject(any())).thenReturn(data_store);
   
    when(tsdb.registry.getPlugin(UniqueIdFactory.class, "LRU"))
      .thenReturn(uid_factory);
    uid_store = new Tsdb1xBigtableUniqueIdStore(data_store);
    when(tsdb.registry.getSharedObject("default_uidstore"))
      .thenReturn(uid_store);
    when(uid_factory.newInstance(eq(tsdb), anyString(), 
        any(UniqueIdType.class), eq(uid_store))).thenAnswer(new Answer<UniqueId>() {
          @Override
          public UniqueId answer(InvocationOnMock invocation)
              throws Throwable {
            // TODO Auto-generated method stub
            return new LRUUniqueId(tsdb, null, (UniqueIdType) invocation.getArguments()[2], uid_store);
          }
        });
    
    schema = spy(new Schema(tsdb, null));
    when(data_store.schema()).thenReturn(schema);
    
    storage = new MockBigtable(session, executor, client, bulk_mutator);
    loadUIDTable();
    loadRawData();
  }
  
  /**
   * Populates the UID table with the mappings and some exceptions.
   */
  public static void loadUIDTable() {
    bothUIDs(UniqueIdType.METRIC, METRIC_STRING, METRIC_BYTES);
    bothUIDs(UniqueIdType.METRIC, METRIC_B_STRING, METRIC_B_BYTES);
    storage.throwException(METRIC_STRING_EX.getBytes(Const.ISO_8859_CHARSET), 
        new UnitTestException(), true);
    storage.throwException(METRIC_BYTES_EX, new UnitTestException(), true);
    
    bothUIDs(UniqueIdType.TAGK, TAGK_STRING, TAGK_BYTES);
    bothUIDs(UniqueIdType.TAGK, TAGK_B_STRING, TAGK_B_BYTES);
    storage.throwException(TAGK_STRING_EX.getBytes(Const.ISO_8859_CHARSET), 
        new UnitTestException(), true);
    storage.throwException(TAGK_BYTES_EX, new UnitTestException(), true);
    
    bothUIDs(UniqueIdType.TAGV, TAGV_STRING, TAGV_BYTES);
    bothUIDs(UniqueIdType.TAGV, TAGV_B_STRING, TAGV_B_BYTES);
    storage.throwException(TAGV_STRING_EX.getBytes(Const.ISO_8859_CHARSET), 
        new UnitTestException(), true);
    storage.throwException(TAGV_BYTES_EX, new UnitTestException(), true);
    
    for (final Map.Entry<String, byte[]> uid : SchemaBase.UIDS.entrySet()) {
      bothUIDs(UniqueIdType.METRIC, uid.getKey(), uid.getValue());
      bothUIDs(UniqueIdType.TAGK, uid.getKey(), uid.getValue());
      bothUIDs(UniqueIdType.TAGV, uid.getKey(), uid.getValue());
    }
  }
  
  /**
   * Mocks out both UIDs, writing them to storage.
   * @param type The type.
   * @param name The name.
   * @param inal The id.
   */
  static void bothUIDs(final UniqueIdType type, 
                       final String name, 
                       final byte[] id) {
    byte[] qualifier = null;
    switch (type) {
    case METRIC:
      qualifier = Tsdb1xBigtableUniqueIdStore.METRICS_QUAL;
      break;
    case TAGK:
      qualifier = Tsdb1xBigtableUniqueIdStore.TAG_NAME_QUAL;
      break;
    case TAGV:
      qualifier = Tsdb1xBigtableUniqueIdStore.TAG_VALUE_QUAL;
      break;
    default:
      throw new IllegalArgumentException("Hmm, " + type 
          + " isn't supported here.");
    }
    storage.addColumn(MockBigtable.UID_TABLE, 
        name.getBytes(Const.ISO_8859_CHARSET), 
        Tsdb1xBigtableUniqueIdStore.ID_FAMILY,
        qualifier, 
        id);
    storage.addColumn(MockBigtable.UID_TABLE, 
        id, 
        Tsdb1xBigtableUniqueIdStore.NAME_FAMILY,
        qualifier, 
        name.getBytes(Const.ISO_8859_CHARSET));
  }
  
  /**
   * Utility to generate a row key for scanners or storage.
   * @param metric A non-null metric UID.
   * @param timestamp A timestamp.
   * @param tags An optional list of key/value pairs.
   * @return The row key.
   */
  public static byte[] makeRowKey(byte[] metric, int timestamp, byte[]... tags) {
    int size = metric.length + 4;
    if (tags != null) {
      for (byte[] tag : tags) {
        size += tag.length;
      }
    }
    byte[] key = new byte[size];
    System.arraycopy(metric, 0, key, 0, metric.length);
    System.arraycopy(Bytes.fromInt(timestamp), 0, key, metric.length, 4);
    
    int offset = metric.length + 4;
    if (tags != null) {
      for (byte[] tag : tags) {
        System.arraycopy(tag, 0, key, offset, tag.length);
        offset += tag.length;
      }
    }
    return key;
  }
  
  /**
   * Populates MockBase with some data.
   * @throws Exception
   */
  public static void loadRawData() throws Exception {
    for (int i = 0; i < TS_SINGLE_SERIES_COUNT; i++) {
      storage.addColumn(MockBigtable.DATA_TABLE, makeRowKey(
          METRIC_BYTES, 
          TS_SINGLE_SERIES + (i * TS_SINGLE_SERIES_INTERVAL), 
          TAGK_BYTES,
          TAGV_BYTES), 
        Tsdb1xBigtableDataStore.DATA_FAMILY, 
        new byte[2], 
        new byte[] { 1 });
      
      storage.addColumn(MockBigtable.DATA_TABLE, makeRowKey(
          METRIC_B_BYTES, 
          TS_SINGLE_SERIES + (i * TS_SINGLE_SERIES_INTERVAL), 
          TAGK_BYTES,
          TAGV_BYTES), 
        Tsdb1xBigtableDataStore.DATA_FAMILY, 
        new byte[2], 
        new byte[] { 1 });
    }
    
    for (int i = 0; i < TS_DOUBLE_SERIES_COUNT; i++) {
      storage.addColumn(MockBigtable.DATA_TABLE, makeRowKey(
          METRIC_BYTES, 
          TS_DOUBLE_SERIES + (i * TS_DOUBLE_SERIES_INTERVAL), 
          TAGK_BYTES,
          TAGV_BYTES), 
        Tsdb1xBigtableDataStore.DATA_FAMILY, 
        new byte[2], 
        new byte[] { 1 });
      
      storage.addColumn(MockBigtable.DATA_TABLE, makeRowKey(
          METRIC_BYTES, 
          TS_DOUBLE_SERIES + (i * TS_DOUBLE_SERIES_INTERVAL), 
          TAGK_BYTES,
          TAGV_B_BYTES), 
        Tsdb1xBigtableDataStore.DATA_FAMILY, 
        new byte[2], 
        new byte[] { 1 });
      
      storage.addColumn(MockBigtable.DATA_TABLE, makeRowKey(
          METRIC_B_BYTES, 
          TS_DOUBLE_SERIES + (i * TS_DOUBLE_SERIES_INTERVAL), 
          TAGK_BYTES,
          TAGV_BYTES), 
        Tsdb1xBigtableDataStore.DATA_FAMILY, 
        new byte[2], 
        new byte[] { 1 });
      
      storage.addColumn(MockBigtable.DATA_TABLE, makeRowKey(
          METRIC_B_BYTES, 
          TS_DOUBLE_SERIES + (i * TS_DOUBLE_SERIES_INTERVAL), 
          TAGK_BYTES,
          TAGV_B_BYTES), 
        Tsdb1xBigtableDataStore.DATA_FAMILY, 
        new byte[2], 
        new byte[] { 1 });
    }
    
    for (int i = 0; i < TS_MULTI_SERIES_EX_COUNT; i++) {
      storage.addColumn(MockBigtable.DATA_TABLE, makeRowKey(
          METRIC_BYTES, 
          TS_MULTI_SERIES_EX + (i * TS_MULTI_SERIES_INTERVAL), 
          TAGK_BYTES,
          TAGV_BYTES), 
        Tsdb1xBigtableDataStore.DATA_FAMILY, 
        new byte[2], 
        new byte[] { 1 });
      
      storage.addColumn(MockBigtable.DATA_TABLE, makeRowKey(
          METRIC_BYTES, 
          TS_MULTI_SERIES_EX + (i * TS_MULTI_SERIES_INTERVAL), 
          TAGK_BYTES,
          TAGV_B_BYTES), 
        Tsdb1xBigtableDataStore.DATA_FAMILY, 
        new byte[2], 
        new byte[] { 1 });
      
      storage.addColumn(MockBigtable.DATA_TABLE, makeRowKey(
          METRIC_B_BYTES, 
          TS_MULTI_SERIES_EX + (i * TS_MULTI_SERIES_INTERVAL), 
          TAGK_BYTES,
          TAGV_BYTES), 
        Tsdb1xBigtableDataStore.DATA_FAMILY, 
        new byte[2], 
        new byte[] { 1 });
      
      storage.addColumn(MockBigtable.DATA_TABLE, makeRowKey(
          METRIC_B_BYTES, 
          TS_MULTI_SERIES_EX + (i * TS_MULTI_SERIES_INTERVAL), 
          TAGK_BYTES,
          TAGV_B_BYTES), 
        Tsdb1xBigtableDataStore.DATA_FAMILY, 
        new byte[2], 
        new byte[] { 1 });
      
      if (i == TS_MULTI_SERIES_EX_INDEX) {
        storage.throwException(makeRowKey(
            METRIC_BYTES, 
            TS_MULTI_SERIES_EX + (i * TS_MULTI_SERIES_INTERVAL), 
            TAGK_BYTES,
            TAGV_BYTES),
            new UnitTestException(), true);
        
        storage.throwException(makeRowKey(
            METRIC_B_BYTES, 
            TS_MULTI_SERIES_EX + (i * TS_MULTI_SERIES_INTERVAL), 
            TAGK_BYTES,
            TAGV_BYTES),
            new UnitTestException(), true);
      }
    }
    
    for (int i = 0; i < TS_NSUI_SERIES_COUNT; i++) {
      storage.addColumn(MockBigtable.DATA_TABLE, makeRowKey(
          METRIC_BYTES, 
          TS_NSUI_SERIES + (i * TS_NSUI_SERIES_INTERVAL), 
          TAGK_BYTES,
          TAGV_BYTES), 
        Tsdb1xBigtableDataStore.DATA_FAMILY, 
        new byte[2], 
        new byte[] { 1 });
      
      // offset a bit
      if (i > 0) {
        storage.addColumn(MockBigtable.DATA_TABLE, makeRowKey(
            METRIC_BYTES, 
            TS_NSUI_SERIES + (i * TS_NSUI_SERIES_INTERVAL), 
            TAGK_BYTES,
            NSUI_TAGV), 
          Tsdb1xBigtableDataStore.DATA_FAMILY, 
          new byte[2], 
          new byte[] { 1 });
      }
      
      storage.addColumn(MockBigtable.DATA_TABLE, makeRowKey(
          METRIC_B_BYTES, 
          TS_NSUI_SERIES + (i * TS_NSUI_SERIES_INTERVAL), 
          TAGK_BYTES,
          TAGV_BYTES), 
        Tsdb1xBigtableDataStore.DATA_FAMILY, 
        new byte[2], 
        new byte[] { 1 });
      
      if (i > 0) {
        storage.addColumn(MockBigtable.DATA_TABLE, makeRowKey(
            METRIC_B_BYTES, 
            TS_NSUI_SERIES + (i * TS_NSUI_SERIES_INTERVAL), 
            TAGK_BYTES,
            NSUI_TAGV), 
          Tsdb1xBigtableDataStore.DATA_FAMILY, 
          new byte[2], 
          new byte[] { 1 });
      }
    }
  }
  
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
    assertTrue(ex.isInstance(trace.spans.get(size - 1).exceptions.get("Exception")));
  }

  static FlatRow buildFlatRow(final byte[] key, final byte[] cf, final byte[] qualifier, final byte[] value) {
    return FlatRow.newBuilder()
        .withRowKey(ByteStringer.wrap(key))
        .addCell(new String(cf), ByteStringer.wrap(qualifier), 1L, ByteStringer.wrap(value))
        .build();
  }
  
  static void addCell(final FlatRow.Builder builder, final byte[] cf, 
      final byte[] qualifier, final byte[] value) {
    builder.addCell(new String(cf), ByteStringer.wrap(qualifier), 1L, 
        ByteStringer.wrap(value));
  }
  
  static Row buildRow(final byte[] key, final byte[] cf, final byte[] qualifier, final byte[] value) {
    return Row.newBuilder()
        .setKey(ByteStringer.wrap(key))
        .addFamilies(Family.newBuilder()
            .setNameBytes(ByteStringer.wrap(cf))
            .addColumns(Column.newBuilder()
                .setQualifier(ByteStringer.wrap(qualifier))
                .addCells(com.google.bigtable.v2.Cell.newBuilder()
                    .setTimestampMicros(1L)
                    .setValue(ByteStringer.wrap(value)))))
        .build();
  }
}
