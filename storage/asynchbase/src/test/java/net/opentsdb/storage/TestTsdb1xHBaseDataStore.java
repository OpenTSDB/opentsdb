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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import net.opentsdb.data.MockLowLevelMetricData;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.UnitTestException;
import org.hbase.async.HBaseClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.UnitTestConfiguration;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.data.BaseTimeSeriesDatumStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesDatumStringId;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.storage.WriteStatus.WriteState;
import net.opentsdb.storage.schemas.tsdb1x.NumericCodec;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xDataStoreFactory;
import net.opentsdb.uid.UniqueIdStore;

import java.util.List;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Tsdb1xHBaseDataStore.class, HBaseClient.class })
public class TestTsdb1xHBaseDataStore extends UTBase {

  private Tsdb1xHBaseFactory factory;
//  private DefaultTSDB tsdb;
//  private Configuration config;
//  private DefaultRegistry registry;
  
  @Before
  public void before() throws Exception {
    factory = mock(Tsdb1xHBaseFactory.class);
//    tsdb = mock(DefaultTSDB.class);
//    config = UnitTestConfiguration.getConfiguration();
//    registry = mock(DefaultRegistry.class);
//    when(tsdb.getConfig()).thenReturn(config);
//    when(tsdb.getRegistry()).thenReturn(registry);
    when(factory.tsdb()).thenReturn(tsdb);
    PowerMockito.whenNew(HBaseClient.class).withAnyArguments().thenReturn(client);
    storage.flushStorage("tsdb".getBytes(Const.ASCII_US_CHARSET));
  }
  
  @Test
  public void ctorDefault() throws Exception {
    final Tsdb1xHBaseDataStore store = 
        new Tsdb1xHBaseDataStore(factory, "UT", schema);
    assertArrayEquals("tsdb".getBytes(Const.ISO_8859_CHARSET), store.dataTable());
    assertArrayEquals("tsdb-uid".getBytes(Const.ISO_8859_CHARSET), store.uidTable());
    assertSame(tsdb, store.tsdb());
    assertNotNull(store.uidStore());
    verify(tsdb.registry, atLeastOnce()).registerSharedObject(eq("UT_uidstore"), 
        any(UniqueIdStore.class));
  }
  
  @Test
  public void writeDatum() throws Exception {
    MutableNumericValue value = 
        new MutableNumericValue(new SecondTimeStamp(1262304000), 42);
    TimeSeriesDatumStringId id = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric(METRIC_STRING)
        .addTags(TAGK_STRING, TAGV_STRING)
        .build();
    
    Tsdb1xHBaseDataStore store = 
        new Tsdb1xHBaseDataStore(factory, "UT", schema);
    Whitebox.setInternalState(store, "use_dp_timestamp", false);
    WriteStatus state = store.write(null, TimeSeriesDatum.wrap(id, value), null).join();

    assertEquals(WriteState.OK, state.state());
    byte[] row_key = new byte[] { 0, 0, 1, 75, 61, 59, 0, 0, 0, 1, 0, 0, 1 };
    assertArrayEquals(new byte[] { 42 }, storage.getColumn(
        store.dataTable(), row_key, Tsdb1xHBaseDataStore.DATA_FAMILY, 
        new byte[] { 0, 0 }));

    // appends
    Whitebox.setInternalState(store, "write_appends", true);
    state = store.write(null, TimeSeriesDatum.wrap(id, value), null).join();
    assertEquals(WriteState.OK, state.state());
    assertArrayEquals(new byte[] { 0, 0, 42 }, storage.getColumn(
        store.dataTable(), row_key, Tsdb1xHBaseDataStore.DATA_FAMILY, 
        NumericCodec.APPEND_QUALIFIER));
    
    Whitebox.setInternalState(store, "write_appends", false);
    Whitebox.setInternalState(store, "encode_as_appends", true);
    value.resetValue(1);
    state = store.write(null, TimeSeriesDatum.wrap(id, value), null).join();
    assertEquals(WriteState.OK, state.state());
    // overwrites
    assertArrayEquals(new byte[] { 0, 0, 1 }, storage.getColumn(
        store.dataTable(), row_key, Tsdb1xHBaseDataStore.DATA_FAMILY, 
        NumericCodec.APPEND_QUALIFIER));
    
    // bad metric
    id = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric(METRIC_STRING_EX)
        .addTags(TAGK_STRING, TAGV_STRING)
        .build();
    state = store.write(null, TimeSeriesDatum.wrap(id, value), null).join();
    assertEquals(WriteState.ERROR, state.state());
  }

  @Test
  public void writeSharedData() throws Exception {
    TimeStamp ts = new SecondTimeStamp(1262304000);
    Map<String, String> tags = ImmutableMap.<String, String>builder()
            .put(TAGK_STRING, TAGV_STRING)
            .build();

    MutableNumericValue dp = new MutableNumericValue(ts, 42);
    TimeSeriesDatumStringId id = BaseTimeSeriesDatumStringId.newBuilder()
            .setMetric(METRIC_STRING)
            .addTags(TAGK_STRING, TAGV_STRING)
            .build();

    List<TimeSeriesDatum> data = Lists.newArrayList();
    data.add(TimeSeriesDatum.wrap(id, dp));

    dp = new MutableNumericValue(ts, 24);
    id = BaseTimeSeriesDatumStringId.newBuilder()
            .setMetric(METRIC_B_STRING)
            .addTags(TAGK_STRING, TAGV_STRING)
            .build();
    data.add(TimeSeriesDatum.wrap(id, dp));

    TimeSeriesSharedTagsAndTimeData shared =
            TimeSeriesSharedTagsAndTimeData.fromCollection(data);
    Tsdb1xHBaseDataStore store =
            new Tsdb1xHBaseDataStore(factory, "UT", schema);
    Whitebox.setInternalState(store, "use_dp_timestamp", false);
    List<WriteStatus> statuses = store.write(null, shared, null).join();

    assertEquals(2, statuses.size());
    assertEquals(WriteState.OK, statuses.get(0).state());
    assertEquals(WriteState.OK, statuses.get(1).state());

    byte[] row_key = new byte[] { 0, 0, 1, 75, 61, 59, 0, 0, 0, 1, 0, 0, 1 };
    assertArrayEquals(new byte[] { 42 }, storage.getColumn(
            store.dataTable(), row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
            new byte[] { 0, 0 }));

    row_key = new byte[] { 0, 0, 2, 75, 61, 59, 0, 0, 0, 1, 0, 0, 1 };
    assertArrayEquals(new byte[] { 24 }, storage.getColumn(
            store.dataTable(), row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
            new byte[] { 0, 0 }));

    // appends
    Whitebox.setInternalState(store, "write_appends", true);
    statuses = store.write(null, shared, null).join();
    assertEquals(2, statuses.size());
    assertEquals(WriteState.OK, statuses.get(0).state());
    assertEquals(WriteState.OK, statuses.get(1).state());

    row_key = new byte[] { 0, 0, 1, 75, 61, 59, 0, 0, 0, 1, 0, 0, 1 };
    assertArrayEquals(new byte[] { 0, 0, 42 }, storage.getColumn(
            store.dataTable(), row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
            NumericCodec.APPEND_QUALIFIER));

    row_key = new byte[] { 0, 0, 2, 75, 61, 59, 0, 0, 0, 1, 0, 0, 1 };
    assertArrayEquals(new byte[] { 0, 0, 24 }, storage.getColumn(
            store.dataTable(), row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
            NumericCodec.APPEND_QUALIFIER));

    Whitebox.setInternalState(store, "write_appends", false);
    Whitebox.setInternalState(store, "encode_as_appends", true);
    ((MutableNumericValue) data.get(0).value()).resetValue(1);
    ((MutableNumericValue) data.get(1).value()).resetValue(2);

    statuses = store.write(null, shared, null).join();
    assertEquals(2, statuses.size());
    assertEquals(WriteState.OK, statuses.get(0).state());
    assertEquals(WriteState.OK, statuses.get(1).state());

    row_key = new byte[] { 0, 0, 1, 75, 61, 59, 0, 0, 0, 1, 0, 0, 1 };
    assertArrayEquals(new byte[] { 0, 0, 1 }, storage.getColumn(
            store.dataTable(), row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
            NumericCodec.APPEND_QUALIFIER));

    row_key = new byte[] { 0, 0, 2, 75, 61, 59, 0, 0, 0, 1, 0, 0, 1 };
    assertArrayEquals(new byte[] { 0, 0, 2 }, storage.getColumn(
            store.dataTable(), row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
            NumericCodec.APPEND_QUALIFIER));

    // one error
    id = BaseTimeSeriesDatumStringId.newBuilder()
            .setMetric(METRIC_STRING_EX)
            .addTags(TAGK_STRING, TAGV_STRING)
            .build();
    dp = new MutableNumericValue(ts, 8);
    data.set(0, TimeSeriesDatum.wrap(id, dp));
    shared =
            TimeSeriesSharedTagsAndTimeData.fromCollection(data);

    statuses = store.write(null, shared, null).join();
    assertEquals(2, statuses.size());
    assertEquals(WriteState.ERROR, statuses.get(0).state());
    assertTrue(statuses.get(0).exception() instanceof StorageException);
    assertEquals(WriteState.OK, statuses.get(1).state());
  }

  @Test
  public void writeLowLevel() throws Exception {
    TimeStamp ts = new SecondTimeStamp(1262304000);
    Map<String, String> tags = ImmutableMap.<String, String>builder()
            .put(TAGK_STRING, TAGV_STRING)
            .build();

    MutableNumericValue dp = new MutableNumericValue(ts, 42);
    TimeSeriesDatumStringId id = BaseTimeSeriesDatumStringId.newBuilder()
            .setMetric(METRIC_STRING)
            .addTags(TAGK_STRING, TAGV_STRING)
            .build();
    TimeSeriesDatum datum_1 = TimeSeriesDatum.wrap(id, dp);

    dp = new MutableNumericValue(ts, 24);
    id = BaseTimeSeriesDatumStringId.newBuilder()
            .setMetric(METRIC_B_STRING)
            .addTags(TAGK_STRING, TAGV_STRING)
            .build();
    TimeSeriesDatum datum_2 = TimeSeriesDatum.wrap(id, dp);

    MockLowLevelMetricData data = lowLevel(datum_1, datum_2);
    Tsdb1xHBaseDataStore store =
            new Tsdb1xHBaseDataStore(factory, "UT", schema);
    Whitebox.setInternalState(store, "use_dp_timestamp", false);
    List<WriteStatus> statuses = store.write(null, data, null).join();

    assertEquals(2, statuses.size());
    assertEquals(WriteState.OK, statuses.get(0).state());
    assertEquals(WriteState.OK, statuses.get(1).state());

    byte[] row_key = new byte[] { 0, 0, 1, 75, 61, 59, 0, 0, 0, 1, 0, 0, 1 };
    assertArrayEquals(new byte[] { 42 }, storage.getColumn(
            store.dataTable(), row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
            new byte[] { 0, 0 }));

    row_key = new byte[] { 0, 0, 2, 75, 61, 59, 0, 0, 0, 1, 0, 0, 1 };
    assertArrayEquals(new byte[] { 24 }, storage.getColumn(
            store.dataTable(), row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
            new byte[] { 0, 0 }));

    // appends
    data = lowLevel(datum_1, datum_2);
    Whitebox.setInternalState(store, "write_appends", true);
    statuses = store.write(null, data, null).join();
    assertEquals(2, statuses.size());
    assertEquals(WriteState.OK, statuses.get(0).state());
    assertEquals(WriteState.OK, statuses.get(1).state());

    row_key = new byte[] { 0, 0, 1, 75, 61, 59, 0, 0, 0, 1, 0, 0, 1 };
    assertArrayEquals(new byte[] { 0, 0, 42 }, storage.getColumn(
            store.dataTable(), row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
            NumericCodec.APPEND_QUALIFIER));

    row_key = new byte[] { 0, 0, 2, 75, 61, 59, 0, 0, 0, 1, 0, 0, 1 };
    assertArrayEquals(new byte[] { 0, 0, 24 }, storage.getColumn(
            store.dataTable(), row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
            NumericCodec.APPEND_QUALIFIER));

    data = lowLevel(datum_1, datum_2);
    Whitebox.setInternalState(store, "write_appends", false);
    Whitebox.setInternalState(store, "encode_as_appends", true);
    ((MutableNumericValue) datum_1.value()).resetValue(1);
    ((MutableNumericValue) datum_2.value()).resetValue(2);

    statuses = store.write(null, data, null).join();
    assertEquals(2, statuses.size());
    assertEquals(WriteState.OK, statuses.get(0).state());
    assertEquals(WriteState.OK, statuses.get(1).state());

    row_key = new byte[] { 0, 0, 1, 75, 61, 59, 0, 0, 0, 1, 0, 0, 1 };
    assertArrayEquals(new byte[] { 0, 0, 1 }, storage.getColumn(
            store.dataTable(), row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
            NumericCodec.APPEND_QUALIFIER));

    row_key = new byte[] { 0, 0, 2, 75, 61, 59, 0, 0, 0, 1, 0, 0, 1 };
    assertArrayEquals(new byte[] { 0, 0, 2 }, storage.getColumn(
            store.dataTable(), row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
            NumericCodec.APPEND_QUALIFIER));

    // one error
    id = BaseTimeSeriesDatumStringId.newBuilder()
            .setMetric(METRIC_STRING_EX)
            .addTags(TAGK_STRING, TAGV_STRING)
            .build();
    dp = new MutableNumericValue(ts, 8);
    datum_1 = TimeSeriesDatum.wrap(id, dp);
    data = lowLevel(datum_1, datum_2);

    statuses = store.write(null, data, null).join();
    assertEquals(2, statuses.size());
    assertEquals(WriteState.ERROR, statuses.get(0).state());
    assertTrue(statuses.get(0).exception() instanceof StorageException);
    assertEquals(WriteState.OK, statuses.get(1).state());
  }

  @Test
  public void writeWithDPTimestamp() throws Exception {
    MutableNumericValue value =
            new MutableNumericValue(new SecondTimeStamp(1262304000), 42);
    TimeSeriesDatumStringId id = BaseTimeSeriesDatumStringId.newBuilder()
            .setMetric(METRIC_STRING)
            .addTags(TAGK_STRING, TAGV_STRING)
            .build();

    Tsdb1xHBaseDataStore store =
            new Tsdb1xHBaseDataStore(factory, "UT", schema);
    WriteStatus state = store.write(null, TimeSeriesDatum.wrap(id, value), null).join();
    assertEquals(WriteState.OK, state.state());
    byte[] row_key = new byte[] { 0, 0, 1, 75, 61, 59, 0, 0, 0, 1, 0, 0, 1 };
    assertArrayEquals(new byte[] { 42 }, storage.getColumn(
            store.dataTable(), row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
            new byte[] { 0, 0 },
            1262304000_000L));

    // now without the timestamp
    value.resetValue(24);
    Whitebox.setInternalState(store, "use_dp_timestamp", false);
    state = store.write(null, TimeSeriesDatum.wrap(id, value), null).join();
    assertEquals(WriteState.OK, state.state());
    row_key = new byte[] { 0, 0, 1, 75, 61, 59, 0, 0, 0, 1, 0, 0, 1 };
    assertArrayEquals(new byte[] { 24 }, storage.getColumn(
            store.dataTable(), row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
            new byte[] { 0, 0 },
            storage.getCurrentTimestamp() - 1));
  }

  MockLowLevelMetricData lowLevel(TimeSeriesDatum... data) {
    MockLowLevelMetricData low_level = new MockLowLevelMetricData();
    for (int i = 0; i < data.length; i++) {
      low_level.add(data[i]);
    }
    return low_level;
  }
}
