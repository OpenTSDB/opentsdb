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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
  public void write() throws Exception {
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
        new byte[] { 0, 0 }));
    storage.dumpToSystemOut(store.dataTable(), false);
    
    // appends
    Whitebox.setInternalState(store, "enable_appends", true);
    state = store.write(null, TimeSeriesDatum.wrap(id, value), null).join();
    assertEquals(WriteState.OK, state.state());
    assertArrayEquals(new byte[] { 0, 0, 42 }, storage.getColumn(
        store.dataTable(), row_key, Tsdb1xHBaseDataStore.DATA_FAMILY, 
        NumericCodec.APPEND_QUALIFIER));
    
    Whitebox.setInternalState(store, "enable_appends", false);
    Whitebox.setInternalState(store, "enable_appends_coproc", true);
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
}
