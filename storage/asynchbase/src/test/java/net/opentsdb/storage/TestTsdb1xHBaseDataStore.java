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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.UnitTestConfiguration;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xDataStoreFactory;
import net.opentsdb.uid.UniqueIdStore;

public class TestTsdb1xHBaseDataStore {

  private Tsdb1xHBaseFactory factory;
  private DefaultTSDB tsdb;
  private Configuration config;
  private DefaultRegistry registry;
  
  @Before
  public void before() throws Exception {
    factory = mock(Tsdb1xHBaseFactory.class);
    tsdb = mock(DefaultTSDB.class);
    config = UnitTestConfiguration.getConfiguration();
    registry = mock(DefaultRegistry.class);
    when(tsdb.getConfig()).thenReturn(config);
    when(tsdb.getRegistry()).thenReturn(registry);
    when(factory.tsdb()).thenReturn(tsdb);
  }
  
  @Test
  public void ctorDefault() throws Exception {
    final Tsdb1xHBaseDataStore store = 
        new Tsdb1xHBaseDataStore(factory, "UT", mock(Schema.class));
    assertArrayEquals("tsdb".getBytes(Const.ISO_8859_CHARSET), store.dataTable());
    assertArrayEquals("tsdb-uid".getBytes(Const.ISO_8859_CHARSET), store.uidTable());
    assertSame(tsdb, store.tsdb());
    assertNotNull(store.uidStore());
    verify(registry, times(1)).registerSharedObject(eq("UT_uidstore"), 
        any(UniqueIdStore.class));
  }
  
}
