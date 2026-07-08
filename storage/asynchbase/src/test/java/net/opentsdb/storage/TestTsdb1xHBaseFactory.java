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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import net.opentsdb.core.TSDB;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xDataStore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.stumbleupon.async.Deferred;

public class TestTsdb1xHBaseFactory {

  private TSDB tsdb;
  private Schema schema;
  private MockedConstruction<Tsdb1xHBaseDataStore> mockedDataStore;

  @Before
  public void before() throws Exception {
    tsdb = mock(TSDB.class);
    schema = mock(Schema.class);
    mockedDataStore = Mockito.mockConstruction(Tsdb1xHBaseDataStore.class, (mock, ctx) -> {
      final String id = (String) ctx.arguments().get(1);
      when(mock.id()).thenReturn(id);
      when(mock.shutdown()).thenReturn(Deferred.fromResult(null));
    });
  }

  @After
  public void tearDown() {
    if (mockedDataStore != null) mockedDataStore.close();
  }

  @Test
  public void ctor() throws Exception {
    Tsdb1xHBaseFactory factory = new Tsdb1xHBaseFactory();
    assertNull(factory.tsdb());
    assertNull(factory.default_client);
    assertTrue(factory.clients.isEmpty());
  }

  @Test
  public void initialize() throws Exception {
    Tsdb1xHBaseFactory factory = new Tsdb1xHBaseFactory();
    assertNull(factory.tsdb());
    assertNull(factory.default_client);
    assertTrue(factory.clients.isEmpty());

    factory.initialize(tsdb, null).join();
    assertSame(tsdb, factory.tsdb());
    assertNull(factory.default_client);
    assertTrue(factory.clients.isEmpty());
  }

  @Test
  public void newInstanceDefault() throws Exception {
    Tsdb1xHBaseFactory factory = new Tsdb1xHBaseFactory();
    assertNull(factory.tsdb());
    assertNull(factory.default_client);
    assertTrue(factory.clients.isEmpty());

    Tsdb1xDataStore store = factory.newInstance(tsdb, null, schema);
    assertSame(store, factory.default_client);
    assertTrue(factory.clients.isEmpty());
    assertEquals(1, mockedDataStore.constructed().size());

    store = factory.newInstance(tsdb, null, schema);
    assertSame(store, factory.default_client);
    assertTrue(factory.clients.isEmpty());
    assertEquals(1, mockedDataStore.constructed().size());

    store = factory.newInstance(tsdb, null, schema);
    assertSame(store, factory.default_client);
    assertTrue(factory.clients.isEmpty());
    assertEquals(1, mockedDataStore.constructed().size());
  }

  @Test
  public void newInstanceWithId() throws Exception {
    Tsdb1xHBaseFactory factory = new Tsdb1xHBaseFactory();
    assertNull(factory.tsdb());
    assertNull(factory.default_client);
    assertTrue(factory.clients.isEmpty());

    Tsdb1xDataStore store = factory.newInstance(tsdb, "id1", schema);
    assertNull(factory.default_client);
    assertEquals(1, factory.clients.size());
    assertSame(store, factory.clients.get("id1"));
    assertEquals("id1", store.id());
    assertEquals(1, mockedDataStore.constructed().size());

    store = factory.newInstance(tsdb, "id1", schema);
    assertNull(factory.default_client);
    assertEquals(1, factory.clients.size());
    assertSame(store, factory.clients.get("id1"));
    assertEquals("id1", store.id());
    assertEquals(1, mockedDataStore.constructed().size());

    store = factory.newInstance(tsdb, "id2", schema);
    assertNull(factory.default_client);
    assertEquals(2, factory.clients.size());
    assertSame(store, factory.clients.get("id2"));
    assertEquals("id2", store.id());
    assertEquals(2, mockedDataStore.constructed().size());
  }

  @Test
  public void shutdown() throws Exception {
    Tsdb1xHBaseFactory factory = new Tsdb1xHBaseFactory();

    // empty, no-op
    assertNull(factory.shutdown().join());

    // full
    factory.newInstance(tsdb, null, schema);
    factory.newInstance(tsdb, "id1", schema);
    factory.newInstance(tsdb, "id2", schema);
    assertNull(factory.shutdown().join());
    verify(factory.default_client, times(1)).shutdown();
    verify(factory.clients.get("id1"), times(1)).shutdown();
    verify(factory.clients.get("id1"), times(1)).shutdown();
  }
}
