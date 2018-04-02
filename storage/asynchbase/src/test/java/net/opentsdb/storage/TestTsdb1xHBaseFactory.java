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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xDataStore;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Tsdb1xHBaseFactory.class })
public class TestTsdb1xHBaseFactory {

  private TSDB tsdb;
  private Schema schema;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(TSDB.class);
    schema = mock(Schema.class);
    PowerMockito.whenNew(Tsdb1xHBaseDataStore.class).withAnyArguments()
      .thenAnswer(new Answer<Tsdb1xHBaseDataStore>() {
      @Override
      public Tsdb1xHBaseDataStore answer(InvocationOnMock invocation) throws Throwable {
        final Tsdb1xHBaseDataStore client = mock(Tsdb1xHBaseDataStore.class);
        final String id = (String) invocation.getArguments()[1];
        when(client.id()).thenReturn(id);
        when(client.shutdown()).thenReturn(Deferred.fromResult(null));
        return client;
      }
    });
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
    
    factory.initialize(tsdb).join();
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
    PowerMockito.verifyNew(Tsdb1xHBaseDataStore.class, times(1))
      .withArguments(factory, null, schema);
    
    store = factory.newInstance(tsdb, null, schema);
    assertSame(store, factory.default_client);
    assertTrue(factory.clients.isEmpty());
    PowerMockito.verifyNew(Tsdb1xHBaseDataStore.class, times(1))
      .withArguments(factory, null, schema);
    
    store = factory.newInstance(tsdb, null, schema);
    assertSame(store, factory.default_client);
    assertTrue(factory.clients.isEmpty());
    PowerMockito.verifyNew(Tsdb1xHBaseDataStore.class, times(1))
      .withArguments(factory, null, schema);
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
    PowerMockito.verifyNew(Tsdb1xHBaseDataStore.class, times(1))
      .withArguments(factory, "id1", schema);
    
    store = factory.newInstance(tsdb, "id1", schema);
    assertNull(factory.default_client);
    assertEquals(1, factory.clients.size());
    assertSame(store, factory.clients.get("id1"));
    assertEquals("id1", store.id());
    PowerMockito.verifyNew(Tsdb1xHBaseDataStore.class, times(1))
      .withArguments(factory, "id1", schema);
    
    store = factory.newInstance(tsdb, "id2", schema);
    assertNull(factory.default_client);
    assertEquals(2, factory.clients.size());
    assertSame(store, factory.clients.get("id2"));
    assertEquals("id2", store.id());
    PowerMockito.verifyNew(Tsdb1xHBaseDataStore.class, times(1))
      .withArguments(factory, "id1", schema);
    PowerMockito.verifyNew(Tsdb1xHBaseDataStore.class, times(1))
      .withArguments(factory, "id2", schema);
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
