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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.core.TSDB;
import net.opentsdb.storage.ReadableTimeSeriesDataStore;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SchemaFactory.class })
public class TestSchemaFactory extends SchemaBase {

  private TSDB tsdb;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(TSDB.class);
    PowerMockito.whenNew(Schema.class).withAnyArguments()
      .thenAnswer(new Answer<Schema>() {
      @Override
      public Schema answer(InvocationOnMock invocation) throws Throwable {
        final Schema schema = mock(Schema.class);
        final String id = (String) invocation.getArguments()[1];
        when(schema.id()).thenReturn(id);
        return schema;
      }
    });
  }
  
  @Test
  public void newInstanceDefault() throws Exception {
    SchemaFactory factory = new SchemaFactory();
    assertNull(factory.default_schema);
    assertTrue(factory.schemas.isEmpty());
    
    ReadableTimeSeriesDataStore store = factory.newInstance(tsdb, null);
    assertSame(store, factory.default_schema);
    assertTrue(factory.schemas.isEmpty());
    assertNull(store.id());
    PowerMockito.verifyNew(Schema.class, times(1)).withArguments(tsdb, (String) null);
    
    store = factory.newInstance(tsdb, null);
    assertSame(store, factory.default_schema);
    assertTrue(factory.schemas.isEmpty());
    assertNull(store.id());
    PowerMockito.verifyNew(Schema.class, times(1)).withArguments(tsdb, (String) null);
    
    store = factory.newInstance(tsdb, null);
    assertSame(store, factory.default_schema);
    assertTrue(factory.schemas.isEmpty());
    assertNull(store.id());
    PowerMockito.verifyNew(Schema.class, times(1)).withArguments(tsdb, (String) null);
  }
  
  @Test
  public void newInstanceWithID() throws Exception {
    SchemaFactory factory = new SchemaFactory();
    assertNull(factory.default_schema);
    assertTrue(factory.schemas.isEmpty());
    
    ReadableTimeSeriesDataStore store = factory.newInstance(tsdb, "id1");
    assertNull(factory.default_schema);
    assertEquals(1, factory.schemas.size());
    assertSame(store, factory.schemas.get("id1"));
    assertEquals("id1", store.id());
    PowerMockito.verifyNew(Schema.class, times(1)).withArguments(tsdb, "id1");
    
    store = factory.newInstance(tsdb, "id1");
    assertNull(factory.default_schema);
    assertEquals(1, factory.schemas.size());
    assertSame(store, factory.schemas.get("id1"));
    assertEquals("id1", store.id());
    PowerMockito.verifyNew(Schema.class, times(1)).withArguments(tsdb, "id1");
    
    store = factory.newInstance(tsdb, "id2");
    assertNull(factory.default_schema);
    assertEquals(2, factory.schemas.size());
    assertSame(store, factory.schemas.get("id2"));
    assertEquals("id2", store.id());
    PowerMockito.verifyNew(Schema.class, times(1)).withArguments(tsdb, "id1");
    PowerMockito.verifyNew(Schema.class, times(1)).withArguments(tsdb, "id2");
  }
}
