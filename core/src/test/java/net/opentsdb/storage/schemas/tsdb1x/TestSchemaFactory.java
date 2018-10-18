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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.List;

import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.stats.Span;
import net.opentsdb.uid.UniqueIdType;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SchemaFactory.class })
public class TestSchemaFactory extends SchemaBase {

  private TSDB tsdb;
  private Tsdb1xDataStore store;
  private QueryNode node;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(TSDB.class);
    store = mock(Tsdb1xDataStore.class);
    node = mock(QueryNode.class);
    
    when(store.newNode(any(QueryPipelineContext.class), 
        any(QueryNodeConfig.class)))
      .thenReturn(node);
    
    PowerMockito.whenNew(Schema.class).withAnyArguments()
      .thenAnswer(new Answer<Schema>() {
      @Override
      public Schema answer(InvocationOnMock invocation) throws Throwable {
        final Schema schema = mock(Schema.class);
        when(schema.dataStore()).thenReturn(store);
        return schema;
      }
    });
  }
  
  @Test
  public void ctor() throws Exception {
    SchemaFactory factory = new SchemaFactory();
    assertNull(factory.id());
    assertEquals(SchemaFactory.TYPE, factory.type());
    PowerMockito.verifyNew(Schema.class, never());
    
    assertNull(factory.initialize(tsdb, null).join(1));
    PowerMockito.verifyNew(Schema.class);
  }
  
  @Test
  public void newNode() throws Exception {
    SchemaFactory factory = new SchemaFactory();
    factory.initialize(tsdb, null).join(1);
    
    assertSame(node, factory.newNode(mock(QueryPipelineContext.class), 
        mock(QueryNodeConfig.class)));
  }
  
  @Test
  public void resolveByteId() throws Exception {
    SchemaFactory factory = new SchemaFactory();
    factory.initialize(tsdb, null).join(1);
    
    factory.resolveByteId(mock(TimeSeriesByteId.class), null);
    verify(factory.schema, times(1)).resolveByteId(
        any(TimeSeriesByteId.class), any(Span.class));
  }
  
  @Test
  public void encodeJoinKeys() throws Exception {
    SchemaFactory factory = new SchemaFactory();
    factory.initialize(tsdb, null).join(1);
    
    factory.encodeJoinKeys(Lists.newArrayList(), null);
    verify(factory.schema, times(1)).getIds(
        eq(UniqueIdType.TAGK), any(List.class), any(Span.class));
  }
  
  @Test
  public void encodeJoinMetrics() throws Exception {
    SchemaFactory factory = new SchemaFactory();
    factory.initialize(tsdb, null).join(1);
    
    factory.encodeJoinMetrics(Lists.newArrayList(), null);
    verify(factory.schema, times(1)).getIds(
        eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
  }
}
