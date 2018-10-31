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
package net.opentsdb.query.idconverter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryPipelineContext;

public class TestByteToStringIdConverterFactory {

  @Test
  public void ctor() throws Exception {
    ByteToStringIdConverterFactory factory = new ByteToStringIdConverterFactory();
    assertEquals(0, factory.types().size());
    assertEquals(ByteToStringIdConverterFactory.TYPE, factory.type());
  }
  
  @Test
  public void newNode() throws Exception {
    ByteToStringIdConverterFactory factory = new ByteToStringIdConverterFactory();
    ByteToStringIdConverter node = (ByteToStringIdConverter) 
        factory.newNode(mock(QueryPipelineContext.class));
    assertEquals(ByteToStringIdConverterFactory.TYPE, node.config().getId());
    
    node = (ByteToStringIdConverter) factory.newNode(
        mock(QueryPipelineContext.class),
        ByteToStringIdConverterConfig.newBuilder()
          .setId("cvtr")
          .build());
    assertEquals("cvtr", node.config().getId());
  }
  
  @Test
  public void initialize() throws Exception {
    ByteToStringIdConverterFactory factory = new ByteToStringIdConverterFactory();
    assertNull(factory.initialize(mock(TSDB.class), null).join(250));
    assertEquals(ByteToStringIdConverterFactory.TYPE, factory.id());
    
    factory = new ByteToStringIdConverterFactory();
    assertNull(factory.initialize(mock(TSDB.class), "cvtr").join(250));
    assertEquals("cvtr", factory.id());
  }
}
