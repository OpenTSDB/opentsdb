//This file is part of OpenTSDB.
//Copyright (C) 2018  The OpenTSDB Authors.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
package net.opentsdb.query.processor.summarizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;

public class TestSummarizerFactory {

  @Test
  public void ctor() throws Exception {
    SummarizerFactory factory = new SummarizerFactory();
    assertEquals(3, factory.types().size());
    assertTrue(factory.types().contains(NumericArrayType.TYPE));
    assertTrue(factory.types().contains(NumericType.TYPE));
    assertTrue(factory.types().contains(NumericSummaryType.TYPE));
    assertEquals(SummarizerFactory.ID, factory.id());
  }
  
  @Test
  public void newIterator() throws Exception {
    SummarizerConfig config = (SummarizerConfig) 
        SummarizerConfig.newBuilder()
        .setSummaries(Lists.newArrayList("sum", "avg"))
        .setInfectiousNan(true)
        .setId("summarizer")
        .build();
    
    final QueryNode node = mock(QueryNode.class);
    when(node.config()).thenReturn(config);
    final QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(node.pipelineContext()).thenReturn(context);
    
    SummarizerFactory factory = new SummarizerFactory();
    
    QueryNode summarizer = factory.newNode(context, "", config);
    assertTrue(summarizer instanceof Summarizer);
  }
}
