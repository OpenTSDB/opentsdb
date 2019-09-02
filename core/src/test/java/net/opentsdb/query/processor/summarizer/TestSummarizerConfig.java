// This file is part of OpenTSDB.
// Copyright (C) 2018-2019  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.summarizer;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.JSON;

public class TestSummarizerConfig {

  @Test
  public void builder() throws Exception {
    SummarizerConfig config = (SummarizerConfig) 
        SummarizerConfig.newBuilder()
        .setSummaries(Lists.newArrayList("sum", "avg"))
        .setInfectiousNan(true)
        .setPassThrough(true)
        .setId("summarizer")
        .build();
    
    assertEquals(2, config.getSummaries().size());
    assertTrue(config.getSummaries().contains("sum"));
    assertTrue(config.getSummaries().contains("avg"));
    assertTrue(config.getInfectiousNan());
    assertTrue(config.passThrough());
    assertEquals("summarizer", config.getId());
    
    try {
      SummarizerConfig.newBuilder()
          //.setSummaries(Lists.newArrayList("sum", "avg"))
          .setInfectiousNan(true)
          .setId("summarizer")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      SummarizerConfig.newBuilder()
          .setSummaries(Lists.newArrayList())
          .setInfectiousNan(true)
          .setId("summarizer")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      SummarizerConfig.newBuilder()
          .setSummaries(Lists.newArrayList("sum", "avg"))
          .setInfectiousNan(true)
          //.setId("summarizer")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      SummarizerConfig.newBuilder()
          .setSummaries(Lists.newArrayList("sum", "avg"))
          .setInfectiousNan(true)
          .setId("")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void equality() throws Exception {
    SummarizerConfig config = (SummarizerConfig)
            SummarizerConfig.newBuilder()
                    .setSummaries(Lists.newArrayList("sum", "avg"))
                    .setInfectiousNan(true)
                    .setId("summarizer")
                    .build();

    SummarizerConfig config2 = (SummarizerConfig)
            SummarizerConfig.newBuilder()
                    .setSummaries(Lists.newArrayList("avg", "sum"))
                    .setInfectiousNan(true)
                    .setPassThrough(true) // ignored
                    .setId("summarizer")
                    .build();

    SummarizerConfig config3 = (SummarizerConfig)
            SummarizerConfig.newBuilder()
                    .setSummaries(Lists.newArrayList("sum", "avg", "other"))
                    .setInfectiousNan(true)
                    .setId("summarizer")
                    .build();


    assertTrue(config.equals(config2));
    assertTrue(!config.equals(config3));
    assertEquals(config.hashCode(), config2.hashCode());
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = (SummarizerConfig)
            SummarizerConfig.newBuilder()
                    .setSummaries(Lists.newArrayList("sum", "avg"))
                    .setInfectiousNan(false)
                    .setId("summarizer")
                    .build();

    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = (SummarizerConfig)
            SummarizerConfig.newBuilder()
                    .setSummaries(Lists.newArrayList("sum", "avg"))
                    .setInfectiousNan(true)
                    .setId("not_summarizer")
                    .build();

    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());


  }
  
  @Test
  public void serdes() throws Exception {
    SummarizerConfig config = (SummarizerConfig) 
        SummarizerConfig.newBuilder()
        .setSummaries(Lists.newArrayList("sum", "avg"))
        .setInfectiousNan(true)
        .setId("summarizer")
        .addSource("m1")
        .build();
    String json = JSON.serializeToString(config);
    
    assertTrue(json.contains("\"id\":\"summarizer\""));
    assertTrue(json.contains("\"type\":\"Summarizer\""));
    assertTrue(json.contains("\"sources\":[\"m1\"]"));
    assertTrue(json.contains("\"summaries\":[\"sum\",\"avg\"]"));
    assertTrue(json.contains("\"infectiousNan\":true"));
    
    JsonNode node = JSON.getMapper().readTree(json);
    config = (SummarizerConfig) new SummarizerFactory()
        .parseConfig(JSON.getMapper(), mock(TSDB.class), node);
    assertEquals(2, config.getSummaries().size());
    assertTrue(config.getSummaries().contains("sum"));
    assertTrue(config.getSummaries().contains("avg"));
    assertTrue(config.getInfectiousNan());
    assertEquals("summarizer", config.getId());
    assertEquals(1, config.getSources().size());
    assertEquals("m1", config.getSources().get(0));
    assertEquals(SummarizerFactory.TYPE, config.getType());
  }
}
