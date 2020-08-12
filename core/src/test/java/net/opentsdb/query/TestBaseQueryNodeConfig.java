// This file is part of OpenTSDB.
// Copyright (C) 2018-2020  The OpenTSDB Authors.
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
package net.opentsdb.query;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;

import net.opentsdb.utils.JSON;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestBaseQueryNodeConfig {

  @Test
  public void builder() {
    QueryNodeConfig config = new TestConfig.Builder()
        .setId("ut")
        .setSources(Lists.newArrayList("s1", "s2"))
        .setType("datasource")
        .addResultId(new DefaultQueryResultId("s1", "s1"))
        .addResultId(new DefaultQueryResultId("s2", "s2"))
        .addOverride("key", "value")
        .build();
    assertEquals("ut", config.getId());
    assertEquals(2, config.getSources().size());
    assertTrue(config.getSources().contains("s1"));
    assertTrue(config.getSources().contains("s2"));
    assertEquals("datasource", config.getType());
    assertEquals(2, config.resultIds().size());
    assertEquals(new DefaultQueryResultId("s1", "s1"), config.resultIds().get(0));
    assertEquals(new DefaultQueryResultId("s2", "s2"), config.resultIds().get(1));
    assertEquals(1, config.getOverrides().size());
    assertEquals("value", config.getOverrides().get("key"));
    
    // bare minimum
    config = new TestConfig.Builder()
        .setId("ut")
        .build();
    assertEquals("ut", config.getId());
    assertTrue(config.getSources().isEmpty());
    assertNull(config.getType());
    assertTrue(config.resultIds().isEmpty());
    assertNull(config.getOverrides());
    
    // set resultIds
    config = new TestConfig.Builder()
        .setId("ut")
        .setSources(Lists.newArrayList("s1", "s2"))
        .setType("datasource")
        .setResultIds(Lists.newArrayList(
            new DefaultQueryResultId("s1", "s1"), 
            new DefaultQueryResultId("s2", "s2")))
        .addOverride("key", "value")
        .build();
    assertEquals("ut", config.getId());
    assertEquals(2, config.getSources().size());
    assertTrue(config.getSources().contains("s1"));
    assertTrue(config.getSources().contains("s2"));
    assertEquals("datasource", config.getType());
    assertEquals(2, config.resultIds().size());
    assertEquals(new DefaultQueryResultId("s1", "s1"), config.resultIds().get(0));
    assertEquals(new DefaultQueryResultId("s2", "s2"), config.resultIds().get(1));
    assertEquals(1, config.getOverrides().size());
    assertEquals("value", config.getOverrides().get("key"));
    
    try {
      new TestConfig.Builder()
          .setId("")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new TestConfig.Builder()
          //.setId("ut")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void serialize() throws Exception {
    QueryNodeConfig config = new TestConfig.Builder()
        .setId("ut")
        .setSources(Lists.newArrayList("s1", "s2"))
        .setType("datasource")
        .build();
    String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"id\":\"ut\""));
    assertTrue(json.contains("\"type\":\"datasource\""));
    assertTrue(json.contains("\"sources\":[\"s1\",\"s2\"]"));
  }

  @Test
  public void equality() throws Exception {
    QueryNodeConfig config = new TestConfig.Builder()
            .setId("ut")
            .setSources(Lists.newArrayList("s1", "s2"))
            .setType("datasource")
            .build();

    QueryNodeConfig config2 = new TestConfig.Builder()
            .setId("ut")
            .setSources(Lists.newArrayList("s1", "s2"))
            .setType("datasource")
            .build();

    QueryNodeConfig config3 = new TestConfig.Builder()
            .setId("ut1")
            .setSources(Lists.newArrayList("s1", "s2"))
            .setType("datasource")
            .build();

    assertTrue(config.equals(config2));
    assertTrue(!config.equals(config3));
    assertEquals(config.hashCode(), config2.hashCode());
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = new TestConfig.Builder()
            .setId("ut")
            .setSources(Lists.newArrayList("s1"))
            .setType("datasource")
            .build();

    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = new TestConfig.Builder()
            .setId("ut")
            .setSources(Lists.newArrayList("s1", "s2"))
            .setType("data")
            .build();

    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());

  }
  
  @Test
  public void toBuilder() throws Exception {
    QueryNodeConfig original = new TestConfig.Builder()
        .setId("ut")
        .setSources(Lists.newArrayList("s1", "s2"))
        .setType("datasource")
        .addResultId(new DefaultQueryResultId("s1", "s1"))
        .addResultId(new DefaultQueryResultId("s2", "s2"))
        .addOverride("key", "value")
        .build();
    QueryNodeConfig rebuilt = original.toBuilder().build();
    
    assertEquals("ut", rebuilt.getId());
    assertEquals(2, rebuilt.getSources().size());
    assertTrue(rebuilt.getSources().contains("s1"));
    assertTrue(rebuilt.getSources().contains("s2"));
    assertEquals("datasource", rebuilt.getType());
    assertEquals(2, rebuilt.resultIds().size());
    assertEquals(new DefaultQueryResultId("s1", "s1"), rebuilt.resultIds().get(0));
    assertEquals(new DefaultQueryResultId("s2", "s2"), rebuilt.resultIds().get(1));
    assertEquals(1, rebuilt.getOverrides().size());
    assertEquals("value", rebuilt.getOverrides().get("key"));
    
    // bare minimum
    original = new TestConfig.Builder()
        .setId("ut")
        .build();
    rebuilt = original.toBuilder().build();
    
    assertEquals("ut", rebuilt.getId());
    assertTrue(rebuilt.getSources().isEmpty());
    assertNull(rebuilt.getType());
    assertTrue(rebuilt.resultIds().isEmpty());
    assertNull(rebuilt.getOverrides());
  }
  
  static class TestConfig extends BaseQueryNodeConfig<TestConfig.Builder, TestConfig> {

    protected TestConfig(Builder builder) {
      super(builder);
      // TODO Auto-generated constructor stub
    }

    @Override
    public HashCode buildHashCode() {
      // TODO Auto-generated method stub
      return super.buildHashCode();
    }

    @Override
    public boolean pushDown() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean joins() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public Builder toBuilder() {
      TestConfig.Builder builder = new Builder();
      super.toBuilder(builder);
      return builder;
    }

    @Override
    public int compareTo(TestConfig o) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public boolean equals(Object o) {
      // TODO Auto-generated method stub
      return super.equals(o);
    }

    @Override
    public int hashCode() {
      // TODO Auto-generated method stub
      return super.hashCode();
    }
    
    public static class Builder extends BaseQueryNodeConfig.Builder<Builder, TestConfig> {

      @Override
      public TestConfig build() {
        return new TestConfig(this);
      }

      @Override
      public Builder self() {
        return this;
      }

    }

    
  }
}
