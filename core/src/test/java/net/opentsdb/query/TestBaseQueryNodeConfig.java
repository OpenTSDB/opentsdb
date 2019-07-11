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
package net.opentsdb.query;

import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.ScalarNumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;

import net.opentsdb.utils.JSON;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotEquals;

public class TestBaseQueryNodeConfig {

  @Test
  public void builder() {
    QueryNodeConfig config = new TestConfig.Builder()
        .setId("ut")
        .setSources(Lists.newArrayList("s1", "s2"))
        .setType("datasource")
        .build();
    assertEquals("ut", config.getId());
    assertEquals(2, config.getSources().size());
    assertTrue(config.getSources().contains("s1"));
    assertTrue(config.getSources().contains("s2"));
    assertEquals("datasource", config.getType());
    
    config = new TestConfig.Builder()
        .setId("ut")
        .build();
    assertEquals("ut", config.getId());
    assertTrue(config.getSources().isEmpty());
    assertNull(config.getType());
    
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
      return null;
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
