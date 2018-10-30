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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;

import net.opentsdb.utils.JSON;

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
  
  static class TestConfig extends BaseQueryNodeConfig {

    protected TestConfig(Builder builder) {
      super(builder);
      // TODO Auto-generated constructor stub
    }

    @Override
    public Builder toBuilder() {
      // TODO Auto-generated method stub
      return null;
    }
    
    @Override
    public HashCode buildHashCode() {
      // TODO Auto-generated method stub
      return null;
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
    public int compareTo(QueryNodeConfig o) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public boolean equals(Object o) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public int hashCode() {
      // TODO Auto-generated method stub
      return 0;
    }
    
    public static class Builder extends BaseQueryNodeConfig.Builder {

      @Override
      public QueryNodeConfig build() {
        return new TestConfig(this);
      }
      
    }

    
  }
}
