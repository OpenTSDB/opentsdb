// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.hash.HashCode;

import net.opentsdb.utils.JSON;

public class TestQueryExecutorConfig {

  @Test
  public void builder() throws Exception {
    
    UTConfig config = (UTConfig) UTConfig.newBuilder()
        .setExecutorId("Timer")
        .setExecutorType("TimedQueryExecutor")
        .build();
    assertEquals("Timer", config.getExecutorId());
    assertEquals("TimedQueryExecutor", config.executorType());
    
    String json = "{\"executorType\":"
        + "\"net.opentsdb.query.execution.TestQueryExecutorConfig$UTConfig\","
        + "\"executorId\":\"Timer\"}";
    config = (UTConfig) JSON.parseToObject(json, QueryExecutorConfig.class);
    assertEquals("Timer", config.getExecutorId());
    assertEquals("net.opentsdb.query.execution.TestQueryExecutorConfig$UTConfig", 
        config.executorType());
    
    json = JSON.serializeToString(config);
    assertTrue(json.contains("\"executorType\":\"TestQueryExecutorConfig\""));
    assertTrue(json.contains("\"executorId\":\"Timer\""));
    
    try {
      UTConfig.newBuilder()
        .setExecutorId(null)
        .setExecutorType("TimedQueryExecutor")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      UTConfig.newBuilder()
        .setExecutorId("")
        .setExecutorType("TimedQueryExecutor")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      UTConfig.newBuilder()
        .setExecutorId("Timer")
        .setExecutorType(null)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      UTConfig.newBuilder()
        .setExecutorId("Timer")
        .setExecutorType("")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @JsonDeserialize(builder = UTConfig.Builder.class)
  public static class UTConfig extends QueryExecutorConfig {

    protected UTConfig(Builder builder) {
      super(builder);
    }

    @Override
    public boolean equals(final Object o) {
      return false;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public HashCode buildHashCode() {
      return null;
    }

    @Override
    public int compareTo(final QueryExecutorConfig config) {
      return 0;
    }
    
    public static Builder newBuilder() {
      return new Builder();
    }
    
    public static class Builder extends QueryExecutorConfig.Builder {

      @Override
      public UTConfig build() {
        return new UTConfig(this);
      }
      
    }
  }
}
