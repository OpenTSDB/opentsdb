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
package net.opentsdb.query.interpolation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.google.common.hash.HashCode;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;

public class TestBaseInterpolatorConfig {

  @Test
  public void builder() throws Exception {
    QueryInterpolatorConfig config = TestInterpolatorConfig.newBuilder()
        .setType("myid")
        .setDataType("numeric")
        .build();
    assertEquals("myid", config.getType());
    assertEquals("numeric", config.getDataType());
    assertNull(config.type());
    
    config = TestInterpolatorConfig.newBuilder()
        .setType("LERP")
        .setDataType("numeric")
        .build();
    assertEquals("LERP", config.getType());
    assertEquals("numeric", config.getDataType());
    assertNull(config.type());
    
    config = TestInterpolatorConfig.newBuilder()
        .setType("")
        .setDataType("numeric")
        .build();
    assertEquals("", config.getType());
    assertEquals("numeric", config.getDataType());
    assertNull(config.type());
    
    try {
      TestInterpolatorConfig.newBuilder()
        .setType("")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  static class TestInterpolatorConfig extends BaseInterpolatorConfig {
    
    protected TestInterpolatorConfig(final Builder builder) {
      super(builder);
    }

    static Builder newBuilder() {
      return new Builder();
    }
    
    static class Builder extends BaseInterpolatorConfig.Builder {

      @Override
      public QueryInterpolatorConfig build() {
        return new TestInterpolatorConfig(this);
      }
      
    }

    @Override
    public HashCode buildHashCode() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public int compareTo(QueryInterpolatorConfig o) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public TypeToken<? extends TimeSeriesDataType> type() {
      // TODO Auto-generated method stub
      return null;
    }
  }
}