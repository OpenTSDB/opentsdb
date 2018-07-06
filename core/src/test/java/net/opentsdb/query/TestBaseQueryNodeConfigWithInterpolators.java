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

import com.google.common.hash.HashCode;

import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.interpolation.BaseInterpolatorConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;

public class TestBaseQueryNodeConfigWithInterpolators {

  @Test
  public void interpolatorConfigs() throws Exception {
    TestConfig node = (TestConfig) new TestConfig.Builder()
        .setId("foo")
        .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
            .setType(NumericType.TYPE.toString())
            .build())
        .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
            .setType(AnnotationType.TYPE.toString())
            .build())
        .build();
    assertEquals("foo", node.getId());
    assertEquals(2, node.interpolatorConfigs().size());
    assertTrue(node.interpolatorConfig(NumericType.TYPE) 
        instanceof TestInterpolatorConfig);
    assertTrue(node.interpolatorConfig(AnnotationType.TYPE) 
        instanceof TestInterpolatorConfig);
    assertNull(node.interpolatorConfig(NumericSummaryType.TYPE));
    
    node = (TestConfig) new TestConfig.Builder()
        .setId("foo2")
        .build();
    assertEquals("foo2", node.getId());
    assertNull(node.interpolatorConfigs());
    assertNull(node.interpolatorConfig(NumericType.TYPE));
    assertNull(node.interpolatorConfig(NumericSummaryType.TYPE));
    
    try {
      new TestConfig.Builder()
      .setId("foo")
      .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
          .setType(NumericType.TYPE.toString())
          .build())
      .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
          .setType("nosuchtype")
          .build())
      .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  static class TestConfig extends BaseQueryNodeConfigWithInterpolators {

    protected TestConfig(final Builder builder) {
      super(builder);
    }

    @Override
    public HashCode buildHashCode() { return null; }

    @Override
    public int compareTo(QueryNodeConfig o) { return 0; }
    
    static class Builder extends BaseQueryNodeConfigWithInterpolators.Builder {

      @Override
      public QueryNodeConfig build() {
        return new TestConfig(this);
      }
      
    }
  }
  
  static class TestInterpolatorConfig extends BaseInterpolatorConfig {

    protected TestInterpolatorConfig(Builder builder) {
      super(builder);
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
  }
}
