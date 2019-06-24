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

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.query.idconverter.ByteToStringIdConverterConfig;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.interpolation.BaseInterpolatorConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.utils.JSON;

public class TestBaseQueryNodeConfigWithInterpolators {

  @Test
  public void builder() throws Exception {
    TestConfig node = (TestConfig) new TestConfig.Builder()
        .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
            .setDataType(NumericType.TYPE.toString())
            .build())
        .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
            .setDataType(AnnotationType.TYPE.toString())
            .build())
        .setId("foo")
        .setSources(Lists.newArrayList("s1", "s2"))
        .setType("datasource")
        .build();
    assertEquals("foo", node.getId());
    assertEquals(2, node.getSources().size());
    assertTrue(node.getSources().contains("s1"));
    assertTrue(node.getSources().contains("s2"));
    assertEquals("datasource", node.getType());
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
          .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
              .setDataType(NumericType.TYPE.toString())
              .build())
          .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
              .setDataType("nosuchtype")
              .build())
          .setId("foo")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new TestConfig.Builder()
          .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
              .setDataType(NumericType.TYPE.toString())
              .build())
          .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
              .setDataType(AnnotationType.TYPE.toString())
              .build())
          .setId("")
          .setSources(Lists.newArrayList("s1", "s2"))
          .setType("datasource")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new TestConfig.Builder()
          .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
              .setDataType(NumericType.TYPE.toString())
              .build())
          .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
              .setDataType(AnnotationType.TYPE.toString())
              .build())
          //.setId("foo")
          .setSources(Lists.newArrayList("s1", "s2"))
          .setType("datasource")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void serialize() throws Exception {
    TestConfig node = (TestConfig) new TestConfig.Builder()
        .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
            .setDataType(NumericType.TYPE.toString())
            .build())
        .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
            .setDataType(AnnotationType.TYPE.toString())
            .build())
        .setId("foo")
        .setSources(Lists.newArrayList("s1", "s2"))
        .setType("datasource")
        .build();
    String json = JSON.serializeToString(node);
    assertTrue(json.contains("\"id\":\"foo\""));
    assertTrue(json.contains("\"type\":\"datasource\""));
    assertTrue(json.contains("\"sources\":[\"s1\",\"s2\"]"));
    assertTrue(json.contains("\"interpolatorConfigs\":["));
    assertTrue(json.contains("\"dataType\":\"net.opentsdb.data.types.annotation.AnnotationType\""));
    assertTrue(json.contains("\"dataType\":\"net.opentsdb.data.types.numeric.NumericType\""));
  }


  @Test
  public void equality() throws Exception {
    TestConfig config = (TestConfig) new TestConfig.Builder()
            .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
                    .setDataType(NumericType.TYPE.toString())
                    .build())
            .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
                    .setDataType(AnnotationType.TYPE.toString())
                    .build())
            .setId("foo")
            .setSources(Lists.newArrayList("s1", "s2"))
            .setType("datasource")
            .build();

    TestConfig config2 = (TestConfig) new TestConfig.Builder()
            .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
                    .setDataType(NumericType.TYPE.toString())
                    .build())
            .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
                    .setDataType(AnnotationType.TYPE.toString())
                    .build())
            .setId("foo")
            .setSources(Lists.newArrayList("s1", "s2"))
            .setType("datasource")
            .build();

    TestConfig config3 = (TestConfig) new TestConfig.Builder()
            .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
                    .setDataType(NumericType.TYPE.toString())
                    .build())
            .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
                    .setDataType(AnnotationType.TYPE.toString())
                    .build())
            .setId("foo")
            .setSources(Lists.newArrayList("s1", "s2", "s3"))
            .setType("datasource")
            .build();


    assertTrue(config.equals(config2));
    assertTrue(!config.equals(config3));
    assertEquals(config.hashCode(), config2.hashCode());
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = (TestConfig) new TestConfig.Builder()
            .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
                    .setDataType(NumericType.TYPE.toString())
                    .build())
            .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
                    .setDataType(AnnotationType.TYPE.toString())
                    .build())
            .setId("bar")
            .setSources(Lists.newArrayList("s1", "s2"))
            .setType("datasource")
            .build();

    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = (TestConfig) new TestConfig.Builder()
            .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
                    .setDataType(NumericType.TYPE.toString())
                    .build())
            .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
                    .setDataType(AnnotationType.TYPE.toString())
                    .build())
            .setId("foo")
            .setSources(Lists.newArrayList("s1", "s2"))
            .setType("data")
            .build();

    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = (TestConfig) new TestConfig.Builder()
            .addInterpolatorConfig(new TestInterpolatorConfig.Builder()
                    .setDataType(AnnotationType.TYPE.toString())
                    .build())
            .setId("foo")
            .setSources(Lists.newArrayList("s1", "s2"))
            .setType("datasource")
            .build();

    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());

  }


  
  static class TestConfig extends BaseQueryNodeConfigWithInterpolators {

    protected TestConfig(final Builder builder) {
      super(builder);
    }

    @Override
    public Builder toBuilder() {
      // TODO Auto-generated method stub
      return null;
    }
    
    @Override
    public boolean pushDown() {
      return false;
    }

    @Override
    public boolean joins() {
      // TODO Auto-generated method stub
      return false;
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
    
    @Override
    public HashCode buildHashCode() { return super.buildHashCode(); }

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
      return super.buildHashCode();
    }

    @Override
    public int compareTo(QueryInterpolatorConfig o) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public TypeToken<? extends TimeSeriesDataType> type() {
      if (data_type.endsWith("NumericType")) {
        return NumericType.TYPE;
      } else if (data_type.endsWith("NumericSummaryType")) {
        return NumericSummaryType.TYPE;
      } else if (data_type.endsWith("AnnotationType")) {
        return AnnotationType.TYPE;
      }
      throw new IllegalArgumentException("No type!");
    }
  }
}
