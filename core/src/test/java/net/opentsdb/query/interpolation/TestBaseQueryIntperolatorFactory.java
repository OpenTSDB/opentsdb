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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.Iterator;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy;

public class TestBaseQueryIntperolatorFactory {

  @Test
  public void registerAndInstantiate() throws Exception {
    TestFactory factory = new TestFactory();
    assertTrue(factory.types.isEmpty());
    
    factory.register(NumericType.TYPE, MockInterpolator.class, 
        new MockParser());
    assertEquals(1, factory.types.size());
    
    TimeSeries time_series = mock(TimeSeries.class);
    Iterator iterator = mock(Iterator.class);
    QueryInterpolatorConfig config = mock(QueryInterpolatorConfig.class);
    
    MockInterpolator interpolator = (MockInterpolator) 
        factory.newInterpolator(NumericType.TYPE, time_series, config);
    assertSame(time_series, interpolator.source);
    assertSame(config, interpolator.config);
   
    interpolator = (MockInterpolator) 
        factory.newInterpolator(NumericType.TYPE, iterator, config);
    assertSame(iterator, interpolator.source);
    assertSame(config, interpolator.config);
    
    assertNull(factory.newInterpolator(NumericSummaryType.TYPE, 
        time_series, config));
    assertNull(factory.newInterpolator(NumericSummaryType.TYPE, 
        iterator, config));
    
    // replace
    factory.register(NumericType.TYPE, MockInterpolator2.class, 
        new MockParser());
    assertEquals(1, factory.types.size());
    
    MockInterpolator2 interpolator2 = (MockInterpolator2) 
        factory.newInterpolator(NumericType.TYPE, time_series, config);
    assertSame(time_series, interpolator2.source);
    assertSame(config, interpolator2.config);
   
    interpolator2 = (MockInterpolator2) 
        factory.newInterpolator(NumericType.TYPE, iterator, config);
    assertSame(iterator, interpolator2.source);
    assertSame(config, interpolator2.config);
    
    try {
      factory.register(NumericType.TYPE, 
          MockInterpolatorMissTimeSeries.class, new MockParser());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.register(NumericType.TYPE, 
          MockInterpolatorMissIterator.class, new MockParser());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.register(null, MockInterpolator2.class, new MockParser());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.register(NumericType.TYPE, null, new MockParser());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory = new TestFactory();
      factory.register(NumericType.TYPE, MockInterpolator.class, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  static class TestFactory extends BaseQueryIntperolatorFactory {

    @Override
    public String type() { return "myid"; }

    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
      return Deferred.fromResult(null);
    }

    @Override
    public Deferred<Object> shutdown() {
      return Deferred.fromResult(null);
    }

    @Override
    public String version() {
      return "3.0.0";
    }

    @Override
    public QueryInterpolatorConfig parseConfig(ObjectMapper mapper, TSDB tsdb,
        JsonNode node) {
      // TODO Auto-generated method stub
      return null;
    }
    
  }
  
  static class MockParser implements QueryInterpolatorConfigParser {
    @Override
    public QueryInterpolatorConfig parse(ObjectMapper mapper, TSDB tsdb,
        JsonNode node) {
      return mock(QueryInterpolatorConfig.class);
    }
  }

  static class MockInterpolator implements QueryInterpolator<NumericType> {
    final Object source;
    final QueryInterpolatorConfig config;
    
    public MockInterpolator(final TimeSeries source, 
                            final QueryInterpolatorConfig config) {
      this.source = source;
      this.config = config;
    }
    
    public MockInterpolator(
        final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator,
        final QueryInterpolatorConfig config) {
      this.source = iterator;
      this.config = config;
    }
    
    @Override
    public boolean hasNext() { return false; }

    @Override
    public TimeSeriesValue<NumericType> next(TimeStamp timestamp) { return null; }

    @Override
    public TimeStamp nextReal() { return null; }

    @Override
    public QueryFillPolicy<NumericType> fillPolicy() { return null; }
    
  }
  
  static class MockInterpolator2 implements QueryInterpolator<NumericType> {
    final Object source;
    final QueryInterpolatorConfig config;
    
    public MockInterpolator2(final TimeSeries source, 
                            final QueryInterpolatorConfig config) {
      this.source = source;
      this.config = config;
    }
    
    public MockInterpolator2(
        final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator,
        final QueryInterpolatorConfig config) {
      this.source = iterator;
      this.config = config;
    }
    
    @Override
    public boolean hasNext() { return false; }

    @Override
    public TimeSeriesValue<NumericType> next(TimeStamp timestamp) { return null; }

    @Override
    public TimeStamp nextReal() { return null; }

    @Override
    public QueryFillPolicy<NumericType> fillPolicy() { return null; }
    
  }
  
  static class MockInterpolatorMissTimeSeries implements 
      QueryInterpolator<NumericType> {
    final Object source;
    final QueryInterpolatorConfig config;
    
    public MockInterpolatorMissTimeSeries(
        final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator,
        final QueryInterpolatorConfig config) {
      this.source = iterator;
      this.config = config;
    }
    
    @Override
    public boolean hasNext() { return false; }
    
    @Override
    public TimeSeriesValue<NumericType> next(TimeStamp timestamp) { return null; }
    
    @Override
    public TimeStamp nextReal() { return null; }
    
    @Override
    public QueryFillPolicy<NumericType> fillPolicy() { return null; }
    
  }
  
  static class MockInterpolatorMissIterator implements 
      QueryInterpolator<NumericType> {
    final Object source;
    final QueryInterpolatorConfig config;
    
    public MockInterpolatorMissIterator(final TimeSeries source, 
                                        final QueryInterpolatorConfig config) {
      this.source = source;
      this.config = config;
    }
    
    @Override
    public boolean hasNext() { return false; }
    
    @Override
    public TimeSeriesValue<NumericType> next(TimeStamp timestamp) { return null; }
    
    @Override
    public TimeStamp nextReal() { return null; }
    
    @Override
    public QueryFillPolicy<NumericType> fillPolicy() { return null; }
    
  }
}
