// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.utils.Config;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSQuery.class, TSDB.class, Config.class })
public final class TestTSQuery {

  private static final long PREDOWNSAMPLE_INTERVAL_2SECS = 2000L;

  private Config mockConfig;
  private TSDB mockTsdb;

  private @Captor ArgumentCaptor<DownsampleOptions> downsample_options_captor_a;
  private @Captor ArgumentCaptor<DownsampleOptions> downsample_options_captor_b;

  @Before
  public void before() {
    MockitoAnnotations.initMocks(this);
    mockConfig = PowerMockito.mock(Config.class);
    when(mockConfig.getPredownsampleInterval()).thenReturn(
        PREDOWNSAMPLE_INTERVAL_2SECS);
    mockTsdb = PowerMockito.mock(TSDB.class);
    when(mockTsdb.getConfig()).thenReturn(mockConfig);
  }

  @Test
  public void constructor() {
    assertNotNull(new TSQuery());
  }

  @Test
  public void validate() {
    TSQuery q = this.getMetricForValidate();
    q.validateAndSetQuery();
    assertEquals(1356998400000L, q.startTime());
    assertEquals(1356998460000L, q.endTime());
    TSSubQuery sub = q.getQueries().get(0);
    assertEquals("sys.cpu.0", sub.getMetric());
    assertEquals("*", sub.getTags().get("host"));
    assertEquals("lga", sub.getTags().get("dc"));
    assertEquals(Aggregators.SUM, sub.aggregator());
    DownsampleOptions postdownsample_options = sub.getDownsampleOptions();
    assertEquals(Aggregators.AVG, postdownsample_options.getDownsampler());
    assertEquals(300000, postdownsample_options.getIntervalMs());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateNullStart() {
    TSQuery q = this.getMetricForValidate();
    q.setStart(null);
    q.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateEmptyStart() {
    TSQuery q = this.getMetricForValidate();
    q.setStart("");
    q.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateInvalidStart() {
    TSQuery q = this.getMetricForValidate();
    q.setStart("Not a timestamp at all");
    q.validateAndSetQuery();
  }
  
  @Test
  public void validateNullEnd() {
    PowerMockito.mockStatic(System.class);
    when(System.currentTimeMillis()).thenReturn(1357300800000L);
    TSQuery q = this.getMetricForValidate();
    q.setEnd(null);
    q.validateAndSetQuery();
    assertEquals(1357300800000L, q.endTime());
  }
  
  @Test
  public void validateEmptyEnd() {    
    PowerMockito.mockStatic(System.class);
    when(System.currentTimeMillis()).thenReturn(1357300800000L);
    TSQuery q = this.getMetricForValidate();
    q.setEnd("");
    q.validateAndSetQuery();
    assertEquals(1357300800000L, q.endTime());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateNullQueries() {
    final TSQuery query = new TSQuery();
    query.setStart("1356998400");
    query.setEnd("1356998460");
    query.validateAndSetQuery();
  }

  @Test
  public void testBuildQueries() throws IOException {
    TSQuery q = this.getMetricForValidate();
    TSSubQuery sub = q.getQueries().get(0);
    // Forces the default pre downsampling options.
    sub.setPredownsample(null);
    q.validateAndSetQuery();
    Query mockQuery = mock(Query.class);
    when(mockTsdb.newQuery()).thenReturn(mockQuery);
    Query[] returnedQueries = q.buildQueries(mockTsdb);
    assertEquals(1, returnedQueries.length);
    assertSame(mockQuery, returnedQueries[0]);
    verify(mockQuery).setStartTime(1356998400000L);
    verify(mockQuery).setEndTime(1356998460000L);
    verify(mockQuery).setPredownsample(downsample_options_captor_a.capture());
    verify(mockQuery).setPostdownsample(downsample_options_captor_b.capture());
    assertEquals(PREDOWNSAMPLE_INTERVAL_2SECS,
        downsample_options_captor_a.getValue().getIntervalMs());
    assertEquals(Aggregators.AVG,
        downsample_options_captor_a.getValue().getDownsampler());
    assertTrue(downsample_options_captor_a.getValue().enabled());
    assertEquals(300000, downsample_options_captor_b.getValue().getIntervalMs());
    assertEquals(Aggregators.AVG,
        downsample_options_captor_b.getValue().getDownsampler());
    assertTrue(downsample_options_captor_b.getValue().enabled());
    verify(mockQuery).setTimeSeries("sys.cpu.0", sub.getTags(),
                                    sub.aggregator(), sub.getRate());
  }

  @Test
  public void testBuildQueries__predownsampling() throws IOException {
    TSQuery q = this.getMetricForValidate();
    TSSubQuery sub = q.getQueries().get(0);
    sub.setPredownsample("2m-max-pre");
    q.validateAndSetQuery();
    TSDB mockTsdb = PowerMockito.mock(TSDB.class);
    Query mockQuery = mock(Query.class);
    when(mockTsdb.newQuery()).thenReturn(mockQuery);
    Query[] returnedQueries = q.buildQueries(mockTsdb);
    assertEquals(1, returnedQueries.length);
    assertSame(mockQuery, returnedQueries[0]);
    verify(mockQuery).setStartTime(1356998400000L);
    verify(mockQuery).setEndTime(1356998460000L);
    verify(mockQuery).setPredownsample(downsample_options_captor_a.capture());
    verify(mockQuery).setPostdownsample(downsample_options_captor_b.capture());
    assertEquals(120000, downsample_options_captor_a.getValue().getIntervalMs());
    assertEquals(Aggregators.MAX,
        downsample_options_captor_a.getValue().getDownsampler());
    assertTrue(downsample_options_captor_a.getValue().enabled());
    assertEquals(300000, downsample_options_captor_b.getValue().getIntervalMs());
    assertEquals(Aggregators.AVG,
        downsample_options_captor_b.getValue().getDownsampler());
    assertTrue(downsample_options_captor_b.getValue().enabled());
    verify(mockQuery).setTimeSeries("sys.cpu.0", sub.getTags(),
                                    sub.aggregator(), sub.getRate());
  }

  @Test
  public void testBuildQueries__predownsamplingBigInterval() throws IOException {
    TSQuery q = this.getMetricForValidate();
    TSSubQuery sub = q.getQueries().get(0);
    sub.setPredownsample("20m-max-pre");
    q.validateAndSetQuery();
    TSDB mockTsdb = PowerMockito.mock(TSDB.class);
    Query mockQuery = mock(Query.class);
    when(mockTsdb.newQuery()).thenReturn(mockQuery);
    Query[] returnedQueries = q.buildQueries(mockTsdb);
    assertEquals(1, returnedQueries.length);
    assertSame(mockQuery, returnedQueries[0]);
    verify(mockQuery).setStartTime(1356998400000L);
    verify(mockQuery).setEndTime(1356998460000L);
    verify(mockQuery).setPredownsample(downsample_options_captor_a.capture());
    verify(mockQuery).setPostdownsample(downsample_options_captor_b.capture());
    // Checks if the post-downsample interval overrides the pre-downsample
    // interval when the pre-downsample interval is bigger than that of
    // the post-downsample interval.
    assertEquals(300000, downsample_options_captor_a.getValue().getIntervalMs());
    assertEquals(Aggregators.MAX,
        downsample_options_captor_a.getValue().getDownsampler());
    assertTrue(downsample_options_captor_a.getValue().enabled());
    assertEquals(300000, downsample_options_captor_b.getValue().getIntervalMs());
    assertEquals(Aggregators.AVG,
        downsample_options_captor_b.getValue().getDownsampler());
    assertTrue(downsample_options_captor_b.getValue().enabled());
    verify(mockQuery).setTimeSeries("sys.cpu.0", sub.getTags(),
                                    sub.aggregator(), sub.getRate());
  }

  @Test
  public void testBuildQueries__predownsamplingZero() throws IOException {
    TSQuery q = this.getMetricForValidate();
    TSSubQuery sub = q.getQueries().get(0);
    sub.setPredownsample("0s-max-pre");
    q.validateAndSetQuery();
    TSDB mockTsdb = PowerMockito.mock(TSDB.class);
    Query mockQuery = mock(Query.class);
    when(mockTsdb.newQuery()).thenReturn(mockQuery);
    Query[] returnedQueries = q.buildQueries(mockTsdb);
    assertEquals(1, returnedQueries.length);
    assertSame(mockQuery, returnedQueries[0]);
    verify(mockQuery).setStartTime(1356998400000L);
    verify(mockQuery).setEndTime(1356998460000L);
    verify(mockQuery).setPredownsample(downsample_options_captor_a.capture());
    verify(mockQuery).setPostdownsample(downsample_options_captor_b.capture());
    assertEquals(0, downsample_options_captor_a.getValue().getIntervalMs());
    assertEquals(Aggregators.MAX,
        downsample_options_captor_a.getValue().getDownsampler());
    assertFalse(downsample_options_captor_a.getValue().enabled());
    assertEquals(300000, downsample_options_captor_b.getValue().getIntervalMs());
    assertEquals(Aggregators.AVG,
        downsample_options_captor_b.getValue().getDownsampler());
    assertTrue(downsample_options_captor_b.getValue().enabled());
    verify(mockQuery).setTimeSeries("sys.cpu.0", sub.getTags(),
                                    sub.aggregator(), sub.getRate());
  }

  private TSQuery getMetricForValidate() {
    final TSQuery query = new TSQuery();
    query.setStart("1356998400");
    query.setEnd("1356998460");
    query.addSubQuery(TestTSSubQuery.getMetricForValidate());
    return query;
  }
}
