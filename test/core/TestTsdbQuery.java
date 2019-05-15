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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TsdbQuery.ForTesting;
import net.opentsdb.query.QueryLimitOverride;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.filter.TagVWildcardFilter;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupQuery;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.utils.DateTime;

import org.jboss.netty.util.internal.ThreadLocalRandom;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.DeferredGroupException;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * This class is for unit testing the TsdbQuery class. Pretty much making sure
 * the various ctors and methods function as expected. For actually running the
 * queries and validating the group by and aggregation logic, see
 * {@link TestTsdbQueryQueries}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ DateTime.class, TsdbQuery.class })
public final class TestTsdbQuery extends BaseTsdbTest {

  private static final long ONE_DAY_MS = 24 * 60 * 60 * 1000;

  private TsdbQuery query = null;

  @Before
  public void beforeLocal() throws Exception {
    query = new TsdbQuery(tsdb);
  }

  @Test
  public void setStartTime() throws Exception {
    query.setStartTime(1356998400L);
    assertEquals(1356998400L, query.getStartTime());
  }

  @Test
  public void setStartTimeZero() throws Exception {
    query.setStartTime(0L);
  }

  @Test (expected = IllegalArgumentException.class)
  public void setStartTimeInvalidNegative() throws Exception {
    query.setStartTime(-1L);
  }

  @Test (expected = IllegalArgumentException.class)
  public void setStartTimeInvalidTooBig() throws Exception {
    query.setStartTime(17592186044416L);
  }

  @Test
  public void setStartTimeEqualtoEndTime() throws Exception {
    query.setEndTime(1356998400L);
    query.setStartTime(1356998400L);
  }

  @Test (expected = IllegalArgumentException.class)
  public void setStartTimeGreaterThanEndTime() throws Exception {
    query.setEndTime(1356998400L);
    query.setStartTime(1356998460L);
  }

  @Test
  public void setEndTime() throws Exception {
    query.setEndTime(1356998400L);
    assertEquals(1356998400L, query.getEndTime());
  }

  @Test
  public void getScanEndTimeSeconds() {
    long now = System.currentTimeMillis() / 1000;
    long baseTime = now - (now % Const.MAX_TIMESPAN);
    long expectedEndScanTime = baseTime + Const.MAX_TIMESPAN;

    for (int i = 0; i < 3600; i++) {
      long sec = baseTime + i;
      long ms = sec * 1000 + ThreadLocalRandom.current().nextInt(1000);
      query.setEndTime(sec);
      Assert.assertEquals("EndTime=" + sec, expectedEndScanTime,
          query.getScanEndTimeSeconds());
      query.setEndTime(ms);
      Assert.assertEquals("EndTime=" + ms, expectedEndScanTime,
          query.getScanEndTimeSeconds());
    }
  }

  @Test (expected = IllegalStateException.class)
  public void getStartTimeNotSet() throws Exception {
    query.getStartTime();
  }

  @Test (expected = IllegalArgumentException.class)
  public void setEndTimeInvalidNegative() throws Exception {
    query.setEndTime(-1L);
  }

  @Test (expected = IllegalArgumentException.class)
  public void setEndTimeInvalidTooBig() throws Exception {
    query.setEndTime(17592186044416L);
  }

  @Test (expected = IllegalArgumentException.class)
  public void setEndTimeEqualtoEndTime() throws Exception {
    query.setStartTime(1356998400L);
    query.setEndTime(1356998400L);
  }

  @Test (expected = IllegalArgumentException.class)
  public void setEndTimeGreaterThanEndTime() throws Exception {
    query.setStartTime(1356998460L);
    query.setEndTime(1356998400L);
  }

  @Test
  public void getEndTimeNotSet() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1357300800000L);
    assertEquals(1357300800000L, query.getEndTime());
  }

  @Test
  public void setTimeSeries() throws Exception {
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    assertNotNull(query);
    assertArrayEquals(METRIC_BYTES, ForTesting.getMetric(query));
    assertEquals(1, ForTesting.getFilters(query).size());
    assertEquals(1, ForTesting.getGroupBys(query).size());
    assertArrayEquals(TAGK_BYTES, ForTesting.getGroupBys(query).get(0));
    assertEquals(1, ForTesting.getRowKeyLiterals(query).size());
    assertEquals(1, ForTesting.getRowKeyLiterals(query).get(TAGV_BYTES).length);
    assertArrayEquals(TAGV_BYTES,
        ForTesting.getRowKeyLiterals(query).get(TAGV_BYTES)[0]);
  }

  @Test (expected = NullPointerException.class)
  public void setTimeSeriesNullTags() throws Exception {
    query.setTimeSeries(METRIC_STRING, null, Aggregators.SUM, false);
  }

  @Test
  public void setTimeSeriesEmptyTags() throws Exception {
    tags.clear();
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    assertNotNull(query);
  }

  @Test (expected = NoSuchUniqueName.class)
  public void setTimeSeriesNosuchMetric() throws Exception {
    query.setTimeSeries(NSUN_METRIC, tags, Aggregators.SUM, false);
  }

  @Test (expected = NoSuchUniqueName.class)
  public void setTimeSeriesNosuchTagk() throws Exception {
    tags.clear();
    tags.put(NSUN_TAGK, TAGV_STRING);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
  }

  @Test (expected = NoSuchUniqueName.class)
  public void setTimeSeriesNosuchTagv() throws Exception {
    tags.put(TAGK_STRING, NSUN_TAGV);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
  }

  @Test
  public void setTimeSeriesTS() throws Exception {
    final List<String> tsuids = new ArrayList<String>(2);
    tsuids.add("000001000001000001");
    tsuids.add("000001000001000002");
    query.setTimeSeries(tsuids, Aggregators.SUM, false);
    assertNotNull(query);
  }

  @Test (expected = IllegalArgumentException.class)
  public void setTimeSeriesTSNullList() throws Exception {
    query.setTimeSeries(null, Aggregators.SUM, false);
  }

  @Test (expected = IllegalArgumentException.class)
  public void setTimeSeriesTSEmptyList() throws Exception {
    final List<String> tsuids = new ArrayList<String>();
    query.setTimeSeries(tsuids, Aggregators.SUM, false);
  }

  @Test (expected = IllegalArgumentException.class)
  public void setTimeSeriesTSDifferentMetrics() throws Exception {
    final List<String> tsuids = new ArrayList<String>(2);
    tsuids.add("000001000001000001");
    tsuids.add("000002000001000002");
    query.setTimeSeries(tsuids, Aggregators.SUM, false);
  }

  @Test
  public void configureFromQuery() throws Exception {
    setDataPointStorage();
    final TSQuery ts_query = getTSQuery();
    ts_query.validateAndSetQuery();
    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();

    assertArrayEquals(METRIC_BYTES, ForTesting.getMetric(query));
    assertEquals(1, ForTesting.getFilters(query).size());
    assertArrayEquals(TAGK_BYTES, ForTesting.getGroupBys(query).get(0));
    assertEquals(1, ForTesting.getGroupBys(query).size());
    assertNotNull(ForTesting.getRateOptions(query));
  }

  @Test
  public void configureFromQueryWithRate() throws Exception {
    setDataPointStorage();
    final TSQuery ts_query = getTSQuery();
    final RateOptions rate_options = new RateOptions();
    rate_options.setResetValue(1024);
    ts_query.getQueries().get(0).setRateOptions(rate_options);
    ts_query.validateAndSetQuery();
    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();

    assertArrayEquals(METRIC_BYTES, ForTesting.getMetric(query));
    assertEquals(1, ForTesting.getFilters(query).size());
    assertArrayEquals(TAGK_BYTES, ForTesting.getGroupBys(query).get(0));
    assertEquals(1, ForTesting.getGroupBys(query).size());
    assertTrue(rate_options == ForTesting.getRateOptions(query));
  }

  @Test
  public void configureFromQueryNoTags() throws Exception {

    setDataPointStorage();
    final TSQuery ts_query = getTSQuery();
    ts_query.getQueries().get(0).setTags(Collections.EMPTY_MAP);
    ts_query.validateAndSetQuery();
    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();

    assertArrayEquals(METRIC_BYTES, ForTesting.getMetric(query));
    assertEquals(0, ForTesting.getFilters(query).size());
    assertNull(ForTesting.getGroupBys(query));
    assertNull(ForTesting.getRowKeyLiterals(query));
  }

  @Test
  public void configureFromQueryGroupByAll() throws Exception {
    setDataPointStorage();
    final TSQuery ts_query = getTSQuery();
    tags.clear();
    tags.put(TAGK_STRING, "*");
    ts_query.getQueries().get(0).setTags(tags);
    ts_query.validateAndSetQuery();
    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();

    assertArrayEquals(METRIC_BYTES, ForTesting.getMetric(query));
    assertEquals(1, ForTesting.getFilters(query).size());
    assertEquals(1, ForTesting.getGroupBys(query).size());
    assertArrayEquals(TAGK_BYTES,
        ForTesting.getGroupBys(query).get(0));
    assertEquals(1, ForTesting.getRowKeyLiterals(query).size());
    assertNull(ForTesting.getRowKeyLiterals(query).get(TAGV_BYTES));
  }

  @Test
  public void configureFromQueryGroupByPipe() throws Exception {
    setDataPointStorage();
    final TSQuery ts_query = getTSQuery();
    tags.clear();
    tags.put(TAGK_STRING, TAGV_STRING + "|" + TAGV_B_STRING);
    ts_query.getQueries().get(0).setTags(tags);
    ts_query.validateAndSetQuery();
    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();

    assertArrayEquals(METRIC_BYTES, ForTesting.getMetric(query));
    assertEquals(1, ForTesting.getFilters(query).size());
    assertEquals(1, ForTesting.getGroupBys(query).size());
    assertArrayEquals(TAGK_BYTES, ForTesting.getGroupBys(query).get(0));
    assertEquals(1, ForTesting.getRowKeyLiterals(query).size());
    assertEquals(2, ForTesting.getRowKeyLiterals(query).get(TAGV_BYTES).length);
    assertArrayEquals(TAGV_BYTES,
        ForTesting.getRowKeyLiterals(query).get(TAGV_BYTES)[0]);
    assertArrayEquals(TAGV_B_BYTES,
        ForTesting.getRowKeyLiterals(query).get(TAGV_BYTES)[1]);
  }

  @Test
  public void configureFromQueryWithGroupByFilter() throws Exception {
    setDataPointStorage();
    final TSQuery ts_query = getTSQuery();
    tags.clear();
    tags.put("host", TagVWildcardFilter.FILTER_NAME + "(*imes)");
    ts_query.getQueries().get(0).setTags(tags);
    ts_query.validateAndSetQuery();
    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();

    assertArrayEquals(METRIC_BYTES, ForTesting.getMetric(query));
    assertEquals(1, ForTesting.getFilters(query).size());
    assertEquals(1, ForTesting.getGroupBys(query).size());
    assertArrayEquals(TAGK_BYTES, ForTesting.getGroupBys(query).get(0));
    assertEquals(1, ForTesting.getRowKeyLiterals(query).size());
    assertNull(ForTesting.getRowKeyLiterals(query).get(TAGV_BYTES));
    assertNotNull(ForTesting.getRateOptions(query));
  }

  @Test
  public void configureFromQueryWithFilter() throws Exception {
    setDataPointStorage();
    final TSQuery ts_query = getTSQuery();
    final List<TagVFilter> filters = new ArrayList<TagVFilter>(1);
    filters.add(new TagVWildcardFilter("host", "*imes"));
    ts_query.getQueries().get(0).setFilters(filters);
    ts_query.validateAndSetQuery();
    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();

    assertArrayEquals(METRIC_BYTES, ForTesting.getMetric(query));
    assertEquals(1, ForTesting.getFilters(query).size());
    assertNull(ForTesting.getGroupBys(query));
    assertEquals(1, ForTesting.getRowKeyLiterals(query).size());
    assertNull(ForTesting.getRowKeyLiterals(query).get(TAGV_BYTES));
    assertNotNull(ForTesting.getRateOptions(query));
  }

  @Test
  public void configureFromQueryWithGroupByAndRegularFilters() throws Exception {
    setDataPointStorage();
    final TSQuery ts_query = getTSQuery();
    final List<TagVFilter> filters = new ArrayList<TagVFilter>(1);
    filters.add(new TagVWildcardFilter("host", "*imes"));
    filters.add(TagVFilter.Builder().setFilter("*").setTagk("host")
        .setType("wildcard").setGroupBy(true).build());
    ts_query.getQueries().get(0).setFilters(filters);
    ts_query.validateAndSetQuery();

    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();
    assertArrayEquals(METRIC_BYTES, ForTesting.getMetric(query));
    assertEquals(2, ForTesting.getFilters(query).size());
    assertEquals(1, ForTesting.getGroupBys(query).size());
    assertArrayEquals(TAGK_BYTES, ForTesting.getGroupBys(query).get(0));
    assertEquals(1, ForTesting.getRowKeyLiterals(query).size());
    assertNull(ForTesting.getRowKeyLiterals(query).get(TAGK_BYTES));
    assertNotNull(ForTesting.getRateOptions(query));
  }

  @Test
  public void configureFromQueryWithForceRaw() throws Exception {
    setDataPointStorage();
    mockEnableRollupQuerySplitting();

    final TSQuery ts_query = getTSQuery(TsdbQuery.ROLLUP_USAGE.ROLLUP_NOFALLBACK);
    ts_query.validateAndSetQuery();
    query = spy(new TsdbQuery(tsdb));
    query.configureFromQuery(ts_query, 0, true).joinUninterruptibly();

    assertFalse(query.isRollupQuery());
    verify(query, never()).transformDownSamplerToRollupQuery(any(Aggregator.class), anyString());

    assertArrayEquals(METRIC_BYTES, ForTesting.getMetric(query));
    assertEquals(1, ForTesting.getFilters(query).size());
    assertArrayEquals(TAGK_BYTES, ForTesting.getGroupBys(query).get(0));
    assertEquals(1, ForTesting.getGroupBys(query).size());
    assertNotNull(ForTesting.getRateOptions(query));
  }

  @Test (expected = IllegalArgumentException.class)
  public void configureFromQueryNullSubs() throws Exception {
    final TSQuery ts_query = new TSQuery();
    new TsdbQuery(tsdb).configureFromQuery(ts_query, 0);
  }

  @Test (expected = IllegalArgumentException.class)
  public void configureFromQueryEmptySubs() throws Exception {
    final TSQuery ts_query = new TSQuery();
    ts_query.setQueries(new ArrayList<TSSubQuery>(0));
    new TsdbQuery(tsdb).configureFromQuery(ts_query, 0);
  }

  @Test (expected = IllegalArgumentException.class)
  public void configureFromQueryNegativeIndex() throws Exception {
    final TSQuery ts_query = getTSQuery();
    new TsdbQuery(tsdb).configureFromQuery(ts_query, -1);
  }

  @Test (expected = IllegalArgumentException.class)
  public void configureFromQueryIndexOutOfBounds() throws Exception {
    final TSQuery ts_query = getTSQuery();
    new TsdbQuery(tsdb).configureFromQuery(ts_query, 2);
  }

  @Test (expected = NoSuchUniqueName.class)
  public void configureFromQueryNSUMetric() throws Exception {
    setDataPointStorage();
    final TSQuery ts_query = getTSQuery();
    ts_query.getQueries().get(0).setMetric(NSUN_METRIC);
    ts_query.validateAndSetQuery();
    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();
  }

  @Test (expected = DeferredGroupException.class)
  public void configureFromQueryNSUTagk() throws Exception {
    setDataPointStorage();
    final TSQuery ts_query = getTSQuery();
    tags.clear();
    tags.put(NSUN_TAGK, TAGV_STRING);
    ts_query.getQueries().get(0).setTags(tags);
    ts_query.validateAndSetQuery();
    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();
  }

  @Test (expected = DeferredGroupException.class)
  public void configureFromQueryNSUTagv() throws Exception {
    setDataPointStorage();
    final TSQuery ts_query = getTSQuery();
    tags.clear();
    tags.put(TAGK_STRING, NSUN_TAGV);
    ts_query.getQueries().get(0).setTags(tags);
    ts_query.validateAndSetQuery();
    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();
  }

  @Test (expected = DeferredGroupException.class)
  public void configureFromQueryGroupByPipeNSUTagk() throws Exception {
    setDataPointStorage();
    final TSQuery ts_query = getTSQuery();
    tags.clear();
    tags.put(NSUN_TAGK, TAGV_STRING + "|" + TAGV_B_STRING);
    ts_query.getQueries().get(0).setTags(tags);
    ts_query.validateAndSetQuery();
    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();
  }

  @Test (expected = DeferredGroupException.class)
  public void configureFromQueryGroupByPipeNSUTagv() throws Exception {
    setDataPointStorage();
    final TSQuery ts_query = getTSQuery();
    tags.clear();
    tags.put(TAGK_STRING, TAGV_STRING + "|" + NSUN_TAGV);
    ts_query.getQueries().get(0).setTags(tags);
    ts_query.validateAndSetQuery();
    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();
  }

  @Test
  public void configureFromQueryGroupByPipeNSUTagvSkipUnresolved()
      throws Exception {
    config.overrideConfig("tsd.query.skip_unresolved_tagvs", "true");
    setDataPointStorage();
    final TSQuery ts_query = getTSQuery();
    tags.clear();
    tags.put(TAGK_STRING, TAGV_STRING + "|" + NSUN_TAGV);
    ts_query.getQueries().get(0).setTags(tags);
    ts_query.validateAndSetQuery();
    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();

    assertArrayEquals(METRIC_BYTES, ForTesting.getMetric(query));
    assertEquals(1, ForTesting.getFilters(query).size());
    assertEquals(1, ForTesting.getGroupBys(query).size());
    assertArrayEquals(TAGK_BYTES,
        ForTesting.getGroupBys(query).get(0));
  }

  @Test
  public void configureFromQueryMaxBytes() throws Exception {
    TSQuery ts_query = getTSQuery();
    ts_query.validateAndSetQuery();
    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();
    assertEquals(config.getInt("tsd.query.limits.bytes.default"), 
        ForTesting.maxBytes(query));
    
    config.overrideConfig("tsd.query.limits.bytes.default", "128");
    config.overrideConfig("tsd.query.limits.data_points.default", "16");
    Whitebox.setInternalState(tsdb, "query_limits", 
        new QueryLimitOverride(tsdb));
    ts_query = getTSQuery();
    ts_query.validateAndSetQuery();
    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();
    assertEquals(128, ForTesting.maxBytes(query));
    
    // disabled by default
    ts_query = getTSQuery();
    ts_query.setOverrideByteLimit(true);
    ts_query.validateAndSetQuery();
    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();
    assertEquals(128, ForTesting.maxBytes(query));
    
    config.overrideConfig("tsd.query.limits.bytes.allow_override", "true");
    ts_query = getTSQuery();
    ts_query.setOverrideByteLimit(true);
    ts_query.validateAndSetQuery();
    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();
    assertEquals(0, ForTesting.maxBytes(query));
  }
  
  @Test
  public void configureFromQueryMaxDataPoints() throws Exception {
    TSQuery ts_query = getTSQuery();
    ts_query.validateAndSetQuery();
    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();
    assertEquals(config.getInt("tsd.query.limits.data_points.default"), 
        ForTesting.maxDataPoints(query));
    
    config.overrideConfig("tsd.query.limits.bytes.default", "128");
    config.overrideConfig("tsd.query.limits.data_points.default", "16");
    Whitebox.setInternalState(tsdb, "query_limits", 
        new QueryLimitOverride(tsdb));
    ts_query = getTSQuery();
    ts_query.validateAndSetQuery();
    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();
    assertEquals(16, ForTesting.maxDataPoints(query));
    
    // disabled by default
    ts_query = getTSQuery();
    ts_query.setOverrideDataPointLimit(true);
    ts_query.validateAndSetQuery();
    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();
    assertEquals(16, ForTesting.maxDataPoints(query));
    
    config.overrideConfig("tsd.query.limits.data_points.allow_override", "true");
    ts_query = getTSQuery();
    ts_query.setOverrideDataPointLimit(true);
    ts_query.validateAndSetQuery();
    query = new TsdbQuery(tsdb);
    query.configureFromQuery(ts_query, 0).joinUninterruptibly();
    assertEquals(0, ForTesting.maxDataPoints(query));
  }
  
  @Test
  public void deleteDatapoints() throws Exception {
    setDataPointStorage();

    tsdb.addPoint(METRIC_STRING, 1356998400, 42, tags).joinUninterruptibly();
    query.setStartTime(1356998400);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    query.setDelete(true);
    final DataPoints[] dps1 = query.run();
    assertEquals(1, dps1.length);
    // second run should be empty
    final DataPoints[] dps2 = query.run();
    assertEquals(0, dps2.length);
  }

  @Test
  public void scannerException() throws Exception {
    storeLongTimeSeriesSeconds(true, false);
    final RuntimeException ex = new RuntimeException("Boo!");
    storage.throwException(MockBase.stringToBytes(
        "00000150E22700000001000001"), ex, true);

    storage.dumpToSystemOut();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    try {
      query.run();
      fail("Expected a RuntimeException");
    } catch (RuntimeException e) {
      assertSame(ex, e);
    }
  }

  @Test
  public void needsSplittingReturnsFalseIfDisabled() {
    Whitebox.setInternalState(tsdb, "rollups_split_queries", false);
    assertFalse(query.needsSplitting());
  }

  @Test
  public void needsSplittingReturnsFalseIfNotARollupQuery() {
    Whitebox.setInternalState(tsdb, "rollups_split_queries", true);
    Whitebox.setInternalState(query, "rollup_query", (RollupQuery) null);
    assertFalse(query.needsSplitting());
  }

  @Test
  public void needsSplittingReturnsFalseIfNoSLAConfigured() {
    Whitebox.setInternalState(tsdb, "rollups_split_queries", true);
    RollupInterval oneHourWithDelay = RollupInterval.builder()
            .setTable("fake-rollup-table")
            .setPreAggregationTable("fake-preagg-table")
            .setInterval("1h")
            .setRowSpan("1d")
            .setDelaySla(null)
            .build();
    RollupQuery rollup_query = new RollupQuery(
            oneHourWithDelay,
            Aggregators.SUM,
            3600000,
            Aggregators.SUM
    );
    Whitebox.setInternalState(query, "rollup_query", rollup_query);

    assertTrue(query.isRollupQuery());

    assertFalse(query.needsSplitting());
  }

  @Test
  public void needsSplittingReturnsFalseIfNotInBlackoutPeriod() {
    mockSystemTime(1356998400000L);
    mockEnableRollupQuerySplitting();

    query.setStartTime(0);
    query.setEndTime(1);

    assertTrue(query.isRollupQuery());

    assertFalse(query.needsSplitting());
  }

  @Test
  public void needsSplittingReturnsFalseIfQueryEndsWithLastRollupTimestamp() {
    mockSystemTime(1356998400000L);
    mockEnableRollupQuerySplitting();

    query.setStartTime(0);
    query.setEndTime(query.getRollupQuery().getLastRollupTimestampSeconds() * 1000L);

    assertTrue(query.isRollupQuery());

    assertFalse(query.needsSplitting());
  }

  @Test
  public void needsSplittingReturnsTrueIfQueryStartsWithLastRollupTimestamp() {
    long mockNowTimestamp = 1356998400000L;
    mockSystemTime(mockNowTimestamp);    mockEnableRollupQuerySplitting();

    query.setStartTime(query.getRollupQuery().getLastRollupTimestampSeconds() * 1000L);

    assertTrue(query.isRollupQuery());

    assertTrue(query.needsSplitting());
  }

  @Test
  public void needsSplittingReturnsTrueIfInBlackoutPeriod() {
    long mockNowTimestamp = 1356998400000L;
    mockSystemTime(mockNowTimestamp);
    mockEnableRollupQuerySplitting();

    query.setStartTime(0L);
    query.setEndTime(mockNowTimestamp);

    assertTrue(query.isRollupQuery());

    assertTrue(query.needsSplitting());
  }

  @Test
  public void needsSplittingReturnsTrueIfStartAndEndInBlackoutPeriod() {
    long mockNowTimestamp = 1356998400000L;
    mockSystemTime(mockNowTimestamp);
    mockEnableRollupQuerySplitting();

    int oneHour =  60 * 60 * 1000;

    query.setStartTime(mockNowTimestamp - oneHour);
    query.setEndTime(mockNowTimestamp);

    assertTrue(query.isRollupQuery());

    assertTrue(query.needsSplitting());
  }

  @Test
  public void split() {
    long mockSystemTime = 1356998400000L;
    mockSystemTime(mockSystemTime);
    mockEnableRollupQuerySplitting();

    TSQuery tsQuery = getTSQuery();
    TsdbQuery rawQuery = spy(new TsdbQuery(tsdb));

    query.setStartTime(mockSystemTime - 7 * ONE_DAY_MS);

    doReturn(Deferred.fromResult(null)).when(rawQuery).configureFromQuery(eq(tsQuery), eq(0), eq(true));

    query.split(tsQuery, 0, rawQuery);

    verify(rawQuery).configureFromQuery(eq(tsQuery), eq(0), eq(true));

    assertEquals(mockSystemTime - 7 * ONE_DAY_MS, query.getStartTime());
    assertEquals(mockSystemTime - 2 * ONE_DAY_MS, query.getEndTime());
    assertEquals(mockSystemTime - 2 * ONE_DAY_MS, rawQuery.getStartTime());
    assertEquals(mockSystemTime, rawQuery.getEndTime());
  }

  @Test(expected = IllegalStateException.class)
  public void splitThrowsIfNotSplittable() {
    Whitebox.setInternalState(tsdb, "rollups_split_queries", false);

    query.split(getTSQuery(), 0, new TsdbQuery(tsdb));
  }

  /** @return a simple TSQuery object for testing */
  private TSQuery getTSQuery() {
    return getTSQuery(null);
  }

  private TSQuery getTSQuery(TsdbQuery.ROLLUP_USAGE rollupUsage) {
    final TSQuery ts_query = new TSQuery();
    ts_query.setStart("1356998400");

    final ArrayList<TSSubQuery> sub_queries = new ArrayList<TSSubQuery>(1);
    sub_queries.add(getSubQuery(rollupUsage));

    ts_query.setQueries(sub_queries);
    return ts_query;
  }

  private TSSubQuery getSubQuery(TsdbQuery.ROLLUP_USAGE rollupUsage) {
    final TSSubQuery sub_query = new TSSubQuery();
    sub_query.setMetric(METRIC_STRING);
    sub_query.setAggregator("sum");

    sub_query.setTags(tags);

    if (rollupUsage != null) {
      sub_query.setRollupUsage(rollupUsage.name());
    }

    return sub_query;
  }

  private void mockSystemTime(long newTimestamp) {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(newTimestamp);
    PowerMockito.when(DateTime.getDurationUnits(anyString())).thenCallRealMethod();
    PowerMockito.when(DateTime.getDurationInterval(anyString())).thenCallRealMethod();
    PowerMockito.when(DateTime.parseDuration(anyString())).thenCallRealMethod();
  }

  private void mockEnableRollupQuerySplitting() {
    Whitebox.setInternalState(tsdb, "rollups_split_queries", true);
    Whitebox.setInternalState(query, "rollup_query", makeRollupQuery());
  }
}
