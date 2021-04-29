// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.data.prometheus;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.format.DateTimeParseException;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.junit.Before;
import org.junit.Test;

import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.ChainFilter;
import net.opentsdb.query.filter.NotFilter;
import net.opentsdb.query.filter.TagValueFilter;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.expressions.ExpressionConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.query.processor.rate.RateConfig;
import net.opentsdb.query.processor.topn.TopNConfig;

public class TestPromQLParser {

  private String start;
  private String end;
  private String step;
  
  @Before
  public void before() throws Exception {
    start = "1577836800";
    end = "1577840400";
  }
  
  @Test
  public void ctor() throws Exception {
    String promql = "http_request_duration_seconds_sum";
    // only the start is required
    end = null;
    PromQLParser parser = new PromQLParser(promql, start, end, step);
    assertEquals(start, parser.start);
    assertNotNull(parser.end);
    
    end = "1577840400";
    parser = new PromQLParser(promql, start, end, step);
    assertEquals(start, parser.start);
    assertEquals(end, parser.end);
    
    step = "1m";
    parser = new PromQLParser(promql, start, end, step);
    assertEquals(start, parser.start);
    assertEquals(end, parser.end);
    assertEquals(step, parser.step);
    
    // internet date format
    start = "1985-04-12T23:20:50.52Z";
    end = "1985-04-12T23:30:50.52Z";
    parser = new PromQLParser(promql, start, end, step);
    assertEquals("482196050", parser.start);
    assertEquals("482196650", parser.end);
    
    // float step
    start = "1577836800";
    end = "1577840400";
    step = "60";
    parser = new PromQLParser(promql, start, end, step);
    assertEquals("60s", parser.step);
    
    step = "60.55";
    parser = new PromQLParser(promql, start, end, step);
    assertEquals("60s", parser.step);
    
    start = "1985-04-12T23:20:5whoops0.52Z";
    try {
      new PromQLParser(promql, start, end, step);
      fail("Expected DateTimeParseException");
    } catch (DateTimeParseException e) { }
  }
  
  @Test
  public void instant() throws Exception {
    String promql = "http_request_duration_seconds_sum";
    PromQLParser parser = new PromQLParser(promql, start, end, step);
    SemanticQuery query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(1, query.getExecutionGraph().size());
    TimeSeriesDataSourceConfig source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("http_request_duration_seconds_sum", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
    
    // hacked to support TSD style metrics.
    promql = "http.request.duration.seconds.sum";
    parser = new PromQLParser(promql, start, end, step);
    query = parser.parse().build();
    // TODO This'll change
    assertEquals(3600, query.endTime().epoch() - query.startTime().epoch());
    assertEquals(1, query.getExecutionGraph().size());
    source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("http.request.duration.seconds.sum", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
    assertNull(source_config.getFilter());
    assertNull(source_config.getFilterId());
  }
  
  @Test
  public void instantWithFilter() throws Exception {
    String promql = "http_requests_total{job=\"prometheus\",group!~\"canary\",colo!=\"DEN\",group=~\"^prod\"}";
    PromQLParser parser = new PromQLParser(promql, start, end, step);
    SemanticQuery query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(1, query.getExecutionGraph().size());
    TimeSeriesDataSourceConfig source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("http_requests_total", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
    
    ChainFilter chain = (ChainFilter) source_config.getFilter();
    assertEquals(4, chain.getFilters().size());
    TagValueFilter filter = (TagValueFilter) chain.getFilters().get(0);
    assertEquals("job", filter.getTagKey());
    assertEquals("prometheus", filter.getFilter());
    assertEquals("TagValueLiteralOr", filter.getType());
    
    filter = (TagValueFilter) ((NotFilter) chain.getFilters().get(1)).getFilter();
    assertEquals("group", filter.getTagKey());
    assertEquals("^canary$", filter.getFilter());
    assertEquals("TagValueRegex", filter.getType());
    
    filter = (TagValueFilter) ((NotFilter) chain.getFilters().get(2)).getFilter();
    assertEquals("colo", filter.getTagKey());
    assertEquals("DEN", filter.getFilter());
    assertEquals("TagValueLiteralOr", filter.getType());
    
    filter = (TagValueFilter) chain.getFilters().get(3);
    assertEquals("group", filter.getTagKey());
    assertEquals("^prod$", filter.getFilter());
    assertEquals("TagValueRegex", filter.getType());
  }

  @Test
  public void dottedAndHashedMetric() throws Exception {
    String promql = "ns.my-app.storage.flusher.segmentsFlushed.lastValue[5m]";
    PromQLParser parser = new PromQLParser(promql, start, end, step);
    SemanticQuery query = parser.parse().build();
    assertEquals("ns.my-app.storage.flusher.segmentsFlushed.lastValue",
            ((TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0)).getMetric().getMetric());
  }

  @Test
  public void range() throws Exception {
    String promql = "http_request_duration_seconds_sum[5m]";
    PromQLParser parser = new PromQLParser(promql, start, end, step);
    SemanticQuery query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(1, query.getExecutionGraph().size());
    TimeSeriesDataSourceConfig source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("http_request_duration_seconds_sum", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
    
    // step
    step = "1m";
    parser = new PromQLParser(promql, start, end, step);
    query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(2, query.getExecutionGraph().size());
    source_config = (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("http_request_duration_seconds_sum", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
    
    DownsampleConfig ds = (DownsampleConfig) query.getExecutionGraph().get(1);
    assertEquals("avg", ds.getAggregator());
    assertEquals("1m", ds.getInterval());
    assertEquals("m0", ds.getSources().get(0));
    
    // TODO - what's this?
    step = null;
    promql = "http_request_duration_seconds_sum[5m:]";
    parser = new PromQLParser(promql, start, end, step);
    query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(1, query.getExecutionGraph().size());
    source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("http_request_duration_seconds_sum", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
  }
  
  @Test
  public void rangeDownsample() throws Exception {
    String promql = "http_request_duration_seconds_sum[5m:1m]";
    PromQLParser parser = new PromQLParser(promql, start, end, step);
    SemanticQuery query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(2, query.getExecutionGraph().size());
    TimeSeriesDataSourceConfig source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("http_request_duration_seconds_sum", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
    
    DownsampleConfig ds = (DownsampleConfig) query.getExecutionGraph().get(1);
    assertEquals("avg", ds.getAggregator());
    assertEquals("1m", ds.getInterval());
    assertEquals("m0", ds.getSources().get(0));
  }
  
  @Test
  public void offset() throws Exception {
    String promql = "http_request_duration_seconds_sum[5m] offset 5m";
    PromQLParser parser = new PromQLParser(promql, start, end, step);
    SemanticQuery query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(1, query.getExecutionGraph().size());
    TimeSeriesDataSourceConfig source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("http_request_duration_seconds_sum", 
        source_config.getMetric().getMetric());
    assertEquals("5m", source_config.getTimeShiftInterval());
    assertEquals("m0", source_config.getId());
  }
  
  @Test
  public void groupBy() throws Exception {
    String promql = "sum by (job, host) (http_request_duration_seconds_sum)";
    PromQLParser parser = new PromQLParser(promql, start, end, step);
    SemanticQuery query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(2, query.getExecutionGraph().size());
    TimeSeriesDataSourceConfig source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("http_request_duration_seconds_sum", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
    
    GroupByConfig gb = (GroupByConfig) query.getExecutionGraph().get(1);
    assertEquals("sum", gb.getAggregator());
    assertEquals(2, gb.getTagKeys().size());
    assertTrue(gb.getTagKeys().contains("job"));
    assertTrue(gb.getTagKeys().contains("host"));
    assertEquals("m0", gb.getSources().get(0));
    
    // alternative way
    promql = "sum(http_request_duration_seconds_sum) by (job, host)";
    parser = new PromQLParser(promql, start, end, step);
    query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(2, query.getExecutionGraph().size());
    source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("http_request_duration_seconds_sum", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
    
    gb = (GroupByConfig) query.getExecutionGraph().get(1);
    assertEquals("sum", gb.getAggregator());
    assertEquals(2, gb.getTagKeys().size());
    assertTrue(gb.getTagKeys().contains("job"));
    assertTrue(gb.getTagKeys().contains("host"));
    assertEquals("m0", gb.getSources().get(0));
    
    // group-all
    promql = "sum(http_request_duration_seconds_sum)";
    parser = new PromQLParser(promql, start, end, step);
    query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(2, query.getExecutionGraph().size());
    source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("http_request_duration_seconds_sum", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
    
    gb = (GroupByConfig) query.getExecutionGraph().get(1);
    assertEquals("sum", gb.getAggregator());
    assertEquals(0, gb.getTagKeys().size());
    assertEquals("m0", gb.getSources().get(0));
    
    // rest of the functions
    promql = "avg(http_request_duration_seconds_sum)";
    parser = new PromQLParser(promql, start, end, step);
    query = parser.parse().build();
    gb = (GroupByConfig) query.getExecutionGraph().get(1);
    assertEquals("avg", gb.getAggregator());
    
    promql = "min(http_request_duration_seconds_sum)";
    parser = new PromQLParser(promql, start, end, step);
    query = parser.parse().build();
    gb = (GroupByConfig) query.getExecutionGraph().get(1);
    assertEquals("min", gb.getAggregator());
    
    promql = "max(http_request_duration_seconds_sum)";
    parser = new PromQLParser(promql, start, end, step);
    query = parser.parse().build();
    gb = (GroupByConfig) query.getExecutionGraph().get(1);
    assertEquals("max", gb.getAggregator());
    
    promql = "count(http_request_duration_seconds_sum)";
    parser = new PromQLParser(promql, start, end, step);
    query = parser.parse().build();
    gb = (GroupByConfig) query.getExecutionGraph().get(1);
    assertEquals("count", gb.getAggregator());
    
    // nope
    promql = "dev(http_request_duration_seconds_sum)";
    parser = new PromQLParser(promql, start, end, step);
    try {
      parser.parse().build();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void rate() throws Exception {
    // TODO - this probably shouldn't be allowed when we handle instants properly.
    String promql = "rate(http_request_duration_seconds_sum)";
    PromQLParser parser = new PromQLParser(promql, start, end, step);
    SemanticQuery query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(2, query.getExecutionGraph().size());
    TimeSeriesDataSourceConfig source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("http_request_duration_seconds_sum", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
    
    RateConfig rate = (RateConfig) query.getExecutionGraph().get(1);
    assertEquals("1s", rate.getInterval());
    assertTrue(rate.isCounter());
    assertTrue(rate.getDropResets());
    
    // with a range, should be the standard.
    promql = "rate(http_request_duration_seconds_sum[5m])";
    parser = new PromQLParser(promql, start, end, step);
    query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(2, query.getExecutionGraph().size());
    source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("http_request_duration_seconds_sum", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
    
    rate = (RateConfig) query.getExecutionGraph().get(1);
    assertEquals("1s", rate.getInterval());
    assertTrue(rate.isCounter());
    assertTrue(rate.getDropResets());
    assertEquals("m0", rate.getSources().get(0));
    
    // this is the odd case where the previous range is now the rate!
    promql = "rate(http_request_duration_seconds_sum[5m])[30m:1m]";
    parser = new PromQLParser(promql, start, end, step);
    query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(3, query.getExecutionGraph().size());
    source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("http_request_duration_seconds_sum", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
    
    rate = (RateConfig) query.getExecutionGraph().get(1);
    assertEquals("1s", rate.getInterval());
    assertTrue(rate.isCounter());
    assertTrue(rate.getDropResets());
    assertEquals("m0", rate.getSources().get(0));
    
    DownsampleConfig ds = (DownsampleConfig) query.getExecutionGraph().get(2);
    assertEquals("DownsampleConfig_m0", ds.getId());
    assertEquals("avg", ds.getAggregator());
    assertEquals("1m", ds.getInterval());
    assertEquals("RateConfig_m0", ds.getSources().get(0));
    
    // make sure we update the extant DS!
    promql = "rate(http_request_duration_seconds_sum[5m:30s])[30m:1m]";
    parser = new PromQLParser(promql, start, end, step);
    query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(3, query.getExecutionGraph().size());
    source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("http_request_duration_seconds_sum", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
    
    rate = (RateConfig) query.getExecutionGraph().get(1);
    assertEquals("1s", rate.getInterval());
    assertTrue(rate.isCounter());
    assertTrue(rate.getDropResets());
    assertEquals("m0", rate.getSources().get(0));
    
    ds = (DownsampleConfig) query.getExecutionGraph().get(2);
    assertEquals("DownsampleConfig_m0", ds.getId());
    assertEquals("avg", ds.getAggregator());
    assertEquals("1m", ds.getInterval());
    assertEquals("RateConfig_m0", ds.getSources().get(0));
  }

  @Test
  public void topk() throws Exception {
    String promql = "topk(5, http_request_duration_seconds_sum[30m:1m])";
    PromQLParser parser = new PromQLParser(promql, start, end, step);
    SemanticQuery query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(3, query.getExecutionGraph().size());
    TimeSeriesDataSourceConfig source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("http_request_duration_seconds_sum", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
    
    DownsampleConfig ds = (DownsampleConfig) query.getExecutionGraph().get(1);
    assertEquals("DownsampleConfig_m0", ds.getId());
    assertEquals("avg", ds.getAggregator());
    assertEquals("1m", ds.getInterval());
    assertEquals("m0", ds.getSources().get(0));
    
    TopNConfig top = (TopNConfig) query.getExecutionGraph().get(2);
    assertEquals(5, top.getCount());
    assertEquals("avg", top.getAggregator());
    assertTrue(top.getTop());
    assertEquals("DownsampleConfig_m0", top.getSources().get(0));
    
    promql = "bottomk(5, http_request_duration_seconds_sum[30m:1m])";
    parser = new PromQLParser(promql, start, end, step);
    query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(3, query.getExecutionGraph().size());
    source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("http_request_duration_seconds_sum", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
    
    top = (TopNConfig) query.getExecutionGraph().get(2);
    assertEquals(5, top.getCount());
    assertEquals("avg", top.getAggregator());
    assertFalse(top.getTop());
    assertEquals("DownsampleConfig_m0", top.getSources().get(0));
  }

  @Test
  public void overTime() throws Exception {
    String promql = "avg_over_time(http_request_duration_seconds_sum)";
    PromQLParser parser = new PromQLParser(promql, start, end, step);
    SemanticQuery query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(2, query.getExecutionGraph().size());
    TimeSeriesDataSourceConfig source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("http_request_duration_seconds_sum", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
    
    DownsampleConfig ds = (DownsampleConfig) query.getExecutionGraph().get(1);
    assertEquals("avg", ds.getAggregator());
    assertTrue(ds.getRunAll());
    assertEquals("0all", ds.getInterval());
    assertEquals("m0", ds.getSources().get(0));
    
    // two downsamples
    promql = "sum_over_time(http_request_duration_seconds_sum[30m:1m])";
    parser = new PromQLParser(promql, start, end, step);
    query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(3, query.getExecutionGraph().size());
    source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("http_request_duration_seconds_sum", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
    
    ds = (DownsampleConfig) query.getExecutionGraph().get(1);
    assertEquals("avg", ds.getAggregator());
    assertFalse(ds.getRunAll());
    assertEquals("1m", ds.getInterval());
    assertEquals("m0", ds.getSources().get(0));
    
    ds = (DownsampleConfig) query.getExecutionGraph().get(2);
    assertEquals("DownsampleConfig_1_m0", ds.getId());
    assertEquals("sum", ds.getAggregator());
    assertTrue(ds.getRunAll());
    assertEquals("0all", ds.getInterval());
    assertEquals("DownsampleConfig_m0", ds.getSources().get(0));
  }

  @Test
  public void arithmeticExpression() throws Exception {
    String promql = 
        "  (http_request_duration_seconds_bucket_a\n" +
        "+\n" + 
        "  http_request_duration_seconds_bucket_b\n" + 
        ") / 2 / http_request_duration_seconds_count";
    PromQLParser parser = new PromQLParser(promql, start, end, step);
    SemanticQuery query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(6, query.getExecutionGraph().size());
    TimeSeriesDataSourceConfig source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("http_request_duration_seconds_bucket_a", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
    
    source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(1);
    assertEquals("http_request_duration_seconds_bucket_b", 
        source_config.getMetric().getMetric());
    assertEquals("m1", source_config.getId());
    
    ExpressionConfig exp = (ExpressionConfig) query.getExecutionGraph().get(2);
    assertEquals("Expression_0", exp.getId());
    assertEquals("m0 + m1", exp.getExpression());
    assertEquals("Expression_0", exp.getAs());
    assertEquals(JoinType.NATURAL_OUTER, exp.getJoin().getJoinType());
    assertEquals("m0", exp.getSources().get(0));
    assertEquals("m1", exp.getSources().get(1));
    
    source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(3);
    assertEquals("http_request_duration_seconds_count", 
        source_config.getMetric().getMetric());
    assertEquals("m2", source_config.getId());
    
    exp = (ExpressionConfig) query.getExecutionGraph().get(4);
    assertEquals("Expression_1", exp.getId());
    assertEquals("Expression_0 / 2", exp.getExpression());
    assertEquals("Expression_1", exp.getAs());
    assertEquals(JoinType.NATURAL_OUTER, exp.getJoin().getJoinType());
    assertEquals("Expression_0", exp.getSources().get(0));
    
    exp = (ExpressionConfig) query.getExecutionGraph().get(5);
    assertEquals("Expression_2", exp.getId());
    assertEquals("Expression_1 / m2", exp.getExpression());
    assertEquals("Expression_2", exp.getAs());
    assertEquals(JoinType.NATURAL_OUTER, exp.getJoin().getJoinType());
    assertEquals("Expression_1", exp.getSources().get(0));
    assertEquals("m2", exp.getSources().get(1));
  }
  
  @Test
  public void arithmeticExpressionWithRanges() throws Exception {
    String promql = 
        "  (http_request_duration_seconds_bucket_a[5m]\n" +
        "+\n" + 
        "  http_request_duration_seconds_bucket_b[5m]\n" + 
        ") / 2 / http_request_duration_seconds_count[5m]";
    PromQLParser parser = new PromQLParser(promql, start, end, step);
    SemanticQuery query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(6, query.getExecutionGraph().size());
    TimeSeriesDataSourceConfig source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("http_request_duration_seconds_bucket_a", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
    
    source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(1);
    assertEquals("http_request_duration_seconds_bucket_b", 
        source_config.getMetric().getMetric());
    assertEquals("m1", source_config.getId());
    
    ExpressionConfig exp = (ExpressionConfig) query.getExecutionGraph().get(2);
    assertEquals("Expression_0", exp.getId());
    assertEquals("m0 + m1", exp.getExpression());
    assertEquals("Expression_0", exp.getAs());
    assertEquals(JoinType.NATURAL_OUTER, exp.getJoin().getJoinType());
    assertEquals("m0", exp.getSources().get(0));
    assertEquals("m1", exp.getSources().get(1));
    
    source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(3);
    assertEquals("http_request_duration_seconds_count", 
        source_config.getMetric().getMetric());
    assertEquals("m2", source_config.getId());
    
    exp = (ExpressionConfig) query.getExecutionGraph().get(4);
    assertEquals("Expression_1", exp.getId());
    assertEquals("Expression_0 / 2", exp.getExpression());
    assertEquals("Expression_1", exp.getAs());
    assertEquals(JoinType.NATURAL_OUTER, exp.getJoin().getJoinType());
    assertEquals("Expression_0", exp.getSources().get(0));
    
    exp = (ExpressionConfig) query.getExecutionGraph().get(5);
    assertEquals("Expression_2", exp.getId());
    assertEquals("Expression_1 / m2", exp.getExpression());
    assertEquals("Expression_2", exp.getAs());
    assertEquals(JoinType.NATURAL_OUTER, exp.getJoin().getJoinType());
    assertEquals("Expression_1", exp.getSources().get(0));
    assertEquals("m2", exp.getSources().get(1));
  }

  @Test
  public void conditionalExpression() throws Exception {
    String promql = "metric_a == metric_b";
    PromQLParser parser = new PromQLParser(promql, start, end, step);
    SemanticQuery query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(3, query.getExecutionGraph().size());
    TimeSeriesDataSourceConfig source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("metric_a", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
    
    source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(1);
    assertEquals("metric_b", 
        source_config.getMetric().getMetric());
    assertEquals("m1", source_config.getId());
    
    ExpressionConfig exp = (ExpressionConfig) query.getExecutionGraph().get(2);
    assertEquals("Expression_0", exp.getId());
    assertEquals("m0 == m1", exp.getExpression());
    assertEquals("Expression_0", exp.getAs());
    assertEquals(JoinType.NATURAL_OUTER, exp.getJoin().getJoinType());
    assertEquals("m0", exp.getSources().get(0));
    assertEquals("m1", exp.getSources().get(1));
    
    // other conditional ops
    promql = "metric_a != metric_b";
    parser = new PromQLParser(promql, start, end, step);
    query = parser.parse().build();

    exp = (ExpressionConfig) query.getExecutionGraph().get(2);
    assertEquals("m0 != m1", exp.getExpression());
    
    promql = "metric_a < metric_b";
    parser = new PromQLParser(promql, start, end, step);
    query = parser.parse().build();

    exp = (ExpressionConfig) query.getExecutionGraph().get(2);
    assertEquals("m0 < m1", exp.getExpression());
    
    promql = "metric_a > metric_b";
    parser = new PromQLParser(promql, start, end, step);
    query = parser.parse().build();

    exp = (ExpressionConfig) query.getExecutionGraph().get(2);
    assertEquals("m0 > m1", exp.getExpression());
    
    promql = "metric_a <= metric_b";
    parser = new PromQLParser(promql, start, end, step);
    query = parser.parse().build();

    exp = (ExpressionConfig) query.getExecutionGraph().get(2);
    assertEquals("m0 <= m1", exp.getExpression());
    
    promql = "metric_a >= metric_b";
    parser = new PromQLParser(promql, start, end, step);
    query = parser.parse().build();

    exp = (ExpressionConfig) query.getExecutionGraph().get(2);
    assertEquals("m0 >= m1", exp.getExpression());
    
    promql = "metric_a <> metric_b";
    parser = new PromQLParser(promql, start, end, step);
    try {
      parser.parse();
      fail("Expected ParseCancellationException");
    } catch (ParseCancellationException e) { }
  }
  
  @Test
  public void relationalExpression() throws Exception {
    String promql = "metric_a or metric_b";
    PromQLParser parser = new PromQLParser(promql, start, end, step);
    SemanticQuery query = parser.parse().build();
    assertEquals(start, query.getStart());
    assertEquals(end, query.getEnd());
    assertEquals(3, query.getExecutionGraph().size());
    TimeSeriesDataSourceConfig source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(0);
    assertEquals("metric_a", 
        source_config.getMetric().getMetric());
    assertEquals("m0", source_config.getId());
    
    source_config = 
        (TimeSeriesDataSourceConfig) query.getExecutionGraph().get(1);
    assertEquals("metric_b", 
        source_config.getMetric().getMetric());
    assertEquals("m1", source_config.getId());
    
    ExpressionConfig exp = (ExpressionConfig) query.getExecutionGraph().get(2);
    assertEquals("Expression_0", exp.getId());
    assertEquals("m0 or m1", exp.getExpression());
    assertEquals("Expression_0", exp.getAs());
    assertEquals(JoinType.NATURAL_OUTER, exp.getJoin().getJoinType());
    assertEquals("m0", exp.getSources().get(0));
    assertEquals("m1", exp.getSources().get(1));
    
    // and
    promql = "metric_a and metric_b";
    parser = new PromQLParser(promql, start, end, step);
    query = parser.parse().build();
    
    exp = (ExpressionConfig) query.getExecutionGraph().get(2);
    assertEquals("m0 and m1", exp.getExpression());
  }
}
