// This file is part of OpenTSDB.
// Copyright (C) 2013-2017 The OpenTSDB Authors.
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
package net.opentsdb.meta;

import net.opentsdb.query.filter.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TestNamespacedAggregatedDocumentQuery {


  @Test
  public void testTagValueRegexQuery() {
    QueryFilter filter = TagValueRegexFilter.newBuilder().setFilter("cpu")
            .setTagKey("host").build();

   SearchSourceBuilder source = NamespacedAggregatedDocumentQuery.newBuilder()
            .setQuery_filter(Arrays
            .asList(filter)).build();

    String s = source.toString().replaceAll("\n","")
            .replaceAll(" ", "");

    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":{\"bool\":{\"must" +
            "\":{\"nested\":{\"filter\":{\"bool\":{\"must\":{\"bool\":{\"must" +
            "\":[{\"terms\":{\"tags.key.lowercase\":[\"host\"]}}," +
            "{\"regexp\":{\"tags.value\":\".*cpu.*\"}}]}}}}," +
            "\"path\":\"tags\"}}}}}}}", s.trim());

  }

  @Test
  public void testMetricRegexQuery() {
    QueryFilter filter = MetricRegexFilter.newBuilder().setMetric("cpu")
           .build();

    SearchSourceBuilder source = NamespacedAggregatedDocumentQuery.newBuilder()
            .setQuery_filter(Arrays
                    .asList(filter)).build();

    String s = source.toString().replaceAll("\n","")
            .replaceAll(" ", "");

    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":{\"bool\":{\"must" +
            "\":{\"nested\":{\"filter\":{\"bool\":{\"must\":{\"bool" +
            "\":{\"should\":{\"regexp\":{\"AM_nested.name.lowercase\":\".*cpu" +
            ".*\"}}}}}},\"path\":\"AM_nested\"}}}}}}}", s.trim());

  }

  @Test
  public void testMetricLiteralQuery() {
    QueryFilter filter = MetricLiteralFilter.newBuilder().setMetric("system" +
            ".cpu.busy.pct")
            .build();

    SearchSourceBuilder source = NamespacedAggregatedDocumentQuery.newBuilder()
            .setQuery_filter(Arrays
                    .asList(filter)).build();

    String s = source.toString().replaceAll("\n","")
            .replaceAll(" ", "");

    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":{\"bool\":{\"must" +
            "\":{\"nested\":{\"filter\":{\"bool\":{\"must\":{\"bool" +
            "\":{\"should\":{\"terms\":{\"AM_nested.name" +
            ".lowercase\":[\"system.cpu.busy.pct\"]}}}}}}," +
            "\"path\":\"AM_nested\"}}}}}}}", s.trim());

  }

  @Test
  public void testMultipleMetricLiterals() {
    QueryFilter filter = MetricLiteralFilter.newBuilder().setMetric("system" +
            ".cpu.busy.pct|system.cpu.idle")
            .build();

    SearchSourceBuilder source = NamespacedAggregatedDocumentQuery.newBuilder()
            .setQuery_filter(Arrays
                    .asList(filter)).build();

    String s = source.toString().replaceAll("\n","")
            .replaceAll(" ", "");

    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":{\"bool\":{\"must" +
            "\":{\"nested\":{\"filter\":{\"bool\":{\"must\":{\"bool" +
            "\":{\"should\":{\"terms\":{\"AM_nested.name" +
            ".lowercase\":[\"system.cpu.busy.pct\",\"system.cpu" +
            ".idle\"]}}}}}},\"path\":\"AM_nested\"}}}}}}}", s.trim());

  }

  @Test
  public void testTagValueLiteral() {
    QueryFilter filter = TagValueLiteralOrFilter.newBuilder().setFilter("cpu")
            .setTagKey("host")
            .build();

    SearchSourceBuilder source = NamespacedAggregatedDocumentQuery.newBuilder()
            .setQuery_filter(Arrays
                    .asList(filter)).build();

    String s = source.toString().replaceAll("\n","")
            .replaceAll(" ", "");

    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":{\"bool\":{\"must" +
            "\":{\"nested\":{\"filter\":{\"bool\":{\"must\":{\"bool\":{\"must" +
            "\":[{\"terms\":{\"tags.key.lowercase\":[\"host\"]}}," +
            "{\"terms\":{\"tags.value\":[\"cpu\"]}}]}}}}," +
            "\"path\":\"tags\"}}}}}}}", s.trim());

  }

  @Test
  public void testMultipleTagValueLiterals() {
    QueryFilter filter = TagValueLiteralOrFilter.newBuilder().setFilter
            ("host1|host2")
            .setTagKey("host")
            .build();

    SearchSourceBuilder source = NamespacedAggregatedDocumentQuery.newBuilder()
            .setQuery_filter(Arrays
                    .asList(filter)).build();

    String s = source.toString().replaceAll("\n","")
            .replaceAll(" ", "");

    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":{\"bool\":{\"must" +
            "\":{\"nested\":{\"filter\":{\"bool\":{\"must\":{\"bool\":{\"must" +
            "\":[{\"terms\":{\"tags.key.lowercase\":[\"host\"]}}," +
            "{\"terms\":{\"tags.value\":[\"host1\",\"host2\"]}}]}}}}," +
            "\"path\":\"tags\"}}}}}}}", s.trim());

  }

  @Test
  public void testAnyFieldRegexFilter() {
    QueryFilter filter = AnyFieldRegexFilter.newBuilder().setFilter
            ("host1|host2")
            .build();

    SearchSourceBuilder source = NamespacedAggregatedDocumentQuery.newBuilder()
            .setQuery_filter(Arrays
                    .asList(filter)).build();

    String s = source.toString().replaceAll("\n","")
            .replaceAll(" ", "");

    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":{\"bool\":{\"should" +
            "\":[{\"nested\":{\"filter\":{\"bool\":{\"should\":{\"regexp" +
            "\":{\"AM_nested.name.lowercase\":\".*host1.*|.*host2.*\"}}}}," +
            "\"path\":\"AM_nested\"}}," +
            "{\"nested\":{\"filter\":{\"bool\":{\"should\":{\"regexp" +
            "\":{\"tags.key.lowercase\":\".*host1.*|.*host2.*\"}}}}," +
            "\"path\":\"tags\"}}," +
            "{\"nested\":{\"filter\":{\"bool\":{\"should\":{\"regexp" +
            "\":{\"tags.value\":\".*host1.*|.*host2.*\"}}}}," +
            "\"path\":\"tags\"}}]}}}}}", s.trim());

  }

  @Test
  public void testAggByMetrics() {
    QueryFilter filter = MetricLiteralFilter.newBuilder().setMetric("system" +
            ".cpu.busy.pct|system.cpu.idle")
            .build();

    SearchSourceBuilder source = NamespacedAggregatedDocumentQuery.newBuilder()
            .setQuery_filter(Arrays
                    .asList(filter)).addAggregate(Arrays.asList(filter),
                    MetaQuery.AggregationField.METRICS, null, 0).build();


    String s = source.toString().replaceAll("\n","")
            .replaceAll(" ", "");

    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":{\"bool\":{\"must" +
            "\":{\"nested\":{\"filter\":{\"bool\":{\"must\":{\"bool" +
            "\":{\"should\":{\"terms\":{\"AM_nested.name" +
            ".lowercase\":[\"system.cpu.busy.pct\",\"system.cpu" +
            ".idle\"]}}}}}},\"path\":\"AM_nested\"}}}}}}," +
            "\"aggregations\":{\"aggs_metrics\":{\"nested\":{\"path" +
            "\":\"AM_nested\"}," +
            "\"aggregations\":{\"metrics\":{\"filter\":{\"bool\":{\"must" +
            "\":{\"terms\":{\"AM_nested.name.lowercase\":[\"system.cpu.busy" +
            ".pct\",\"system.cpu.idle\"]}}}}," +
            "\"aggregations\":{\"unique_metrics\":{\"terms\":{\"field" +
            "\":\"AM_nested.name.raw\",\"size\":0}}}}}}}}", s.trim());

  }

  @Test
  public void testAggByTagKeys() {
    QueryFilter filter = MetricLiteralFilter.newBuilder().setMetric("system" +
            ".cpu.busy.pct|system.cpu.idle")
            .build();

    SearchSourceBuilder source = NamespacedAggregatedDocumentQuery.newBuilder()
            .setQuery_filter(Arrays
                    .asList(filter)).addAggregate(Arrays.asList(filter),
                    MetaQuery.AggregationField.TAGS_KEYS, null, 0).build();


    String s = source.toString().replaceAll("\n","")
            .replaceAll(" ", "");

    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":{\"bool\":{\"must" +
            "\":{\"nested\":{\"filter\":{\"bool\":{\"must\":{\"bool" +
            "\":{\"should\":{\"terms\":{\"AM_nested.name" +
            ".lowercase\":[\"system.cpu.busy.pct\",\"system.cpu" +
            ".idle\"]}}}}}},\"path\":\"AM_nested\"}}}}}}," +
            "\"aggregations\":{\"aggs_tags" +
            ".key\":{\"nested\":{\"path\":\"tags\"}," +
            "\"aggregations\":{\"unique_tags" +
            ".key_keys\":{\"terms\":{\"field\":\"key.raw\",\"size\":0}}}}}}", s.trim());

  }

  @Test
  public void testAggByTagValuesWithTagKey() {
    QueryFilter filter = MetricLiteralFilter.newBuilder().setMetric("system" +
            ".cpu.busy.pct|system.cpu.idle")
            .build();

    SearchSourceBuilder source = NamespacedAggregatedDocumentQuery.newBuilder()
            .setQuery_filter(Arrays
                    .asList(filter)).addAggregate(Arrays.asList(filter),
                    MetaQuery.AggregationField.TAGS_VALUES, "host", 0).build();


    String s = source.toString().replaceAll("\n","")
            .replaceAll(" ", "");

    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":{\"bool\":{\"must" +
            "\":{\"nested\":{\"filter\":{\"bool\":{\"must\":{\"bool" +
            "\":{\"should\":{\"terms\":{\"AM_nested.name" +
            ".lowercase\":[\"system.cpu.busy.pct\",\"system.cpu" +
            ".idle\"]}}}}}},\"path\":\"AM_nested\"}}}}}}," +
            "\"aggregations\":{\"aggs_tags" +
            ".value\":{\"nested\":{\"path\":\"tags\"}," +
            "\"aggregations\":{\"tag_keys\":{\"filter\":{\"bool\":{\"should" +
            "\":{\"terms\":{\"tags.key.lowercase\":[\"host\"]}}}}," +
            "\"aggregations\":{\"unique_tags" +
            ".value_keys\":{\"terms\":{\"field\":\"key.raw\",\"size\":0}," +
            "\"aggregations\":{\"tag_values\":{\"filter\":{\"bool\":{\"should" +
            "\":{\"bool\":{\"must\":[{\"regexp\":{\"tags.value\":\".*.*" +
            ".*\"}},{\"regexp\":{\"tags.key.lowercase\":\".*host.*\"}}]}}}}," +
            "\"aggregations\":{\"unique_tags" +
            ".value_values\":{\"terms\":{\"field\":\"value.raw\"," +
            "\"size\":0}}}}}}}}}}}}", s.trim());

  }

  @Test
  public void testAggByTagValuesWithOutTagKey() {
    QueryFilter filter = MetricLiteralFilter.newBuilder().setMetric("system" +
            ".cpu.busy.pct|system.cpu.idle")
            .build();

    SearchSourceBuilder source = NamespacedAggregatedDocumentQuery.newBuilder()
            .setQuery_filter(Arrays
                    .asList(filter)).addAggregate(Arrays.asList(filter),
                    MetaQuery.AggregationField.TAGS_VALUES, null, 0).build();


    String s = source.toString().replaceAll("\n","")
            .replaceAll(" ", "");

    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":{\"bool\":{\"must" +
            "\":{\"nested\":{\"filter\":{\"bool\":{\"must\":{\"bool" +
            "\":{\"should\":{\"terms\":{\"AM_nested.name" +
            ".lowercase\":[\"system.cpu.busy.pct\",\"system.cpu" +
            ".idle\"]}}}}}},\"path\":\"AM_nested\"}}}}}}," +
            "\"aggregations\":{\"aggs_tags" +
            ".value\":{\"nested\":{\"path\":\"tags\"}," +
            "\"aggregations\":{\"unique_tags" +
            ".value_keys\":{\"terms\":{\"field\":\"key.raw\",\"size\":0}," +
            "\"aggregations\":{\"tag_values\":{\"filter\":{\"bool\":{\"should" +
            "\":{\"bool\":{\"must\":[{\"regexp\":{\"tags.value\":\".*.*" +
            ".*\"}},{\"regexp\":{\"tags.key.lowercase\":\".**.*\"}}]}}}}," +
            "\"aggregations\":{\"unique_tags" +
            ".value_values\":{\"terms\":{\"field\":\"value.raw\"," +
            "\"size\":0}}}}}}}}}}", s.trim());

  }

  @Test
  public void testAggByTagValuesWithSize() {
    QueryFilter filter = MetricLiteralFilter.newBuilder().setMetric("system" +
            ".cpu.busy.pct|system.cpu.idle")
            .build();

    SearchSourceBuilder source = NamespacedAggregatedDocumentQuery.newBuilder()
            .setQuery_filter(Arrays
                    .asList(filter)).addAggregate(Arrays.asList(filter),
                    MetaQuery.AggregationField.TAGS_VALUES, null, 100).build();


    String s = source.toString().replaceAll("\n","")
            .replaceAll(" ", "");

    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":{\"bool\":{\"must" +
            "\":{\"nested\":{\"filter\":{\"bool\":{\"must\":{\"bool" +
            "\":{\"should\":{\"terms\":{\"AM_nested.name" +
            ".lowercase\":[\"system.cpu.busy.pct\",\"system.cpu" +
            ".idle\"]}}}}}},\"path\":\"AM_nested\"}}}}}}," +
            "\"aggregations\":{\"aggs_tags" +
            ".value\":{\"nested\":{\"path\":\"tags\"}," +
            "\"aggregations\":{\"unique_tags" +
            ".value_keys\":{\"terms\":{\"field\":\"key.raw\",\"size\":0}," +
            "\"aggregations\":{\"tag_values\":{\"filter\":{\"bool\":{\"should" +
            "\":{\"bool\":{\"must\":[{\"regexp\":{\"tags.value\":\".*.*" +
            ".*\"}},{\"regexp\":{\"tags.key.lowercase\":\".**.*\"}}]}}}}," +
            "\"aggregations\":{\"unique_tags" +
            ".value_values\":{\"terms\":{\"field\":\"value.raw\"," +
            "\"size\":100}}}}}}}}}}", s.trim());

  }
}
