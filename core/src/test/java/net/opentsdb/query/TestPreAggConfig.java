// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import net.opentsdb.query.PreAggConfig.MetricPattern;
import net.opentsdb.query.PreAggConfig.TagsAndAggs;
import net.opentsdb.utils.JSON;

public class TestPreAggConfig {
  private static final int BASE_TIMESTAMP = 1546300800;
  private static Map<String, PreAggConfig> PRE_AGG_CONFIG;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    PRE_AGG_CONFIG = Maps.newHashMap();
    
    PreAggConfig config = PreAggConfig.newBuilder()
        .addMetric(MetricPattern.newBuilder()
              .setMetric("sys.*")
              .addAggs(TagsAndAggs.newBuilder()
                  .addAgg("SUM", BASE_TIMESTAMP)
                  .addAgg("COUNT", BASE_TIMESTAMP)
                  .build())
              .addAggs(TagsAndAggs.newBuilder()
                  .addTag("app")
                  .addAgg("SUM", BASE_TIMESTAMP)
                  .addAgg("COUNT", BASE_TIMESTAMP)
                  .build())
              .addAggs(TagsAndAggs.newBuilder()
                  .addTag("colo")
                  .addTag("app")
                  .addAgg("SUM", BASE_TIMESTAMP)
                  .addAgg("COUNT", BASE_TIMESTAMP)
                  .build())
              .build())
        .addMetric(MetricPattern.newBuilder()
              .setMetric("net.*")
              .addAggs(TagsAndAggs.newBuilder()
                  .addTag("app")
                  .addAgg("SUM", BASE_TIMESTAMP + 3600)
                  .addAgg("COUNT", BASE_TIMESTAMP + 3600)
                  .build())
              .addAggs(TagsAndAggs.newBuilder()
                  .addTag("colo")
                  .addTag("app")
                  .addTag("dept")
                  .addAgg("SUM", BASE_TIMESTAMP + 3600)
                  .addAgg("COUNT", BASE_TIMESTAMP + 3600)
                  .build())
              .build())
        .build();
    PRE_AGG_CONFIG.put("MyNS", config);
  }
  
  @Test
  public void parse() throws Exception {
    String json = "{\"ns1\": {\"metrics\": [{\"metric\": \"sys.*\",\"aggs\": [{\"tags\": [\"dc\",\"service\"],\"aggs\": {\"sum\": 1234,\"count\": 1234}},{\"tags\": [\"dc\",\"service\",\"app\"],\"aggs\": {\"sum\": 2345,\"count\": 2345}}]}]},\"ns2\": {\"metrics\": [{\"metric\": \"sys.*\",\"aggs\": [{\"tags\": [],\"aggs\": {\"sum\": 3456,\"count\": 3456}}, {\"tags\": [\"dc\",\"service\"],\"aggs\": {\"sum\": 4567,\"count\": 5678}},{\"tags\": [\"dc\",\"service\",\"app\"],\"aggs\": {\"sum\": 7890,\"count\": 7890}}]}]}}";
    
    Map<String, PreAggConfig> config = JSON.parseToObject(json, PreAggConfig.TYPE_REF);
    assertEquals(2, config.size());
    
    PreAggConfig preagg = config.get("ns1");
    assertEquals(1, preagg.getMetrics().size());
    
    MetricPattern pattern = preagg.getMetrics().get(0);
    assertEquals("sys.*", pattern.getMetric());
    assertEquals(2, pattern.getAggs().size());
    
    // order is deterministic.
    TagsAndAggs aggs = pattern.getAggs().get(0);
    assertEquals(2, aggs.getTags().size());
    assertEquals("dc", aggs.getTags().get(0));
    assertEquals("service", aggs.getTags().get(1));
    assertEquals(2, aggs.getAggs().size());
    assertEquals(1234, (int) aggs.getAggs().get("SUM"));
    assertEquals(1234, (int) aggs.getAggs().get("COUNT"));
    
    aggs = pattern.getAggs().get(1);
    assertEquals(3, aggs.getTags().size());
    assertEquals("app", aggs.getTags().get(0));
    assertEquals("dc", aggs.getTags().get(1));
    assertEquals("service", aggs.getTags().get(2));
    assertEquals(2, aggs.getAggs().size());
    assertEquals(2345, (int) aggs.getAggs().get("SUM"));
    assertEquals(2345, (int) aggs.getAggs().get("COUNT"));
    
    preagg = config.get("ns2");
    assertEquals(1, preagg.getMetrics().size());
    
    pattern = preagg.getMetrics().get(0);
    assertEquals("sys.*", pattern.getMetric());
    assertEquals(3, pattern.getAggs().size());
    
    aggs = pattern.getAggs().get(0);
    assertEquals(0, aggs.getTags().size());
    assertEquals(2, aggs.getAggs().size());
    assertEquals(3456, (int) aggs.getAggs().get("SUM"));
    assertEquals(3456, (int) aggs.getAggs().get("COUNT"));
    
    aggs = pattern.getAggs().get(1);
    assertEquals(2, aggs.getTags().size());
    assertEquals("dc", aggs.getTags().get(0));
    assertEquals("service", aggs.getTags().get(1));
    assertEquals(2, aggs.getAggs().size());
    assertEquals(4567, (int) aggs.getAggs().get("SUM"));
    assertEquals(5678, (int) aggs.getAggs().get("COUNT"));
    
    aggs = pattern.getAggs().get(2);
    assertEquals(3, aggs.getTags().size());
    assertEquals("app", aggs.getTags().get(0));
    assertEquals("dc", aggs.getTags().get(1));
    assertEquals("service", aggs.getTags().get(2));
    assertEquals(2, aggs.getAggs().size());
    assertEquals(7890, (int) aggs.getAggs().get("SUM"));
    assertEquals(7890, (int) aggs.getAggs().get("COUNT"));
  }

  @Test
  public void equalsAndHash() throws Exception {
    PreAggConfig extant = PRE_AGG_CONFIG.get("MyNS");
    PreAggConfig config = PreAggConfig.newBuilder()
        .addMetric(MetricPattern.newBuilder()
            .setMetric("sys.*")
            .addAggs(TagsAndAggs.newBuilder()
                .addAgg("SUM", BASE_TIMESTAMP)
                .addAgg("COUNT", BASE_TIMESTAMP)
                .build())
            .addAggs(TagsAndAggs.newBuilder()
                .addTag("app")
                .addAgg("SUM", BASE_TIMESTAMP)
                .addAgg("COUNT", BASE_TIMESTAMP)
                .build())
            .addAggs(TagsAndAggs.newBuilder()
                .addTag("colo")
                .addTag("app")
                .addAgg("SUM", BASE_TIMESTAMP)
                .addAgg("COUNT", BASE_TIMESTAMP)
                .build())
            .build())
      .addMetric(MetricPattern.newBuilder()
            .setMetric("net.*")
            .addAggs(TagsAndAggs.newBuilder()
                .addTag("app")
                .addAgg("SUM", BASE_TIMESTAMP + 3600)
                .addAgg("COUNT", BASE_TIMESTAMP + 3600)
                .build())
            .addAggs(TagsAndAggs.newBuilder()
                .addTag("colo")
                .addTag("app")
                .addTag("dept")
                .addAgg("SUM", BASE_TIMESTAMP + 3600)
                .addAgg("COUNT", BASE_TIMESTAMP + 3600)
                .build())
            .build())
      .build();
    
    assertEquals(extant, config);
    assertEquals(extant.hashCode(), config.hashCode());
    
    config = PreAggConfig.newBuilder()
//        .addMetric(MetricPattern.newBuilder()
//              .setMetric("sys.*")
//              .addAggs(TagsAndAggs.newBuilder()
//                  .addAgg("SUM", BASE_TIMESTAMP)
//                  .addAgg("COUNT", BASE_TIMESTAMP)
//                  .build())
//              .addAggs(TagsAndAggs.newBuilder()
//                  .addTag("app")
//                  .addAgg("SUM", BASE_TIMESTAMP)
//                  .addAgg("COUNT", BASE_TIMESTAMP)
//                  .build())
//              .addAggs(TagsAndAggs.newBuilder()
//                  .addTag("colo")
//                  .addTag("app")
//                  .addAgg("SUM", BASE_TIMESTAMP)
//                  .addAgg("COUNT", BASE_TIMESTAMP)
//                  .build())
//              .build())
        .addMetric(MetricPattern.newBuilder()
              .setMetric("net.*")
              .addAggs(TagsAndAggs.newBuilder()
                  .addTag("app")
                  .addAgg("SUM", BASE_TIMESTAMP + 3600)
                  .addAgg("COUNT", BASE_TIMESTAMP + 3600)
                  .build())
              .addAggs(TagsAndAggs.newBuilder()
                  .addTag("colo")
                  .addTag("app")
                  .addTag("dept")
                  .addAgg("SUM", BASE_TIMESTAMP + 3600)
                  .addAgg("COUNT", BASE_TIMESTAMP + 3600)
                  .build())
              .build())
        .build();
    
    assertNotEquals(extant, config);
    assertNotEquals(extant.hashCode(), config.hashCode());
    
    config = PreAggConfig.newBuilder()
        .addMetric(MetricPattern.newBuilder()
              .setMetric("sys.*")
              .addAggs(TagsAndAggs.newBuilder()
                  .addAgg("SUM", BASE_TIMESTAMP)
                  .addAgg("COUNT", BASE_TIMESTAMP)
                  .build())
              .addAggs(TagsAndAggs.newBuilder() // diff order
                  .addTag("colo")
                  .addTag("app")
                  .addAgg("SUM", BASE_TIMESTAMP)
                  .addAgg("COUNT", BASE_TIMESTAMP)
                  .build())
              .addAggs(TagsAndAggs.newBuilder()
                  .addTag("app")
                  .addAgg("SUM", BASE_TIMESTAMP)
                  .addAgg("COUNT", BASE_TIMESTAMP)
                  .build())
              .build())
        .addMetric(MetricPattern.newBuilder()
              .setMetric("net.*")
              .addAggs(TagsAndAggs.newBuilder()
                  .addTag("app")
                  .addAgg("SUM", BASE_TIMESTAMP + 3600)
                  .addAgg("COUNT", BASE_TIMESTAMP + 3600)
                  .build())
              .addAggs(TagsAndAggs.newBuilder()
                  .addTag("colo")
                  .addTag("app")
                  .addTag("dept")
                  .addAgg("SUM", BASE_TIMESTAMP + 3600)
                  .addAgg("COUNT", BASE_TIMESTAMP + 3600)
                  .build())
              .build())
        .build();
    
    assertEquals(extant, config);
    assertEquals(extant.hashCode(), config.hashCode());
    
    config = PreAggConfig.newBuilder()
        .addMetric(MetricPattern.newBuilder()
              .setMetric("sys.*")
              .addAggs(TagsAndAggs.newBuilder()
                  .addAgg("SUM", BASE_TIMESTAMP)
                  .addAgg("COUNT", BASE_TIMESTAMP)
                  .build())
              .addAggs(TagsAndAggs.newBuilder()
                  .addTag("app")
                  .addAgg("SUM", BASE_TIMESTAMP)
                  .addAgg("COUNT", BASE_TIMESTAMP)
                  .build())
              .addAggs(TagsAndAggs.newBuilder()
                  .addTag("app") // diff order
                  .addTag("colo")
                  .addAgg("SUM", BASE_TIMESTAMP)
                  .addAgg("COUNT", BASE_TIMESTAMP)
                  .build())
              .build())
        .addMetric(MetricPattern.newBuilder()
              .setMetric("net.*")
              .addAggs(TagsAndAggs.newBuilder()
                  .addTag("app")
                  .addAgg("SUM", BASE_TIMESTAMP + 3600)
                  .addAgg("COUNT", BASE_TIMESTAMP + 3600)
                  .build())
              .addAggs(TagsAndAggs.newBuilder()
                  .addTag("colo")
                  .addTag("app")
                  .addTag("dept")
                  .addAgg("SUM", BASE_TIMESTAMP + 3600)
                  .addAgg("COUNT", BASE_TIMESTAMP + 3600)
                  .build())
              .build())
        .build();
    
    assertEquals(extant, config);
    assertEquals(extant.hashCode(), config.hashCode());
    
    config = PreAggConfig.newBuilder()
        .addMetric(MetricPattern.newBuilder() // diff order
            .setMetric("net.*")
            .addAggs(TagsAndAggs.newBuilder()
                .addTag("app")
                .addAgg("SUM", BASE_TIMESTAMP + 3600)
                .addAgg("COUNT", BASE_TIMESTAMP + 3600)
                .build())
            .addAggs(TagsAndAggs.newBuilder()
                .addTag("colo")
                .addTag("app")
                .addTag("dept")
                .addAgg("SUM", BASE_TIMESTAMP + 3600)
                .addAgg("COUNT", BASE_TIMESTAMP + 3600)
                .build())
            .build())
        .addMetric(MetricPattern.newBuilder()
              .setMetric("sys.*")
              .addAggs(TagsAndAggs.newBuilder()
                  .addAgg("SUM", BASE_TIMESTAMP)
                  .addAgg("COUNT", BASE_TIMESTAMP)
                  .build())
              .addAggs(TagsAndAggs.newBuilder()
                  .addTag("app")
                  .addAgg("SUM", BASE_TIMESTAMP)
                  .addAgg("COUNT", BASE_TIMESTAMP)
                  .build())
              .addAggs(TagsAndAggs.newBuilder()
                  .addTag("colo")
                  .addTag("app")
                  .addAgg("SUM", BASE_TIMESTAMP)
                  .addAgg("COUNT", BASE_TIMESTAMP)
                  .build())
              .build())
        .build();
    
    assertEquals(extant, config);
    assertEquals(extant.hashCode(), config.hashCode());
  }

  @Test
  public void matchingTagsAndAggs() throws Exception {
    PreAggConfig extant = PRE_AGG_CONFIG.get("MyNS");
    
    Set<String> tag_keys = Sets.newHashSet();
    
    // empty only matches sys, not net
    assertNull(extant.getMetrics().get(0).matchingTagsAndAggs(tag_keys));
    
    TagsAndAggs aggs = extant.getMetrics().get(1).matchingTagsAndAggs(tag_keys);
    assertEquals(0, aggs.getTags().size());
    assertEquals(BASE_TIMESTAMP, (int) aggs.getAggs().get("SUM"));
    assertEquals(BASE_TIMESTAMP, (int) aggs.getAggs().get("COUNT"));
    
    // one tag
    tag_keys.add("app");
    aggs = extant.getMetrics().get(0).matchingTagsAndAggs(tag_keys);
    assertEquals(1, aggs.getTags().size());
    assertEquals(BASE_TIMESTAMP + 3600, (int) aggs.getAggs().get("SUM"));
    assertEquals(BASE_TIMESTAMP + 3600, (int) aggs.getAggs().get("COUNT"));
    
    aggs = extant.getMetrics().get(1).matchingTagsAndAggs(tag_keys);
    assertEquals(1, aggs.getTags().size());
    assertEquals(BASE_TIMESTAMP, (int) aggs.getAggs().get("SUM"));
    assertEquals(BASE_TIMESTAMP, (int) aggs.getAggs().get("COUNT"));
    
    // two tags, one not in rules
    tag_keys.add("nosuchtagkey");
    assertNull(extant.getMetrics().get(0).matchingTagsAndAggs(tag_keys));
    assertNull(extant.getMetrics().get(1).matchingTagsAndAggs(tag_keys));
    
    // two tags but matched a 3 tag rule
    tag_keys.clear();
    tag_keys.add("app");
    tag_keys.add("dept");
    
    assertNull(extant.getMetrics().get(1).matchingTagsAndAggs(tag_keys));
    
    aggs = extant.getMetrics().get(0).matchingTagsAndAggs(tag_keys);
    assertEquals(3, aggs.getTags().size());
    assertEquals(BASE_TIMESTAMP + 3600, (int) aggs.getAggs().get("SUM"));
    assertEquals(BASE_TIMESTAMP + 3600, (int) aggs.getAggs().get("COUNT"));
  }
}
