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
package net.opentsdb.query.filter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Maps;

import net.opentsdb.query.filter.ChainFilter.FilterOp;

public class TestFilterUtils {

  @Test
  public void matchesExplicitTags() throws Exception {
    Map<String, String> tags = Maps.newHashMap();
    tags.put("host", "web01");

    // nested explicit tags chains
    QueryFilter filter = ChainFilter.newBuilder()
        .setOp(FilterOp.OR)
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setKey("owner")
            .setFilter("tyrion")
            .build())
        .addFilter(ChainFilter.newBuilder()
            .setOp(FilterOp.OR)
            .addFilter(TagValueLiteralOrFilter.newBuilder()
                .setKey("host")
                .setFilter("web02")
                .build())
            .addFilter(TagValueLiteralOrFilter.newBuilder()
                .setKey("host")
                .setFilter("web01")
                .build())
            .build())
        .build();

    QueryFilter explicitFilter = ExplicitTagsFilter.newBuilder().setFilter(filter).build();
    assertTrue(FilterUtils.matchesTags(explicitFilter, tags, null));


    // nested explicit tags chains
    filter = ChainFilter.newBuilder()
        .setOp(FilterOp.OR)
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setKey("owner")
            .setFilter("tyrion")
            .build())
        .addFilter(ChainFilter.newBuilder()
            .setOp(FilterOp.OR)
            .addFilter(TagValueLiteralOrFilter.newBuilder()
                .setKey("host")
                .setFilter("web02")
                .build())
            .addFilter(TagValueLiteralOrFilter.newBuilder()
                .setKey("host1")
                .setFilter("web01")
                .build())
            .build())
        .build();

    explicitFilter = ExplicitTagsFilter.newBuilder().setFilter(filter).build();
    assertFalse(FilterUtils.matchesTags(explicitFilter, tags, null));

  }

  @Test
  public void matchesTags() throws Exception {
    Map<String, String> tags = Maps.newHashMap();
    tags.put("host", "web01");
    
    // chain test
    QueryFilter filter = ChainFilter.newBuilder()
        .setOp(FilterOp.OR)
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setKey("host")
            .setFilter("web01|web02")
            .build())
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setKey("owner")
            .setFilter("tyrion")
            .build())
        .build();
    assertTrue(FilterUtils.matchesTags(filter, tags, null));
    
    filter = ChainFilter.newBuilder()
        .setOp(FilterOp.AND)
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setKey("host")
            .setFilter("web01|web02")
            .build())
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setKey("owner")
            .setFilter("tyrion")
            .build())
        .build();
    assertFalse(FilterUtils.matchesTags(filter, tags, null));
    
    // not chains
    filter = NotFilter.newBuilder()
        .setFilter(ChainFilter.newBuilder()
          .setOp(FilterOp.OR)
          .addFilter(TagValueLiteralOrFilter.newBuilder()
              .setKey("host")
              .setFilter("web01|web02")
              .build())
          .addFilter(TagValueLiteralOrFilter.newBuilder()
              .setKey("owner")
              .setFilter("tyrion")
              .build())
          .build())
        .build();
    assertFalse(FilterUtils.matchesTags(filter, tags, null));
    
    filter = NotFilter.newBuilder()
        .setFilter(ChainFilter.newBuilder()
          .setOp(FilterOp.AND)
          .addFilter(TagValueLiteralOrFilter.newBuilder()
              .setKey("host")
              .setFilter("web01|web02")
              .build())
          .addFilter(TagValueLiteralOrFilter.newBuilder()
              .setKey("owner")
              .setFilter("tyrion")
              .build())
          .build())
        .build();
    assertTrue(FilterUtils.matchesTags(filter, tags, null));
    
    // nested chains
    filter = ChainFilter.newBuilder()
        .setOp(FilterOp.OR)
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setKey("owner")
            .setFilter("tyrion")
            .build())
        .addFilter(ChainFilter.newBuilder()
            .setOp(FilterOp.OR)
            .addFilter(TagValueLiteralOrFilter.newBuilder()
                .setKey("host")
                .setFilter("web02")
                .build())
            .addFilter(TagValueLiteralOrFilter.newBuilder()
                .setKey("host")
                .setFilter("web01")
                .build())
          .build())
        .build();
    assertTrue(FilterUtils.matchesTags(filter, tags, null));

    filter = ChainFilter.newBuilder()
        .setOp(FilterOp.AND)
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setKey("owner")
            .setFilter("tyrion")
            .build())
        .addFilter(ChainFilter.newBuilder()
            .setOp(FilterOp.OR)
            .addFilter(TagValueLiteralOrFilter.newBuilder()
                .setKey("host")
                .setFilter("web02")
                .build())
            .addFilter(TagValueLiteralOrFilter.newBuilder()
                .setKey("host")
                .setFilter("web01")
                .build())
          .build())
        .build();
    assertFalse(FilterUtils.matchesTags(filter, tags, null));
    
    // singles
    filter = TagValueLiteralOrFilter.newBuilder()
        .setKey("host")
        .setFilter("web01")
        .build();
    assertTrue(FilterUtils.matchesTags(filter, tags, null));
    
    filter = TagValueLiteralOrFilter.newBuilder()
        .setKey("owner")
        .setFilter("tyrion")
        .build();
    assertFalse(FilterUtils.matchesTags(filter, tags, null));
    
    filter = NotFilter.newBuilder()
        .setFilter(TagValueLiteralOrFilter.newBuilder()
          .setKey("host")
          .setFilter("web01")
          .build())
        .build();
    assertFalse(FilterUtils.matchesTags(filter, tags, null));
    
    filter = NotFilter.newBuilder()
        .setFilter(TagValueLiteralOrFilter.newBuilder()
          .setKey("owner")
          .setFilter("tyrion")
          .build())
        .build();
    assertTrue(FilterUtils.matchesTags(filter, tags, null));
    
    // TODO - tag key filters
    
    filter = MetricLiteralFilter.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    assertTrue(FilterUtils.matchesTags(filter, tags, null));
    
    try {
      FilterUtils.matchesTags(null, tags, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      FilterUtils.matchesTags(filter, null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
