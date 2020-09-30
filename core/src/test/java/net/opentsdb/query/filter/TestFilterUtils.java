// This file is part of OpenTSDB.
// Copyright (C) 2018-2020  The OpenTSDB Authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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
    assertTrue(FilterUtils.matchesTags(explicitFilter, tags, Sets.newHashSet()));

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

  @Test
  public void desiredTagKeysNonTagRelated() throws Exception {
    QueryFilter filter = MetricLiteralFilter.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    assertNull(FilterUtils.desiredTagKeys(filter));
    
    filter = ChainFilter.newBuilder()
        .addFilter(MetricLiteralFilter.newBuilder()
          .setMetric("sys.cpu.user")
          .build())
        .addFilter(AnyFieldRegexFilter.newBuilder()
            .setFilter("host.*")
            .build())
        .build();
    assertNull(FilterUtils.desiredTagKeys(filter));
  }
  
  @Test
  public void desiredTagKeysSingles() throws Exception {
    QueryFilter filter = TagKeyLiteralOrFilter.newBuilder()
        .setFilter("host")
        .build();
    Set<String> keys = FilterUtils.desiredTagKeys(filter);
    
    assertEquals(1, keys.size());
    assertTrue(keys.contains("host"));
    
    filter = TagKeyRegexFilter.newBuilder()
        .setFilter("host.*")
        .build();
    assertNull(FilterUtils.desiredTagKeys(filter));
    
    filter = TagValueLiteralOrFilter.newBuilder()
        .setKey("host")
        .setFilter("web01")
        .build();
    keys = FilterUtils.desiredTagKeys(filter);
    
    assertEquals(1, keys.size());
    assertTrue(keys.contains("host"));
    
    filter = TagValueRegexFilter.newBuilder()
        .setKey("host")
        .setFilter("web.*")
        .build();
    keys = FilterUtils.desiredTagKeys(filter);
    
    assertEquals(1, keys.size());
    assertTrue(keys.contains("host"));
    
    filter = TagValueWildcardFilter.newBuilder()
        .setKey("host")
        .setFilter("web*")
        .build();
    keys = FilterUtils.desiredTagKeys(filter);
    
    assertEquals(1, keys.size());
    assertTrue(keys.contains("host"));
    
    filter = TagValueRangeFilter.newBuilder()
        .setKey("host")
        .setFilter("web{1-2}")
        .build();
    keys = FilterUtils.desiredTagKeys(filter);
    
    assertEquals(1, keys.size());
    assertTrue(keys.contains("host"));
  }
  
  @Test
  public void desiredTagKeysNots() throws Exception {
    QueryFilter filter = NotFilter.newBuilder()
        .setFilter(TagKeyLiteralOrFilter.newBuilder()
          .setFilter("host")
          .build())
        .build();
    Set<String> keys = FilterUtils.desiredTagKeys(filter);
    
    assertNull(keys);
    
    filter = NotFilter.newBuilder()
        .setFilter(TagKeyRegexFilter.newBuilder()
          .setFilter("host.*")
          .build())
        .build();
    assertNull(FilterUtils.desiredTagKeys(filter));
    
    filter = NotFilter.newBuilder()
        .setFilter(TagValueLiteralOrFilter.newBuilder()
          .setKey("host")
          .setFilter("web01")
          .build())
        .build();
    keys = FilterUtils.desiredTagKeys(filter);
    
    assertEquals(1, keys.size());
    assertTrue(keys.contains("host"));
    
    filter = NotFilter.newBuilder()
        .setFilter(TagValueRegexFilter.newBuilder()
          .setKey("host")
          .setFilter("web.*")
          .build())
        .build();
    keys = FilterUtils.desiredTagKeys(filter);
    
    assertEquals(1, keys.size());
    assertTrue(keys.contains("host"));
    
    // equivalent of not tag key
    filter = NotFilter.newBuilder()
        .setFilter(TagValueRegexFilter.newBuilder()
          .setKey("host")
          .setFilter(".*")
          .build())
        .build();
    keys = FilterUtils.desiredTagKeys(filter);
    
    assertNull(keys);
    
    filter = NotFilter.newBuilder()
        .setFilter(TagValueWildcardFilter.newBuilder()
          .setKey("host")
          .setFilter("web*")
          .build())
        .build();
    keys = FilterUtils.desiredTagKeys(filter);
    
    assertEquals(1, keys.size());
    assertTrue(keys.contains("host"));
    
    filter = NotFilter.newBuilder()
        .setFilter(TagValueWildcardFilter.newBuilder()
          .setKey("host")
          .setFilter("*")
          .build())
        .build();
    keys = FilterUtils.desiredTagKeys(filter);
    
    assertNull(keys);
    
    filter = NotFilter.newBuilder()
        .setFilter(TagValueRangeFilter.newBuilder()
          .setKey("host")
          .setFilter("web{1-2}")
          .build())
        .build();
    keys = FilterUtils.desiredTagKeys(filter);
    
    assertEquals(1, keys.size());
    assertTrue(keys.contains("host"));
  }

  @Test
  public void desiredTagKeysNested() throws Exception {
    QueryFilter filter = ChainFilter.newBuilder()
        .addFilter(TagKeyLiteralOrFilter.newBuilder()
          .setFilter("host")
          .build())
        .addFilter(TagValueRangeFilter.newBuilder()
          .setKey("service")
          .setFilter("web{1-2}")
          .build())
        .addFilter(MetricLiteralFilter.newBuilder()
          .setMetric("sys.cpu.user")
          .build())
        .build();
    Set<String> keys = FilterUtils.desiredTagKeys(filter);
    
    assertEquals(2, keys.size());
    assertTrue(keys.contains("host"));
    assertTrue(keys.contains("service"));
    
    filter = ChainFilter.newBuilder()
        .addFilter(TagKeyLiteralOrFilter.newBuilder()
          .setFilter("host")
          .build())
        .addFilter(NotFilter.newBuilder()
            .setFilter(TagValueRegexFilter.newBuilder()
                .setKey("service")
                .setFilter(".*")
                .build())
              .build())
        .addFilter(MetricLiteralFilter.newBuilder()
          .setMetric("sys.cpu.user")
          .build())
        .build();
    keys = FilterUtils.desiredTagKeys(filter);
    
    assertEquals(1, keys.size());
    assertTrue(keys.contains("host"));
    
    filter = ExplicitTagsFilter.newBuilder()
        .setFilter(ChainFilter.newBuilder()
          .addFilter(TagKeyLiteralOrFilter.newBuilder()
            .setFilter("host")
            .build())
          .addFilter(NotFilter.newBuilder()
              .setFilter(TagValueRegexFilter.newBuilder()
                  .setKey("service")
                  .setFilter(".*")
                  .build())
                .build())
          .addFilter(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
          .build())
        .build();
    keys = FilterUtils.desiredTagKeys(filter);
    
    assertEquals(1, keys.size());
    assertTrue(keys.contains("host"));
  }
}
