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
package net.opentsdb.storage.schemas.tsdb1x;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import com.stumbleupon.async.Deferred;

import net.opentsdb.query.filter.ChainFilter;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.filter.NotFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.TagValueLiteralOrFilter;
import net.opentsdb.query.filter.TagValueWildcardFilter;
import net.opentsdb.query.filter.ChainFilter.FilterOp;
import net.opentsdb.stats.MockTrace;
import net.opentsdb.storage.StorageException;

public class TestFilterUidResolver extends SchemaBase {

  @Test
  public void resolveUids() throws Exception {
    Schema schema = schema();
    
    QueryFilter filter = ChainFilter.newBuilder()
        .addFilter(TagValueWildcardFilter.newBuilder()
            .setFilter("*")
            .setTagKey(TAGK_B_STRING)
            .build())
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
            .setTagKey(TAGK_STRING)
            .build())
        .build();
    
    ResolvedChainFilter resolved = (ResolvedChainFilter) schema.resolveUids(filter, null).join();
    ResolvedTagValueFilter tag_filter = (ResolvedTagValueFilter) resolved.resolved().get(0);
    assertArrayEquals(TAGK_B_BYTES, tag_filter.getTagKey());
    assertNull(tag_filter.getTagValues());
    
    tag_filter = (ResolvedTagValueFilter) resolved.resolved().get(1);
    assertArrayEquals(TAGK_BYTES, tag_filter.getTagKey());
    assertEquals(2, tag_filter.getTagValues().size());
    assertArrayEquals(TAGV_BYTES, tag_filter.getTagValues().get(0));
    assertArrayEquals(TAGV_B_BYTES, tag_filter.getTagValues().get(1));
    
    // one tagv not found
    filter = ChainFilter.newBuilder()
        .addFilter(TagValueWildcardFilter.newBuilder()
            .setFilter("*")
            .setTagKey(TAGK_B_STRING)
            .build())
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setFilter(TAGV_STRING + "|" + TAGV_B_STRING + "|" + NSUN_TAGV)
            .setTagKey(TAGK_STRING)
            .build())
        .build();
    resolved = (ResolvedChainFilter) schema.resolveUids(filter, null).join();
    tag_filter = (ResolvedTagValueFilter) resolved.resolved().get(0);
    assertArrayEquals(TAGK_B_BYTES, tag_filter.getTagKey());
    assertNull(tag_filter.getTagValues());
    
    tag_filter = (ResolvedTagValueFilter) resolved.resolved().get(1);
    assertArrayEquals(TAGK_BYTES, tag_filter.getTagKey());
    assertEquals(3, tag_filter.getTagValues().size());
    assertArrayEquals(TAGV_BYTES, tag_filter.getTagValues().get(0));
    assertArrayEquals(TAGV_B_BYTES, tag_filter.getTagValues().get(1));
    assertNull(tag_filter.getTagValues().get(2));
    
    // tagk not found
    filter = ChainFilter.newBuilder()
        .addFilter(TagValueWildcardFilter.newBuilder()
            .setFilter("*")
            .setTagKey(NSUN_TAGK)
            .build())
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setFilter(TAGV_STRING + "|" + NSUN_TAGV + "|" + TAGV_B_STRING)
            .setTagKey(TAGK_STRING)
            .build())
        .build();
    resolved = (ResolvedChainFilter) schema.resolveUids(filter, null).join();
    tag_filter = (ResolvedTagValueFilter) resolved.resolved().get(0);
    assertNull(tag_filter.getTagKey());
    assertNull(tag_filter.getTagValues());
    
    tag_filter = (ResolvedTagValueFilter) resolved.resolved().get(1);
    assertArrayEquals(TAGK_BYTES, tag_filter.getTagKey());
    assertEquals(3, tag_filter.getTagValues().size());
    assertArrayEquals(TAGV_BYTES, tag_filter.getTagValues().get(0));
    assertNull(tag_filter.getTagValues().get(1));
    assertArrayEquals(TAGV_B_BYTES, tag_filter.getTagValues().get(2));
    
    // nothing found at all.
    filter = ChainFilter.newBuilder()
        .addFilter(TagValueWildcardFilter.newBuilder()
            .setFilter("*")
            .setTagKey(NSUN_TAGK)
            .build())
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setFilter(NSUN_TAGV)
            .setTagKey("nope")
            .build())
        .build();
    resolved = (ResolvedChainFilter) schema.resolveUids(filter, null).join();
    tag_filter = (ResolvedTagValueFilter) resolved.resolved().get(0);
    assertNull(tag_filter.getTagKey());
    assertNull(tag_filter.getTagValues());
    
    tag_filter = (ResolvedTagValueFilter) resolved.resolved().get(1);
    assertNull(tag_filter.getTagKey());
    assertEquals(1, tag_filter.getTagValues().size());
    assertNull(tag_filter.getTagValues().get(0));
  }
  
  @Test
  public void resolveUidsEmptyListAndTrace() throws Exception {
    Schema schema = schema();
    
    QueryFilter mock = mock(QueryFilter.class);
    ResolvedQueryFilter filter = schema.resolveUids(mock, null).join();
    assertSame(mock, filter.filter());
    
    trace = new MockTrace();
    filter = schema.resolveUids(mock, trace.newSpan("UT").start()).join();
    assertEquals(0, trace.spans.size());
    assertSame(mock, filter.filter());
    
    trace = new MockTrace(true);
    filter = schema.resolveUids(mock, trace.newSpan("UT").start()).join();
    verifySpan(FilterUidResolver.class.getName() + ".resolve");
    assertSame(mock, filter.filter());
  }
  
  @Test
  public void resolveUidsIllegalArguments() throws Exception {
    Schema schema = schema();
    
    try {
      schema.resolveUids(null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void resolveUidsExceptionFromGet() throws Exception {
    Schema schema = schema();
    
    // in tagk
    QueryFilter filter = ChainFilter.newBuilder()
        .addFilter(TagValueWildcardFilter.newBuilder()
            .setFilter("*")
            .setTagKey(TAGK_STRING_EX)
            .build())
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
            .setTagKey(TAGK_STRING)
            .build())
        .build();
    
    Deferred<ResolvedQueryFilter> deferred = schema.resolveUids(filter, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    
    // in tag value
    filter = ChainFilter.newBuilder()
        .addFilter(TagValueWildcardFilter.newBuilder()
            .setFilter("*")
            .setTagKey(TAGK_B_STRING)
            .build())
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setFilter(TAGV_STRING + "|" + TAGV_STRING_EX)
            .setTagKey(TAGK_STRING)
            .build())
        .build();
    
    deferred = schema.resolveUids(filter, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
  }
  
  @Test
  public void resolveUidsTraceException() throws Exception {
    Schema schema = schema();
    trace = new MockTrace(true);
    
    QueryFilter filter = ChainFilter.newBuilder()
        .addFilter(TagValueWildcardFilter.newBuilder()
            .setFilter("*")
            .setTagKey(TAGK_STRING_EX)
            .build())
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
            .setTagKey(TAGK_STRING)
            .build())
        .build();
    
    Deferred<ResolvedQueryFilter> deferred = schema.resolveUids(filter, 
        trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { 
      verifySpan(FilterUidResolver.class.getName() + ".resolve", 
          StorageException.class, 6);
    }
  }
  
  @Test
  public void resolveNested() throws Exception {
    Schema schema = schema();
    
    QueryFilter filter = NotFilter.newBuilder()
        .setFilter(ChainFilter.newBuilder()
          .addFilter(TagValueWildcardFilter.newBuilder()
              .setFilter("*")
              .setTagKey(TAGK_B_STRING)
              .build())
          .addFilter(TagValueLiteralOrFilter.newBuilder()
              .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
              .setTagKey(TAGK_STRING)
              .build())
          .build())
        .build();
    
    ResolvedPassThroughFilter passthrough = 
        (ResolvedPassThroughFilter) schema.resolveUids(filter, null).join();
    ResolvedChainFilter chain = (ResolvedChainFilter) passthrough.resolved();
    ResolvedTagValueFilter tag_filter = (ResolvedTagValueFilter) chain.resolved().get(0);
    assertArrayEquals(TAGK_B_BYTES, tag_filter.getTagKey());
    assertNull(tag_filter.getTagValues());
    
    tag_filter = (ResolvedTagValueFilter) chain.resolved().get(1);
    assertArrayEquals(TAGK_BYTES, tag_filter.getTagKey());
    assertEquals(2, tag_filter.getTagValues().size());
    assertArrayEquals(TAGV_BYTES, tag_filter.getTagValues().get(0));
    assertArrayEquals(TAGV_B_BYTES, tag_filter.getTagValues().get(1));
  }
  
  @Test
  public void resolveUnknown() throws Exception {
    Schema schema = schema();
    
    QueryFilter filter = mock(QueryFilter.class);
    UnResolvedFilter unresolved = 
        (UnResolvedFilter) schema.resolveUids(filter, null).join();
    assertSame(filter, unresolved.filter());
  }
  
  @Test
  public void resolveWithMetric() throws Exception {
    Schema schema = schema();
    
    QueryFilter filter = ChainFilter.newBuilder()
          .addFilter(MetricLiteralFilter.newBuilder()
              .setMetric(METRIC_STRING)
              .build())
          .addFilter(TagValueWildcardFilter.newBuilder()
              .setFilter("*")
              .setTagKey(TAGK_B_STRING)
              .build())
          .addFilter(TagValueLiteralOrFilter.newBuilder()
              .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
              .setTagKey(TAGK_STRING)
              .build())
          .build();
    
    ResolvedChainFilter chain = (ResolvedChainFilter) schema.resolveUids(filter, null).join();
    ResolvedMetricLiteralFilter metric = (ResolvedMetricLiteralFilter) chain.resolved().get(0);
    assertEquals(METRIC_BYTES, metric.uid());
    
    ResolvedTagValueFilter tag_filter = (ResolvedTagValueFilter) chain.resolved().get(1);
    assertArrayEquals(TAGK_B_BYTES, tag_filter.getTagKey());
    assertNull(tag_filter.getTagValues());
    
    tag_filter = (ResolvedTagValueFilter) chain.resolved().get(2);
    assertArrayEquals(TAGK_BYTES, tag_filter.getTagKey());
    assertEquals(2, tag_filter.getTagValues().size());
    assertArrayEquals(TAGV_BYTES, tag_filter.getTagValues().get(0));
    assertArrayEquals(TAGV_B_BYTES, tag_filter.getTagValues().get(1));
  }
  
  @Test
  public void resolveNestedChain() throws Exception {
    Schema schema = schema();
    
    QueryFilter filter = ChainFilter.newBuilder()
          .addFilter(TagValueWildcardFilter.newBuilder()
              .setFilter("*")
              .setTagKey(TAGK_B_STRING)
              .build())
          .addFilter(ChainFilter.newBuilder()
              .setOp(FilterOp.OR)
              .addFilter(TagValueLiteralOrFilter.newBuilder()
                .setFilter(TAGV_STRING)
                .setTagKey(TAGK_STRING)
                .build())
              .addFilter(TagValueLiteralOrFilter.newBuilder()
                .setFilter(TAGV_B_STRING)
                .setTagKey(TAGK_STRING)
                .build())
              .build())
          .build();
    
    ResolvedChainFilter chain = (ResolvedChainFilter) schema.resolveUids(filter, null).join();
    ResolvedTagValueFilter tag_filter = (ResolvedTagValueFilter) chain.resolved().get(0);
    assertArrayEquals(TAGK_B_BYTES, tag_filter.getTagKey());
    assertNull(tag_filter.getTagValues());
    
    chain = (ResolvedChainFilter) chain.resolved().get(1);
    
    tag_filter = (ResolvedTagValueFilter) chain.resolved().get(0);
    assertArrayEquals(TAGK_BYTES, tag_filter.getTagKey());
    assertEquals(1, tag_filter.getTagValues().size());
    assertArrayEquals(TAGV_BYTES, tag_filter.getTagValues().get(0));
    
    tag_filter = (ResolvedTagValueFilter) chain.resolved().get(1);
    assertArrayEquals(TAGK_BYTES, tag_filter.getTagKey());
    assertEquals(1, tag_filter.getTagValues().size());
    assertArrayEquals(TAGV_B_BYTES, tag_filter.getTagValues().get(0));
  }
  
}
