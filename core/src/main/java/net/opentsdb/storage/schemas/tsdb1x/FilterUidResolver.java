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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.query.filter.ChainFilter;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.filter.NestedQueryFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.TagValueFilter;
import net.opentsdb.query.filter.TagValueLiteralOrFilter;
import net.opentsdb.stats.Span;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Exceptions;

/**
 * A class for resolving the strings to UIDs in literal filters where
 * possible. Performs a recursive walk of the filter. The implementation
 * will process a filter tree, looking for specific types (TODO - implement
 * some interface that marks it as resolvable).
 * <p>
 * For {@link TagValueFilter} instances we resolve the tag key and if 
 * the values are a literal list, we'll resolve those.
 * <p>
 * {@link ChainFilter}s are resolved recursively.
 * <p>
 * {@link MetricLiteralFilter} are the only metric filters resolved right
 * now.
 * <p>
 * {@link NestedQueryFilter}s resolve to a {@link ResolvedPassThroughFilter}.
 * <p>
 * For all unhandled types, it returns an {@link UnResolvedFilter}.
 * 
 * @since 3.0
 */
public class FilterUidResolver {

  /** The parent schema. */
  private final Schema schema;
  
  /** The original query filter. */
  private final QueryFilter filter;
  
  /**
   * Protected ctor. We only want the Schema messing with it.
   * @param schema The non-null schema.
   * @param filter The non-null filter.
   */
  protected FilterUidResolver(final Schema schema, 
                              final QueryFilter filter) {
    this.schema = schema;
    this.filter = filter;
  }
  
  /**
   * Executes the resolution process.
   * @param span An optional tracing span.
   * @return A deferred resolving to the resolved filter tree or an
   * exception if something went pear shaped.
   */
  protected Deferred<ResolvedQueryFilter> resolve(final Span span) {
    return resolve(filter, span);
  }
  
  /**
   * The private recursive resolver that'll call itself with each nested
   * filter.
   * @param filter The current filter to process.
   * @param span An optional tracing span.
   * @return A deferred resolving to the resolved filter tree or an
   * exception if something went pear shaped.
   */
  private Deferred<ResolvedQueryFilter> resolve(final QueryFilter filter, 
                                                final Span span) {
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".resolve")
          .withTag("filter", filter.toString())
          .start();
    } else {
      child = null;
    }
    
    // TAG FILTER
    if (filter instanceof TagValueFilter) {
      // TODO - need to add some kind of interface plugins can implement
      // so we don't have to hard code this.
      final ResolvedTagValueFilter resolved = 
          new ResolvedTagValueFilter((TagValueFilter) filter);
      if (filter instanceof TagValueLiteralOrFilter) {
        class GroupCB implements Callback<ResolvedQueryFilter, 
            ArrayList<ResolvedQueryFilter>> {

          @Override
          public ResolvedQueryFilter call(
                final ArrayList<ResolvedQueryFilter> ignored)
              throws Exception {
            if (child != null) {
              child.setSuccessTags().finish();
            }
            return resolved;
          }
          
        }
        
        final List<Deferred<ResolvedQueryFilter>> deferreds = 
            Lists.newArrayListWithExpectedSize(2);
        deferreds.add(schema.getId(UniqueIdType.TAGK, 
            ((TagValueFilter) filter).tagKey(), child)
          .addCallback(new TagKCB(resolved)));
        deferreds.add(schema.getIds(UniqueIdType.TAGV, 
            ((TagValueLiteralOrFilter) filter).literals(), child)
            .addCallback(new TagVCB(resolved)));
        return Deferred.group(deferreds)
            .addCallbacks(new GroupCB(), new ErrorCB(child));
      } else {
        return schema.getId(UniqueIdType.TAGK, 
            ((TagValueFilter) filter).tagKey(), child)
          .addCallbacks(new TagKCB(resolved), new ErrorCB(child));
      }
    // CHAIN FILTER
    } else if (filter instanceof ChainFilter) {
      final ChainFilter chain = (ChainFilter) filter;
      final List<Deferred<ResolvedQueryFilter>> deferreds = 
          Lists.newArrayListWithExpectedSize(chain.getFilters().size());
      for (final QueryFilter chained : chain.getFilters()) {
        deferreds.add(resolve(chained, child));
      }
      
      class GroupCB implements Callback<ResolvedQueryFilter, 
          ArrayList<ResolvedQueryFilter>> {

        @Override
        public ResolvedQueryFilter call(
              final ArrayList<ResolvedQueryFilter> resolved)
            throws Exception {
          if (child != null) {
            child.setSuccessTags().finish();
          }
          return new ResolvedChainFilter(chain, resolved);
        }
        
      }
      
      return Deferred.groupInOrder(deferreds)
          .addCallbacks(new GroupCB(), new ErrorCB(child));
    // METRIC LITERAL
    } else if (filter instanceof MetricLiteralFilter) {
      return schema.getId(UniqueIdType.METRIC, 
          ((MetricLiteralFilter) filter).metric(), child)
          .addCallbacks(new MetricCB(filter), new ErrorCB(child));
    // NESTED
    } else if (filter instanceof NestedQueryFilter) {
      class ResolveCB implements Callback<ResolvedQueryFilter, ResolvedQueryFilter> {

        @Override
        public ResolvedQueryFilter call(final ResolvedQueryFilter resolved)
            throws Exception {
          if (child != null) {
            child.setSuccessTags().finish();
          }
          return new ResolvedPassThroughFilter(filter, resolved);
        }
        
      }
      
      return resolve(((NestedQueryFilter) filter).getFilter(), child)
          .addCallbacks(new ResolveCB(), new ErrorCB(child));
    // UNKNOWN
    } else {
      if (child != null) {
        child.setSuccessTags().finish();
      }
      return Deferred.fromResult(new UnResolvedFilter(filter));
    }
  }
  
  class ErrorCB implements Callback<ResolvedQueryFilter, Exception> {
    private final Span child;
    
    ErrorCB(final Span child) {
      this.child = child;
    }
    
    @Override
    public ResolvedQueryFilter call(final Exception ex) throws Exception {
      if (ex instanceof DeferredGroupException) {
        final Throwable t = Exceptions.getCause((DeferredGroupException) ex);
        if (child != null) {
          child.setErrorTags(t).finish();
        }
        if (t instanceof Exception) {
          throw (Exception) t;
        } else {
          throw new RuntimeException("Unexpected exception", t);
        }
      } else {
        if (child != null) {
          child.setErrorTags(ex).finish();
        }
        throw ex;
      }
    }
    
  }
  
  class MetricCB implements Callback<ResolvedQueryFilter, byte[]> {
    final QueryFilter filter;
    
    MetricCB(final QueryFilter filter) {
      this.filter = filter;
    }
    
    @Override
    public ResolvedQueryFilter call(final byte[] uid) throws Exception {
      return new ResolvedMetricLiteralFilter(filter, uid);
    }
    
  }
  
  class TagVCB implements Callback<ResolvedQueryFilter, List<byte[]>> {
    final ResolvedTagValueFilter resolved;
    
    TagVCB(final ResolvedTagValueFilter resolved) {
      this.resolved = resolved;
    }

    @Override
    public ResolvedQueryFilter call(final List<byte[]> uids) throws Exception {
      resolved.setTagValues(uids);
      return resolved;
    }
    
  }
  
  class TagKCB implements Callback<ResolvedQueryFilter, byte[]> {
    final ResolvedTagValueFilter resolved;
    
    TagKCB(final ResolvedTagValueFilter resolved) {
      this.resolved = resolved;
    }

    @Override
    public ResolvedQueryFilter call(final byte[] uid) throws Exception {
      // TODO - null check?
      resolved.setTagKey(uid);
      return resolved;
    }
    
  }
}
