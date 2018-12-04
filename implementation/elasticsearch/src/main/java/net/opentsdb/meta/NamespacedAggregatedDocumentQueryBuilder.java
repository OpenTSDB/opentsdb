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
package net.opentsdb.meta;

import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.TagKeyFilter;
import net.opentsdb.query.filter.TagValueFilter;
import net.opentsdb.query.filter.TagValueLiteralOrFilter;
import net.opentsdb.query.filter.TagValueRegexFilter;
import net.opentsdb.query.filter.TagValueWildcardFilter;
import net.opentsdb.meta.MetaQuery.QueryType;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.AnyFieldRegexFilter;
import net.opentsdb.query.filter.ChainFilter;
import net.opentsdb.query.filter.ChainFilter.FilterOp;
import net.opentsdb.query.filter.MetricFilter;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.filter.MetricRegexFilter;
import net.opentsdb.query.filter.NotFilter;
import net.opentsdb.utils.DateTime;

import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.google.common.collect.Lists;

import java.util.*;

/**
 * Builds the ElasticSearch query
 *
 * @since 3.0
 */
public class NamespacedAggregatedDocumentQueryBuilder {
  public static final String QUERY_NAMESPACE_KEY = "namespace.lowercase";
  public static final String QUERY_TAG_KEY_KEY = "tags.key.lowercase";
  public static final String QUERY_TAG_VALUE_KEY = "tags.value";
  public static final String RESULT_TAG_KEY_KEY = "key.raw";
  public static final String RESULT_TAG_VALUE_KEY = "value.raw";
  public static final String METRIC_PATH = "AM_nested";
  public static final String TAG_PATH = "tags";
  public static final String QUERY_METRIC = "AM_nested.name.lowercase";
  public static final String RESULT_METRIC = "AM_nested.name.raw";
  public static final String RESULT_NAMESPACE = "namespace.raw";
  
  public static final String LAST_SEEN = "lastSeenTime";
  public static final String NAMESPACE_AGG = "ns_agg";
  public static final String METRIC_AGG = "metric_agg";
  public static final String METRIC_UNIQUE = "unique_metrics";
  public static final String TAG_KEY_AGG = "tagk_agg";
  public static final String TAG_KEY_UNIQUE = "unique_tagks";
  public static final String TAG_VALUE_AGG = "tagv_agg";
  public static final String TAG_VALUE_UNIQUE = "unique_tagvs";
  public static final String TAGS_AGG = "tags_agg";
  public static final String TAGS_UNIQUE = "unique_tags";
  public static final String TAGS_SUB_AGG = "tags_sub_agg";
  public static final String TAGS_SUB_UNIQUE = "unique_sub_tags";
  
  public final SearchSourceBuilder search_source_builder;
  
  private final MetaQuery query;
  
  private NamespacedAggregatedDocumentQueryBuilder(final MetaQuery query) {
    search_source_builder = new SearchSourceBuilder();
    this.query = query;
  }
  
  FilterBuilder setFilter(final QueryFilter filter) {
    if (filter == null) {
      return null;
    }
    
    if (filter instanceof MetricFilter) {
      return getMetricFilter((MetricFilter) filter);
    }
    
    if (filter instanceof TagKeyFilter) {
      return getTagKeyFilter((TagKeyFilter) filter, true);
    }
    
    if (filter instanceof TagValueFilter) {
      return getTagValueFilter((TagValueFilter) filter, true);
    }
    
    if (filter instanceof AnyFieldRegexFilter) {
      return getAnyFieldFilter((AnyFieldRegexFilter) filter);
    }
    
    if (filter instanceof NotFilter) {
      return FilterBuilders.notFilter(
          setFilter(((NotFilter) filter).getFilter()));
    }
    
    if (filter instanceof ChainFilter) {
      BoolFilterBuilder builder = FilterBuilders.boolFilter();
      if (((ChainFilter) filter).getOp() == FilterOp.AND) {
        for (final QueryFilter sub_filter : ((ChainFilter) filter).getFilters()) {
          builder.must(setFilter(sub_filter));
        }
      } else {
        for (final QueryFilter sub_filter : ((ChainFilter) filter).getFilters()) {
          builder.should(setFilter(sub_filter));
        }
      }
      return builder;
    }
    
    throw new UnsupportedOperationException("Unsupported filter: " 
        + filter.getClass().toString());
  }
  
  FilterBuilder getMetricFilter(final MetricFilter filter) {
    if (filter instanceof MetricLiteralFilter) {
      return FilterBuilders.nestedFilter(METRIC_PATH, 
          FilterBuilders.boolFilter().must(
              FilterBuilders.termFilter(QUERY_METRIC, 
                  filter.getMetric().toLowerCase())));
    } else if (filter instanceof MetricRegexFilter) {
      return FilterBuilders.nestedFilter(METRIC_PATH, 
          FilterBuilders.boolFilter().must(
              FilterBuilders.regexpFilter(QUERY_METRIC, 
                  convertToLuceneRegex(filter.getMetric()))));
    } else {
      throw new UnsupportedOperationException("Unsupported metric filter: " 
          + filter.getClass().toString());
    }
  }
  
  FilterBuilder getTagValueFilter(final TagValueFilter filter, final boolean nested) {
    if (filter instanceof TagValueLiteralOrFilter) {
      // handles the range filter as well.
      final List<String> lower_case = Lists.newArrayListWithCapacity(
          ((TagValueLiteralOrFilter) filter).literals().size());
      for (final String tag : ((TagValueLiteralOrFilter) filter).literals()) {
        lower_case.add(tag.toLowerCase());
      }
      final FilterBuilder builder = FilterBuilders.boolFilter()
          .must(FilterBuilders.termsFilter(QUERY_TAG_VALUE_KEY, lower_case))
          .must(FilterBuilders.termFilter(QUERY_TAG_KEY_KEY, filter.getTagKey()));
      if (nested) {
        return FilterBuilders.nestedFilter(TAG_PATH, builder);
      }
      return builder;
    } else if (filter instanceof TagValueRegexFilter) {
      final String regexp = convertToLuceneRegex(
          ((TagValueRegexFilter) filter).getFilter());
      final FilterBuilder builder = FilterBuilders.boolFilter()
          .must(FilterBuilders.regexpFilter(QUERY_TAG_VALUE_KEY, regexp))
          .must(FilterBuilders.termFilter(QUERY_TAG_KEY_KEY, filter.getTagKey()));
      if (nested) {
        return FilterBuilders.nestedFilter(TAG_PATH, builder);
      }
      return builder;
    } else if (filter instanceof TagValueWildcardFilter) {
      final FilterBuilder builder = FilterBuilders.boolFilter()
          .must(FilterBuilders.regexpFilter(QUERY_TAG_VALUE_KEY, 
              ((TagValueWildcardFilter) filter).getFilter()
                .toLowerCase().replace("*", ".*")))
          .must(FilterBuilders.termFilter(QUERY_TAG_KEY_KEY, filter.getTagKey()));
      if (nested) {
        return FilterBuilders.nestedFilter(TAG_PATH, builder);
      }
      return builder;
    } else {
      throw new UnsupportedOperationException("Unsupported tag value filter: " 
          + filter.getClass().toString());
    }
  }

  FilterBuilder getTagKeyFilter(final TagKeyFilter filter, final boolean nested) {
    final FilterBuilder builder = FilterBuilders.boolFilter()
        .must(FilterBuilders.regexpFilter(QUERY_TAG_VALUE_KEY, ".*"))
        .must(FilterBuilders.termFilter(QUERY_TAG_KEY_KEY, filter.filter()));
    if (nested) {
      return FilterBuilders.nestedFilter(TAG_PATH, builder);
    }
    return builder;
  }
  
  FilterBuilder getAnyFieldFilter(final AnyFieldRegexFilter filter) {
    final String pattern = convertToLuceneRegex(filter.pattern().toString());
    final BoolFilterBuilder builder = FilterBuilders.boolFilter();
    
    // metric
    builder.should(FilterBuilders.nestedFilter(METRIC_PATH, 
          FilterBuilders.boolFilter()
            .should(FilterBuilders.regexpFilter(RESULT_METRIC, pattern))
            .should(FilterBuilders.regexpFilter(QUERY_METRIC, pattern))));
    
    // tags
    builder.should(FilterBuilders.nestedFilter(TAG_PATH, 
        FilterBuilders.boolFilter()
          .should(FilterBuilders.regexpFilter(QUERY_TAG_KEY_KEY, pattern))
          .should(FilterBuilders.regexpFilter(QUERY_TAG_VALUE_KEY, pattern))));
    // TODO - verify this
    return builder;
  }
  
  AggregationBuilder<?> metricAgg(final QueryFilter filter) {
    return AggregationBuilders.nested(METRIC_AGG)
        .path(METRIC_PATH)
        .subAggregation(AggregationBuilders.terms(METRIC_UNIQUE)
            .field(RESULT_METRIC)
            .size(0)
            .order(query.order() == MetaQuery.Order.ASCENDING ? 
                Order.term(true) : Order.term(false)));
  }
  
  AggregationBuilder<?> tagKeyAgg(final QueryFilter filter) {
    return AggregationBuilders.nested(TAG_KEY_AGG)
        .path(TAG_PATH)
        .subAggregation(AggregationBuilders.terms(TAG_KEY_UNIQUE)
            .field(RESULT_TAG_KEY_KEY)
            .size(0)
            .order(query.order() == MetaQuery.Order.ASCENDING ? 
                Order.term(true) : Order.term(false)));
  }
  
  AggregationBuilder<?> tagValueAgg(final QueryFilter filter) {
    return AggregationBuilders.nested(TAG_VALUE_AGG)
        .path(TAG_PATH)
        .subAggregation(AggregationBuilders.terms(TAG_VALUE_UNIQUE)
            .field(RESULT_TAG_VALUE_KEY)
            .size(0)
            .order(query.order() == MetaQuery.Order.ASCENDING ? 
                Order.term(true) : Order.term(false)));
  }
  
  AggregationBuilder<?> tagKeyAndValueAgg(final QueryFilter filter) {
    // we have to recurse here and find tag key/tag value filters.
    FilterBuilder pair_filter = getTagPairFilter(filter);
    if (pair_filter == null) {
      return null;
    }
    
    return AggregationBuilders.nested(TAGS_AGG)
        .path(TAG_PATH)
        .subAggregation(AggregationBuilders.terms(TAGS_UNIQUE)
            .field(RESULT_TAG_KEY_KEY)
            .size(0)
            .order(query.order() == MetaQuery.Order.ASCENDING ? 
                Order.term(true) : Order.term(false))
            .subAggregation(AggregationBuilders.filter(TAGS_SUB_AGG)
                .filter(pair_filter)
                .subAggregation(AggregationBuilders.terms(TAGS_SUB_UNIQUE)
                    .field(RESULT_TAG_VALUE_KEY)
                    .size(0)
                    .order(query.order() == MetaQuery.Order.ASCENDING ? 
                        Order.term(true) : Order.term(false)))));
  }
  
  FilterBuilder getTagPairFilter(final QueryFilter filter) {
    if (filter == null) {
      return null;
    }
    
    if (filter instanceof TagValueFilter) {
      return getTagValueFilter((TagValueFilter) filter, false);
    }
    
    if (filter instanceof TagKeyFilter) {
      return getTagKeyFilter((TagKeyFilter) filter, false);
    }
    
    if (filter instanceof AnyFieldRegexFilter) {
      return FilterBuilders.boolFilter()
            .must(FilterBuilders.regexpFilter(QUERY_TAG_VALUE_KEY, ".*"))
            .must(FilterBuilders.regexpFilter(QUERY_TAG_KEY_KEY, 
                ((AnyFieldRegexFilter) filter).pattern().toString()));
    }
    
    if (filter instanceof NotFilter) {
      return FilterBuilders.notFilter(
          getTagPairFilter(((NotFilter) filter).getFilter()));
    }
    
    if (filter instanceof ChainFilter) {
      BoolFilterBuilder builder = FilterBuilders.boolFilter();
      // everything has to be a should here.
      for (final QueryFilter sub_filter : ((ChainFilter) filter).getFilters()) {
        final FilterBuilder sub_builder = getTagPairFilter(sub_filter);
        if (sub_builder != null) {
          builder.should(sub_builder);
        }
      }
      return builder;
    }
    
    return null;
  }
  
  public static NamespacedAggregatedDocumentQueryBuilder newBuilder(
      final MetaQuery query) {
    return new NamespacedAggregatedDocumentQueryBuilder(query);
  }
  
  public SearchSourceBuilder build() {
    switch (query.type()) {
    case NAMESPACES:
      search_source_builder.query(FilterBuilders.boolFilter()
          .must(FilterBuilders.regexpFilter(QUERY_NAMESPACE_KEY, 
              convertToLuceneRegex(query.namespace())))
          .buildAsBytes());
      search_source_builder.aggregation(AggregationBuilders.terms(NAMESPACE_AGG)
          .field(RESULT_NAMESPACE)
          .size(0)
          .order(query.order() == MetaQuery.Order.ASCENDING ? 
              Order.term(true) : Order.term(false)));
      search_source_builder.size(0);
      return search_source_builder;
    case METRICS:
      search_source_builder.aggregation(metricAgg(query.filter()));
      search_source_builder.size(0);
      break;
    case TAG_KEYS:
      search_source_builder.aggregation(tagKeyAgg(query.filter()));
      search_source_builder.size(0);
      break;
    case TAG_VALUES:
      search_source_builder.aggregation(tagValueAgg(query.filter()));
      search_source_builder.size(0);
      break;
    case TAG_KEYS_AND_VALUES:
      search_source_builder.aggregation(tagKeyAndValueAgg(query.filter()));
      search_source_builder.size(0);
      break;
    case TIMESERIES:
      search_source_builder.from(query.from());
      search_source_builder.size(query.to() - query.from());
      break;
    default:
      throw new UnsupportedOperationException(query.type() + " not implemented yet.");
    }
    
    if (query.filter() != null || query.start() != null || query.end() != null) {
      if (query.start() != null || query.end() != null) {
        FilterBuilder time_filter;
        if (query.start() != null && query.end() != null) {
          time_filter = FilterBuilders.rangeFilter(LAST_SEEN)
              .from(query.start().msEpoch())
              .to(query.end().msEpoch());
        } else if (query.start() != null) {
          time_filter = FilterBuilders.rangeFilter(LAST_SEEN)
              .from(query.start().msEpoch())
              .to(DateTime.currentTimeMillis());
        } else {
          time_filter = FilterBuilders.rangeFilter(LAST_SEEN)
              .from(0)
              .to(query.end().epoch());
        }
        search_source_builder.query(FilterBuilders.boolFilter()
            .must(time_filter)
            .must(setFilter(query.filter()))
            .buildAsBytes());
      } else {
        search_source_builder.query(setFilter(query.filter()).buildAsBytes());
      }
    }
    return search_source_builder;
  }
  
  static String convertToLuceneRegex(final String value_str) throws
          RuntimeException {
    if (value_str == null || value_str.isEmpty()) {
      throw new IllegalArgumentException("Please provide a valid regex");
    }

    String result = value_str.toLowerCase().trim().replaceAll("\\|", ".*|.*");
    int length = result.length();
    if (result.charAt(0) == '(') {
      result = result.substring(0);
    }
    if (result.charAt(length - 1) == '(') {
      result = result.substring(0, length - 1);
    }

    if (result.startsWith("^")) {
      result = result.substring(1, length);
    } else if (!result.startsWith("~") && 
               !result.startsWith(".*")) {
      result = ".*" + result;
    }
    length = result.length();
    if (result.endsWith("$")) {
      result = result.substring(0, length - 1);
    } else if (!result.startsWith("~") && 
               !result.endsWith(".*")) {
      result = result + ".*";
    }

    return result;
  }
  
}