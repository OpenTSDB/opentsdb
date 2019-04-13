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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import net.opentsdb.query.filter.AnyFieldRegexFilter;
import net.opentsdb.query.filter.ChainFilter;
import net.opentsdb.query.filter.ChainFilter.FilterOp;
import net.opentsdb.query.filter.ExplicitTagsFilter;
import net.opentsdb.query.filter.MetricFilter;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.filter.MetricRegexFilter;
import net.opentsdb.query.filter.NotFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.TagKeyFilter;
import net.opentsdb.query.filter.TagKeyLiteralOrFilter;
import net.opentsdb.query.filter.TagKeyRegexFilter;
import net.opentsdb.query.filter.TagValueFilter;
import net.opentsdb.query.filter.TagValueLiteralOrFilter;
import net.opentsdb.query.filter.TagValueRegexFilter;
import net.opentsdb.query.filter.TagValueWildcardFilter;
import net.opentsdb.utils.DateTime;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

  public final Map<String, SearchSourceBuilder> search_source_builders;

  private final BatchMetaQuery query;

  private int num_tags;

  private NamespacedAggregatedDocumentQueryBuilder(final BatchMetaQuery query) {
    this.search_source_builders = new LinkedHashMap<>();
    this.query = query;
  }

  FilterBuilder setFilter(final QueryFilter filter) {
    if (filter == null) {
      return null;
    }

    if (filter instanceof ExplicitTagsFilter) {
      return setFilter(((ExplicitTagsFilter) filter).getFilter());
    }

    if (filter instanceof MetricFilter) {
      return getMetricFilter((MetricFilter) filter, true);
    }

    if (filter instanceof TagKeyFilter) {
      return getTagKeyFilter((TagKeyFilter) filter, true);
    }

    if (filter instanceof TagValueFilter) {
      num_tags++;
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

  FilterBuilder getMetricFilter(final MetricFilter filter, final boolean nested) {
    if (filter instanceof MetricLiteralFilter) {
      String metric = filter.getMetric().toLowerCase();
      String[] metric_literals = metric.split("\\|");
      FilterBuilder builder =  FilterBuilders.boolFilter().must(
              FilterBuilders.termsFilter(QUERY_METRIC,
                  metric_literals));
      if (nested) {
        return FilterBuilders.nestedFilter(METRIC_PATH, builder);
      }
      return builder;
    } else if (filter instanceof MetricRegexFilter) {
      FilterBuilder builder = FilterBuilders.boolFilter().must(
              FilterBuilders.regexpFilter(QUERY_METRIC,
                      convertToLuceneRegex(filter.getMetric())));
      if (nested) {
        return FilterBuilders.nestedFilter(METRIC_PATH, builder);
      }
      return builder;
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
          .must(FilterBuilders.termFilter(QUERY_TAG_KEY_KEY, filter.getTagKey
            ().toLowerCase()));
      if (nested) {
        return FilterBuilders.nestedFilter(TAG_PATH, builder);
      }
      return builder;
    } else if (filter instanceof TagValueRegexFilter) {
      final String regexp = convertToLuceneRegex(
          ((TagValueRegexFilter) filter).getFilter());
      final FilterBuilder builder = FilterBuilders.boolFilter()
          .must(FilterBuilders.regexpFilter(QUERY_TAG_VALUE_KEY, regexp))
          .must(FilterBuilders.termFilter(QUERY_TAG_KEY_KEY, filter.getTagKey
            ().toLowerCase()));
      if (nested) {
        return FilterBuilders.nestedFilter(TAG_PATH, builder);
      }
      return builder;
    } else if (filter instanceof TagValueWildcardFilter) {
      final FilterBuilder builder = FilterBuilders.boolFilter()
          .must(FilterBuilders.regexpFilter(QUERY_TAG_VALUE_KEY,
              ((TagValueWildcardFilter) filter).getFilter()
                .toLowerCase().replace("*", ".*")))
          .must(FilterBuilders.termFilter(QUERY_TAG_KEY_KEY, filter.getTagKey
            ().toLowerCase()));
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
    final FilterBuilder builder = FilterBuilders.boolFilter();
    if (filter instanceof TagKeyLiteralOrFilter) {
      String filter_str = filter.filter().toLowerCase();
      String[] filter_literals = filter_str.split("\\|");
      ((BoolFilterBuilder) builder).must(FilterBuilders.regexpFilter
              (QUERY_TAG_VALUE_KEY, ".*"))
              .must(FilterBuilders.termsFilter(QUERY_TAG_KEY_KEY, filter_literals));

    } else if (filter instanceof TagKeyRegexFilter) {
      ((BoolFilterBuilder) builder).must(FilterBuilders.regexpFilter(QUERY_TAG_VALUE_KEY, ".*"))
              .must(FilterBuilders.regexpFilter(QUERY_TAG_KEY_KEY, convertToLuceneRegex(filter
                .filter())));
    }
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

  AggregationBuilder<?> metricAgg(final QueryFilter filter, final int size) {
    if (filter instanceof ExplicitTagsFilter) {
      return metricAgg(((ExplicitTagsFilter) filter).getFilter(), size);
    }
    ChainFilter.Builder metric_only_filter = ChainFilter.newBuilder();
    if (filter instanceof ChainFilter) {

      for (final QueryFilter sub_filter : ((ChainFilter) filter).getFilters()) {
        if (sub_filter instanceof MetricFilter) {
          metric_only_filter.addFilter(sub_filter);
        }
      }
    }
    FilterBuilder pair_filter = getTagPairFilter(metric_only_filter.build(),
            false);
    return AggregationBuilders.nested(METRIC_AGG)
        .path(METRIC_PATH)
        .subAggregation(AggregationBuilders.filter(METRIC_AGG)
                .filter(pair_filter)
                .subAggregation(AggregationBuilders.terms(METRIC_UNIQUE)
            .field(RESULT_METRIC)
            .size(size)
            .order(query.order() == BatchMetaQuery.Order.ASCENDING ?
                Order.term(true) : Order.term(false))));
  }

  AggregationBuilder<?> tagKeyAgg(final QueryFilter filter, final int size) {
    if (filter instanceof ExplicitTagsFilter) {
      return tagKeyAgg(((ExplicitTagsFilter) filter).getFilter(), size);
    }
    ChainFilter.Builder tags_filters = ChainFilter.newBuilder();
    if (filter instanceof ChainFilter) {
      for (final QueryFilter sub_filter : ((ChainFilter) filter).getFilters()) {
        if (sub_filter instanceof TagKeyFilter) {
            tags_filters.addFilter(sub_filter);
        }
      }
    }

    FilterBuilder pair_filter = getTagPairFilter(tags_filters.build(), true);
    if (pair_filter == null) {
      return null;
    }

    return AggregationBuilders.nested(TAG_KEY_AGG)
        .path(TAG_PATH)
        .subAggregation(AggregationBuilders.filter(TAG_KEY_UNIQUE)
                .filter(pair_filter)
                .subAggregation(AggregationBuilders.terms(TAG_KEY_UNIQUE)
                        .field(RESULT_TAG_KEY_KEY)
                        .size(size)
                        .order(query.order() == BatchMetaQuery.Order.ASCENDING ?
                                Order.term(true) : Order.term(false))));
  }

  AggregationBuilder<?> tagValueAgg(final QueryFilter filter, int size) {
    if (filter instanceof ExplicitTagsFilter) {
      return tagValueAgg(((ExplicitTagsFilter) filter).getFilter(), size);
    }
    return AggregationBuilders.nested(TAG_VALUE_AGG)
        .path(TAG_PATH)
        .subAggregation(AggregationBuilders.terms(TAG_VALUE_UNIQUE)
            .field(RESULT_TAG_VALUE_KEY)
            .size(size)
            .order(query.order() == BatchMetaQuery.Order.ASCENDING ?
                Order.term(true) : Order.term(false)));
  }

  AggregationBuilder<?> tagKeyAndValueAgg(final QueryFilter filter, final String
          field, final int size) {
    if (filter instanceof ExplicitTagsFilter) {
      return tagKeyAndValueAgg(((ExplicitTagsFilter) filter).getFilter(),
              field, size);
    }
    ChainFilter.Builder tags_filters = ChainFilter.newBuilder();

    if (filter instanceof ChainFilter) {
      for (final QueryFilter sub_filter : ((ChainFilter) filter).getFilters()) {
        if (sub_filter instanceof TagValueFilter) {
          if (Strings.isNullOrEmpty(field) || field.equalsIgnoreCase
                  (((TagValueFilter) sub_filter).getTagKey())) {
            tags_filters.addFilter(sub_filter);
          }
        }
        if (sub_filter instanceof TagKeyFilter) {
          if (Strings.isNullOrEmpty(field) || field.equalsIgnoreCase
                  (((TagKeyFilter) sub_filter).filter())) {
            tags_filters.addFilter(sub_filter);
          }
        }
      }
    }

    if (tags_filters.filters() == null || tags_filters.filters().size() == 0) {
      tags_filters.addFilter(TagValueWildcardFilter.newBuilder().setTagKey
              (field).setFilter(".*").build());
    }
    // we have to recurse here and find tag key/tag value filters.

    FilterBuilder pair_filter = getTagPairFilter(tags_filters.build(), true);
    if (pair_filter == null) {
      return null;
    }

    return AggregationBuilders.nested(TAGS_AGG)
        .path(TAG_PATH)
        .subAggregation(AggregationBuilders.filter(TAGS_UNIQUE)
                .filter(pair_filter)
                .subAggregation(AggregationBuilders.terms(TAGS_UNIQUE)
            .field(RESULT_TAG_KEY_KEY)
            .size(0)
            .order(query.order() == BatchMetaQuery.Order.ASCENDING ?
                Order.term(true) : Order.term(false))
            .subAggregation(AggregationBuilders.filter(TAGS_SUB_AGG)
                .filter(pair_filter)
                .subAggregation(AggregationBuilders.terms(TAGS_SUB_UNIQUE)
                    .field(RESULT_TAG_VALUE_KEY)
                    .size(size)
                    .order(query.order() == BatchMetaQuery.Order.ASCENDING ?
                        Order.term(true) : Order.term(false))))));
  }

  FilterBuilder getTagPairFilter(final QueryFilter filter, final boolean
          use_must) {
    if (filter == null) {
      return null;
    }

    if (filter instanceof MetricFilter) {
      return getMetricFilter((MetricFilter) filter, false);
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
      return FilterBuilders.boolFilter().mustNot(
          getTagPairFilter(((NotFilter) filter).getFilter(), use_must));
    }

    if (filter instanceof ChainFilter) {
      BoolFilterBuilder builder = FilterBuilders.boolFilter();
      // Metrics are should, tags_key_and_value is a must filter
      for (final QueryFilter sub_filter : ((ChainFilter) filter).getFilters()) {
        final FilterBuilder sub_builder = getTagPairFilter(sub_filter, use_must);
        if (sub_builder != null) {
          if (use_must) {
            builder.must(sub_builder);
          } else {
            builder.should(sub_builder);
          }
        }
      }
      return builder;
    }

    return null;
  }

  public static NamespacedAggregatedDocumentQueryBuilder newBuilder(
      final BatchMetaQuery query) {
    return new NamespacedAggregatedDocumentQueryBuilder(query);
  }

  public Map<String, SearchSourceBuilder> build() {
    for (final MetaQuery meta_query : query.metaQueries()) {
      SearchSourceBuilder search_source_builder = new SearchSourceBuilder();
      switch (query.type()) {
        case NAMESPACES:
          search_source_builder.query(FilterBuilders.boolFilter()
            .must(FilterBuilders.regexpFilter(QUERY_NAMESPACE_KEY,
              convertToLuceneRegex(meta_query.namespace())))
            .buildAsBytes());
          search_source_builder.aggregation(AggregationBuilders.terms(NAMESPACE_AGG)

            .field(RESULT_NAMESPACE)
            .size(0)
            .order(query.order() == BatchMetaQuery.Order.ASCENDING ?
              Order.term(true) : Order.term(false)));
          search_source_builder.size(0);
          search_source_builders.put("all_namespace", search_source_builder);
          return search_source_builders;
        case METRICS:
          search_source_builder.aggregation(metricAgg(meta_query.filter(), query.aggregationSize()));
          search_source_builder.size(0);
          break;
        case TAG_KEYS:
          search_source_builder.aggregation(tagKeyAgg(meta_query.filter(), query.aggregationSize()));
          search_source_builder.size(0);
          break;
        case TAG_VALUES:
          search_source_builder.aggregation(tagValueAgg(meta_query.filter(), query.aggregationSize()));
          search_source_builder.size(0);
          break;
        case TAG_KEYS_AND_VALUES:
          search_source_builder.aggregation(tagKeyAndValueAgg(meta_query.filter(),
            query.aggregationField().toLowerCase(), query.aggregationSize()));
          search_source_builder.size(0);
          break;
        case TIMESERIES:
          search_source_builder.from(query.from());
          search_source_builder.size(query.to() - query.from());
          break;
        default:
          throw new UnsupportedOperationException(query.type() + " not implemented yet.");
      }


      if (meta_query.filter() != null || query.start() != null || query.end() !=
        null) {
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
            .must(setFilter(meta_query.filter()))
            .buildAsBytes());
        } else if (meta_query.filter() instanceof ExplicitTagsFilter) {
          FilterBuilder filters = setFilter(meta_query.filter());
          FilterBuilder explicit_filter = FilterBuilders.termFilter
            ("tags_value", num_tags);
          search_source_builder.query(FilterBuilders.boolFilter()
            .must(explicit_filter)
            .must(filters)
            .buildAsBytes());
        } else {
          search_source_builder.query(setFilter(meta_query.filter()).buildAsBytes());
        }
      }
      search_source_builders.put(meta_query.namespace().toLowerCase(),
        search_source_builder);
    }
    return search_source_builders;
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