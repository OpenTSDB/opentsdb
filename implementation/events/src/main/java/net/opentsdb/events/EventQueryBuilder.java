package net.opentsdb.events;

import com.google.common.collect.Lists;
import net.opentsdb.query.filter.*;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram
  .DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventQueryBuilder {


  public final Map<String, SearchSourceBuilder> search_source_builders;
  private final BatchEventQuery query;

  private static final String EVENT_TYPE_AGG = "event_type";
  private static final String TIME_AGG = "events_over_time";
  private static final String START_TIME_FIELD = "startTimestamp";


  public static final String TAG_PATH = "tags";
  public static final String QUERY_TAG_KEY_KEY = "tags.key.lowercase";
  public static final String QUERY_TAG_VALUE_KEY = "tags.value";
  public static final String RESULT_TAG_KEY_KEY = "key.raw";
  public static final String RESULT_TAG_VALUE_KEY = "value.raw";
  private static final String RESULT_EVENT_TYPE_FIELD = "eventType.raw";
  private static final String QUERY_EVENT_TYPE_FIELD = "eventType.lowercase";



  private EventQueryBuilder(final BatchEventQuery query) {
    this.search_source_builders = new HashMap<>();
    this.query = query;
  }

  public static EventQueryBuilder newBuilder(
    final BatchEventQuery query) {
    return new EventQueryBuilder(query);
  }



  QueryBuilder setFilter(final QueryFilter filter) {
    if (filter == null) {
      return null;
    }

//    if (filter instanceof ExplicitTagsFilter) {
//      return setFilter(((ExplicitTagsFilter) filter).getFilter());
//    }
//
//    if (filter instanceof MetricFilter) {
//      return getMetricFilter((MetricFilter) filter, true);
//    }
//
//    if (filter instanceof TagKeyFilter) {
//      return getTagKeyFilter((TagKeyFilter) filter, true);
//    }
//
//    if (filter instanceof TagValueFilter) {
//      num_tags++;
//      return getTagValueFilter((TagValueFilter) filter, true);
//    }
//
//    if (filter instanceof AnyFieldRegexFilter) {
//      return getAnyFieldFilter((AnyFieldRegexFilter) filter);
//    }
//
//    if (filter instanceof NotFilter) {
//      return QueryBuilders.boolQuery().mustNot(
//        setFilter(((NotFilter) filter).getFilter()));
//    }

    if (filter instanceof EventLiteralOrFilter) {
      return getEventFilter((EventLiteralOrFilter) filter);
    }

    if (filter instanceof ChainFilter) {
      BoolQueryBuilder builder = QueryBuilders.boolQuery();
      if (((ChainFilter) filter).getOp() == ChainFilter.FilterOp.AND) {
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

  public QueryBuilder getEventFilter(EventLiteralOrFilter filter) {
    QueryBuilder builder =  QueryBuilders.boolQuery().must(
      QueryBuilders.termQuery(filter.getField(),
        filter.getFilter()));
    return builder;
  }

  QueryBuilder getTagValueFilter(final TagValueFilter filter, final boolean nested) {
    if (filter instanceof TagValueLiteralOrFilter) {
      // handles the range filter as well.
      final List<String> lower_case = Lists.newArrayListWithCapacity(
        ((TagValueLiteralOrFilter) filter).literals().size());
      for (final String tag : ((TagValueLiteralOrFilter) filter).literals()) {
        lower_case.add(tag.toLowerCase());
      }
      final QueryBuilder builder = QueryBuilders.boolQuery()
        .must(QueryBuilders.termsQuery(QUERY_TAG_VALUE_KEY, lower_case))
        .must(QueryBuilders.termQuery(QUERY_TAG_KEY_KEY, filter.getTagKey
          ().toLowerCase()));
      if (nested) {
        return QueryBuilders.nestedQuery(TAG_PATH, builder, ScoreMode.None);
      }
      return builder;
    } else if (filter instanceof TagValueRegexFilter) {
      final String regexp = convertToLuceneRegex(
        ((TagValueRegexFilter) filter).getFilter());
      final QueryBuilder builder = QueryBuilders.boolQuery()
        .must(QueryBuilders.regexpQuery(QUERY_TAG_VALUE_KEY, regexp))
        .must(QueryBuilders.termQuery(QUERY_TAG_KEY_KEY, filter.getTagKey
          ().toLowerCase()));
      if (nested) {
        return QueryBuilders.nestedQuery(TAG_PATH, builder, ScoreMode.None);
      }
      return builder;
    } else if (filter instanceof TagValueWildcardFilter) {
      final QueryBuilder builder = QueryBuilders.boolQuery()
        .must(QueryBuilders.regexpQuery(QUERY_TAG_VALUE_KEY,
          ((TagValueWildcardFilter) filter).getFilter()
            .toLowerCase().replace("*", ".*")))
        .must(QueryBuilders.termQuery(QUERY_TAG_KEY_KEY, filter.getTagKey
          ().toLowerCase()));
      if (nested) {
        return QueryBuilders.nestedQuery(TAG_PATH, builder, ScoreMode.None);
      }
      return builder;
    } else {
      throw new UnsupportedOperationException("Unsupported tag value filter: "
        + filter.getClass().toString());
    }
  }

  AggregationBuilder bucketsAgg(final QueryFilter filter, final String interval) {

    DateHistogramInterval dateHistogramInterval = null;
    if (interval.endsWith("d")) {
      dateHistogramInterval = DateHistogramInterval
        .days(Integer.valueOf(interval.substring(0, interval.length() -
          1)));
    } else if (interval.endsWith("h")) {
      dateHistogramInterval = DateHistogramInterval
        .hours(Integer.valueOf(interval.substring(0, interval.length() -
          1)));
    } else if (interval.endsWith("m")) {
      dateHistogramInterval = DateHistogramInterval
        .hours(Integer.valueOf(interval.substring(0, interval.length() -
          1)));
    } else {
      throw new IllegalArgumentException("Please set valid interval");
    }
    System.out.println("HERE");


    DateHistogramAggregationBuilder agg
    = AggregationBuilders.dateHistogram(TIME_AGG)
      .field(START_TIME_FIELD)
      .dateHistogramInterval(dateHistogramInterval);

//    AggregationBuilders.dateHistogram("test")
//      .field("dateOfBirth")
//      .dateHistogramInterval(DateHistogramInterval.YEAR);

//    AggregationBuilder aggregation =
//      AggregationBuilders
//        .dateHistogram("agg")
//        .field("dateOfBirth")
//        .dateHistogramInterval(DateHistogramInterval.YEAR);

    System.out.println("HERE1" );

   // return null;

    return AggregationBuilders.terms(EVENT_TYPE_AGG).
      field(RESULT_EVENT_TYPE_FIELD)
      .subAggregation(agg);
        //.size(0));
  }


  public Map<String, SearchSourceBuilder> build() {
    for (final EventQuery event_query : query.eventQueries()) {
      SearchSourceBuilder search_source_builder = new SearchSourceBuilder();
      if (query.isSummary()) {
        System.out.println("HERE123");
          search_source_builder.aggregation(bucketsAgg(event_query.filter(),
            query.interval()));
          search_source_builder.size(0);
      } else {
        search_source_builder.from(query.from());
        search_source_builder.size(query.to() - query.from());
      }



    if (event_query.filter() != null) {
      search_source_builder.query(setFilter(event_query.filter()));
    }
    search_source_builders.put(event_query.namespace().toLowerCase(),
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
