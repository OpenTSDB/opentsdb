package net.opentsdb.meta;


import net.opentsdb.meta.MetaQuery.AggregationField;
import net.opentsdb.query.filter.*;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.nested.NestedBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ESQuery {
  public static final String QUERY_TAG_KEY_KEY = "tags.key.lowercase";
  public static final String QUERY_TAG_VALUE_KEY = "tags.value";
  public static final String RESULT_TAG_KEY_KEY = "key.raw";
  public static final String RESULT_TAG_VALUE_KEY = "value.raw";
  public static final String QUERY_METRIC = "AM_nested.name.lowercase";
  public static final String RESULT_METRIC = "AM_nested.name.raw";
  private static final Logger LOG = LoggerFactory.getLogger(ESQuery.class);
  public SearchSourceBuilder search_source_builder;

  public ESQuery(ESQueryBuilder builder) {

    this.search_source_builder = builder.search_source_builder;
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
    } else if (!result.startsWith("~")) {
      result = ".*" + result;
    }
    length = result.length();
    if (result.endsWith("$")) {
      result = result.substring(0, length - 1);
    } else if (!result.startsWith("~")) {
      result = result + ".*";
    }

    return result;
  }

  public static ESQueryBuilder newBuilder() {
    return new ESQueryBuilder();
  }


  public static class ESQueryBuilder {
    private FilterBuilder query_filter;

    private List<NestedBuilder> aggregate = new ArrayList<>();

    private SearchSourceBuilder search_source_builder;

    public ESQueryBuilder setQuery_filter(List<QueryFilter> filters) {

      FilterBuilder final_query = FilterBuilders.boolFilter();// .should
      // (metricNestedQuery);

      if (filters != null && filters.size() != 0) {

        for (QueryFilter queryFilter : filters) {
          FilterBuilder shouldBlock = FilterBuilders.boolFilter();
          if (queryFilter instanceof AnyFieldRegexFilter) {
            TagValueFilter filter = (TagValueFilter) queryFilter;
            String tagValue = filter.getFilter();
            ((BoolFilterBuilder) shouldBlock).should(FilterBuilders
                    .nestedFilter("AM_nested", addShouldFilterToBool
                            (FilterBuilders.boolFilter(), QUERY_METRIC,
                                    Arrays.asList(tagValue.toLowerCase()),
                                    "regexp")));
            ((BoolFilterBuilder) shouldBlock).should(FilterBuilders
                    .nestedFilter("tags", addShouldFilterToBool
                            (FilterBuilders.boolFilter(), QUERY_TAG_KEY_KEY,
                                    Arrays.asList(tagValue.toLowerCase()),
                                    "regexp")));
            ((BoolFilterBuilder) shouldBlock).should(FilterBuilders
                    .nestedFilter("tags", addShouldFilterToBool
                            (FilterBuilders.boolFilter(),
                                    QUERY_TAG_VALUE_KEY, Arrays.asList
                                            (tagValue.toLowerCase()),
                                    "regexp")));
          } else if (queryFilter instanceof MetricLiteralFilter) {
            MetricFilter metricFilter = (MetricFilter) queryFilter;
            FilterBuilder metricBoolQuery = FilterBuilders.boolFilter();
            String metric = metricFilter.getMetric();

            BoolFilterBuilder bool_filter_builder = FilterBuilders.boolFilter();
            FilterBuilder terms_query = addShouldFilterToBool
                    (bool_filter_builder, QUERY_METRIC, Arrays.asList(metric
                            .split("\\|")), "literal_or");
            ((BoolFilterBuilder) metricBoolQuery).must(terms_query);

            FilterBuilder tags_nested_query = FilterBuilders.nestedFilter
                    ("AM_nested", metricBoolQuery);

            ((BoolFilterBuilder) shouldBlock).must(tags_nested_query);
          } else if (queryFilter instanceof MetricRegexFilter) {
            MetricFilter metric_filter = (MetricFilter) queryFilter;
            FilterBuilder metric_bool_query = FilterBuilders.boolFilter();
            String metric = metric_filter.getMetric();

            BoolFilterBuilder bool_filter_builder = FilterBuilders.boolFilter();
            FilterBuilder terms_query = addShouldFilterToBool
                    (bool_filter_builder, QUERY_METRIC, Arrays.asList(metric)
                            , "regexp");
            ((BoolFilterBuilder) metric_bool_query).must(terms_query);

            FilterBuilder tags_nested_query = FilterBuilders.nestedFilter
                    ("AM_nested", metric_bool_query);

            ((BoolFilterBuilder) shouldBlock).must(tags_nested_query);
          } else {
            FilterBuilder tags_bool_query = FilterBuilders.boolFilter();
            if (queryFilter instanceof TagValueLiteralOrFilter) {
              TagValueLiteralOrFilter filter = (TagValueLiteralOrFilter)
                      queryFilter;
              String tag_key = filter.getTagKey();
              String tag_value = filter.getFilter();

              BoolFilterBuilder bool_filter_builder = FilterBuilders
                      .boolFilter();
              FilterBuilder terms_query = addMustFilterToBool
                      (bool_filter_builder, QUERY_TAG_KEY_KEY, Arrays.asList
                              (tag_key.toLowerCase()), "literal_or");

              terms_query = addMustFilterToBool(bool_filter_builder,
                      QUERY_TAG_VALUE_KEY, Arrays.asList(tag_value
                              .toLowerCase().split("\\|")), "literal_or");
              ((BoolFilterBuilder) tags_bool_query).must(terms_query);


            } else if (queryFilter instanceof TagValueRegexFilter) {
              TagValueRegexFilter filter = (TagValueRegexFilter) queryFilter;
              String tag_key = filter.getTagKey();
              String tag_value = filter.getFilter();

              BoolFilterBuilder bool_filter_builder = FilterBuilders
                      .boolFilter();
              FilterBuilder terms_query = addMustFilterToBool
                      (bool_filter_builder, QUERY_TAG_KEY_KEY, Arrays.asList
                              (tag_key.toLowerCase()), "literal_or");

              terms_query = addMustFilterToBool(bool_filter_builder,
                      QUERY_TAG_VALUE_KEY, Arrays.asList(tag_value
                              .toLowerCase()), "regexp");
              ((BoolFilterBuilder) tags_bool_query).must(terms_query);
            }


            FilterBuilder tags_nested_query = FilterBuilders.
                    nestedFilter("tags", tags_bool_query);

            ((BoolFilterBuilder) shouldBlock).must(tags_nested_query);
          }
          ((BoolFilterBuilder) final_query).must(shouldBlock);
        }

      }
      this.query_filter = final_query;
      return this;
    }

    public ESQueryBuilder addAggregate(List<QueryFilter> filters,
                                       AggregationField agg_by, String
                                               opt_agg_value) {

      NestedBuilder agg = null;
      FilterBuilder should_filter = null;

      if (agg_by == AggregationField.METRICS) {
        agg = AggregationBuilders.nested("aggs_" + agg_by).path("AM_nested");
        List<String> literal_values = new ArrayList<>();
        List<String> regexp_values = new ArrayList<>();

        for (QueryFilter query_filter : filters) {
          if (query_filter instanceof MetricLiteralFilter) {
            MetricFilter filter = (MetricFilter) query_filter;
            literal_values.addAll(Arrays.asList(filter.getMetric().split
                    ("\\|")));
          } else if (query_filter instanceof AnyFieldRegexFilter) {
            TagValueFilter filter = (TagValueFilter) query_filter;
            regexp_values.addAll(Arrays.asList(filter.getFilter()));
          } else if (query_filter instanceof MetricRegexFilter) {
            MetricFilter filter = (MetricRegexFilter) query_filter;
            regexp_values.addAll(Arrays.asList(filter.getMetric()));
          }
        }

        BoolFilterBuilder bool_filter = FilterBuilders.boolFilter();
        if (literal_values.size() != 0 || regexp_values.size() != 0) {
          if (literal_values.size() != 0) {
            addMustFilterToBool(bool_filter, QUERY_METRIC, literal_values,
                    "literal_or");
          }
          if (regexp_values.size() != 0) {
            should_filter = addMustFilterToBool(bool_filter, QUERY_METRIC,
                    regexp_values, "regexp");
          }


          agg.subAggregation(AggregationBuilders.filter("metrics").filter
                  (should_filter).
                  subAggregation((AggregationBuilders.terms("unique_" +
                          agg_by).field(RESULT_METRIC).size(0))));
        } else {
          agg.subAggregation((AggregationBuilders.terms("unique_" + agg_by)
                  .field(RESULT_METRIC).size(0)));
        }

      } else if (agg_by == MetaQuery.AggregationField.TAGS_KEYS) {
        agg = AggregationBuilders.nested("aggs_" + agg_by).path("tags");
        agg.subAggregation(AggregationBuilders.terms("unique_" + agg_by
                .toString() + "_keys").field(RESULT_TAG_KEY_KEY).size(0));

      } else if (agg_by == MetaQuery.AggregationField.TAGS_VALUES) {

        agg = AggregationBuilders.nested("aggs_" + agg_by).path("tags");
        Map<String, List<String>> values = new HashMap<>();

        for (QueryFilter queryFilter : filters) {
          if (queryFilter instanceof TagValueFilter) {
            TagValueFilter filter = (TagValueFilter) queryFilter;
            if (opt_agg_value == null) {
              if (filter instanceof AnyFieldRegexFilter) {
                values.computeIfAbsent("*", (k -> new ArrayList<>())).add
                        (filter.getFilter());
              } else {
                values.computeIfAbsent(filter.getTagKey(), (k -> new
                        ArrayList<>())).add(filter.getFilter());
              }
            } else if (filter.getTagKey().equalsIgnoreCase(opt_agg_value)) {
              values.computeIfAbsent(filter.getTagKey(), (k -> new
                      ArrayList<>())).add(filter.getFilter());
            }
          }
        }

        FilterBuilder tag_value_should_filter = FilterBuilders.boolFilter();
        FilterBuilder tag_value_must_filter = null;
        if (values.size() == 0) {
          if (opt_agg_value != null) {
            values.put(opt_agg_value.toLowerCase(), Arrays.asList(".*"));
          } else {
            values.put("*", Arrays.asList(".*"));
          }
        }
        for (Map.Entry<String, List<String>> entry : values.entrySet()) {
          BoolFilterBuilder mustFilterBuilder = FilterBuilders.boolFilter();
          tag_value_must_filter = addMustFilterToBool(mustFilterBuilder,
                  QUERY_TAG_VALUE_KEY, entry.getValue(), "regexp");
          tag_value_must_filter = addMustFilterToBool(mustFilterBuilder,
                  QUERY_TAG_KEY_KEY, Arrays.asList(entry.getKey()), "regexp");
          ((BoolFilterBuilder) tag_value_should_filter).should
                  (tag_value_must_filter);
        }

        if (opt_agg_value != null) {
          should_filter = addShouldFilterToBool(FilterBuilders.boolFilter(),
                  QUERY_TAG_KEY_KEY, Arrays.asList(opt_agg_value),
                  "literal_or");

          agg.
                  subAggregation(AggregationBuilders.filter("tag_keys")
                          .filter(should_filter).
                          subAggregation(AggregationBuilders.terms("unique_"
                                  + agg_by.toString() + "_keys").field
                                  (RESULT_TAG_KEY_KEY).size(0).
                                  subAggregation(AggregationBuilders.filter
                                          ("tag_values").filter
                                          (tag_value_should_filter).
                                          subAggregation(AggregationBuilders
                                                  .terms("unique_" + agg_by
                                                          .toString() +
                                                          "_values").field
                                                          (RESULT_TAG_VALUE_KEY).size(0)))));
        }
// else if (values != null && values.size() > 0) {
//          agg.
//                  //   subAggregation(AggregationBuilders.filter
// ("tag_keys").filter(shouldFilter).
//                          subAggregation(AggregationBuilders.terms
// ("unique_" + aggBy.toString() + "_keys").field(RESULT_TAG_KEY_KEY).size(0).
//                          subAggregation(AggregationBuilders.filter
// ("tag_values").filter(tagValueShouldFilter).
//                                  subAggregation(AggregationBuilders.terms
// ("unique_" + aggBy.toString() + "_values").field(RESULT_TAG_VALUE_KEY)
// .size(0))));
//        }
        else {
          agg.
                  //   subAggregation(AggregationBuilders.filter("tag_keys")
                  // .filter(shouldFilter).
                          subAggregation(AggregationBuilders.terms("unique_"
                          + agg_by.toString() + "_keys").field
                          (RESULT_TAG_KEY_KEY).size(0).
                          subAggregation(AggregationBuilders.filter
                                  ("tag_values").filter
                                  (tag_value_should_filter).
                                  subAggregation(AggregationBuilders.terms
                                          ("unique_" + agg_by.toString() +
                                                  "_values").field
                                          (RESULT_TAG_VALUE_KEY).size(0))));
        }
      }

      aggregate.add(agg);
      return this;
    }

    public SearchSourceBuilder build() {
      SearchSourceBuilder search_source_builder = new SearchSourceBuilder();
      search_source_builder.query(query_filter.toString()); //.aggregation
      // (query.getAggs("system.cpu.busy.pct", "AM_nested.name"));
      for (NestedBuilder each_aggregate : aggregate) {
        search_source_builder.aggregation(each_aggregate);
      }
      return search_source_builder;

    }


    private FilterBuilder addMustFilterToBool(BoolFilterBuilder boolFilter,
                                              String key, List<String>
                                                      values, String op) {

      BoolFilterBuilder filter = boolFilter;
      if (op.equalsIgnoreCase("literal_or")) {
        filter.must(FilterBuilders.termsFilter(key, values));
      } else if (op.equalsIgnoreCase("regexp")) {
        FilterBuilder[] regexp_filter_builders = new FilterBuilder[values
                .size()];
        for (int i = 0; i < values.size(); i++) {
          regexp_filter_builders[i] = FilterBuilders.regexpFilter(key,
                  convertToLuceneRegex(values.get(i)));
        }
        filter.must(regexp_filter_builders);
      }

      return filter;
    }

    private FilterBuilder addShouldFilterToBool(BoolFilterBuilder boolFilter,
                                                String key, List<String>
                                                        values, String op) {
      BoolFilterBuilder filter = boolFilter;
      if (op.equalsIgnoreCase("literal_or")) {
        filter.should(FilterBuilders.termsFilter(key, values));
      } else if (op.equalsIgnoreCase("regexp")) {
        FilterBuilder[] regexp_filter_builders = new FilterBuilder[values
                .size()];
        for (int i = 0; i < values.size(); i++) {
          regexp_filter_builders[i] = FilterBuilders.regexpFilter(key,
                  convertToLuceneRegex(values.get(i)));
        }
        filter.should(regexp_filter_builders);
      }

      return filter;
    }

  }

  /*
  public static void main(String[] args) throws IOException {
//      //  ESQuery.eval("(A&B)");
//        ESQuery query = new ESQuery();
//        Map<String, List<String>> tags = new HashMap<String, List<String>>();
//        tags.put("host", Arrays.asList("system.cpu.busy.pct"));
//        tags.put("hostgroup", Arrays.asList("system.cpu.busy.pct"));
//        FilterBuilder esquery = query.formQuery("system.cpu.busy.pct", tags);


    List<QueryFilter> filters = new ArrayList<>();
    TagValueFilter filter1 = AnyFieldRegexFilter.newBuilder()
            .setFilter(".*bf.*|.*gq.*").build();
    filters.add((QueryFilter) filter1);
//    TagValueFilter filter2 = new TagValueLiteralOrFilter
(TagValueLiteralOrFilter.newBuilder()
//            .setTagKey("colo")
//            .setFilter("bf|gq")
//    );
//    //AnyFieldRegexFilter.newBuilder().setFilter("abc").build();
//    filters.add((QueryFilter) filter2);
//    TagValueFilter filter3 = new TagValueLiteralOrFilter
(TagValueLiteralOrFilter.newBuilder()
//            .setTagKey("writeType")
//            .setFilter("newReplace")
//    );
//    filters.add((QueryFilter) filter3);
//    MetricLiteralFilter filter4 = new MetricLiteralFilter
(MetricLiteralFilter.newBuilder()
//            .setMetric("spikebuster.newcardinality")
//
//    );
//    filters.add((QueryFilter) filter4);
//        TagVFilter filter2 = new TagVLiteralOrFilter("*", ".*kafka.*");
//        filters.add(filter2);

    SearchSourceBuilder query = ESQuery.newBuilder().setQuery_filter(filters)
    .addAggregate(filters, MetaQuery.AggregationField.METRICS, null).build();
    System.out.println(query);

//    SearchRequest searchRequest = new SearchRequest("yamas");
//
//    searchRequest.source(search_source_builder);
//
//
//    ActionFuture<SearchResponse> searchResponse = client.search
(searchRequest);
//    System.out.println(searchResponse.actionGet());


  }
*/
}
