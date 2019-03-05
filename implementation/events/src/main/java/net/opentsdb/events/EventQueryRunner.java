package net.opentsdb.events;

import com.google.common.base.Strings;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.Span;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram
  .ParsedDateHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class EventQueryRunner  extends BaseTSDBPlugin implements
  EventsStorageSchema {

  private static final Logger LOG = LoggerFactory.getLogger(
    EventQueryRunner.class);

  /** The elastic search client to use */
  private ESClient client;

  private TSDB tsdb;

  public static final String TYPE = "EventQueryRunner";

  public static final String QUERY_TIMEOUT_KEY = "es.query_timeout";
  public static final String MAX_RESULTS = "tsd.meta.max.cardinality";


  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    this.tsdb = tsdb;
    client = tsdb.getRegistry().getPlugin(ESClient.class,null);
    if (client == null) {
      throw new IllegalStateException("No client found!");
    }

    if (!tsdb.getConfig().hasProperty(QUERY_TIMEOUT_KEY)) {
      tsdb.getConfig().register(QUERY_TIMEOUT_KEY, 5000, true,
        "How long, in milliseconds, to wait for responses.");
    }
    if (!tsdb.getConfig().hasProperty(MAX_RESULTS)) {
      tsdb.getConfig().register(MAX_RESULTS, 4096, true,
        "The maximum number of event results.");
    }

    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Map<String, EventsStorageResult>> runQuery(BatchEventQuery query, Span
    span) {

    final Span child;
    if (span != null) {
      child = span.newChild(getClass().getSimpleName() + ".runQuery")
        .start();
    } else {
      child = null;
    }

    Map<String, SearchSourceBuilder> search_source_builder =
      EventQueryBuilder.newBuilder(query).build(); // TODO: Build query.

    LOG.info("Running ES Query: " + search_source_builder);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Running ES Query: " + search_source_builder);
    }


    class ResultCB implements Callback<Map<String, EventsStorageResult>,
          Map<String, MultiSearchResponse>> {

      @Override
      public Map<String, EventsStorageResult> call(final Map<String,
        MultiSearchResponse> results) throws Exception {
        //int size = query.to() - query.from();
        //search_source_builder.size(size);
        final Map<String, EventsStorageResult> final_results = new
          LinkedHashMap<>();

        for (int i = 0; i < query.eventQueries().size(); i++) {
          long max_hits = 0;
          EventQuery event_query = query.eventQueries().get(i);
          EventResult result = null;
          for (final Map.Entry<String, MultiSearchResponse> search_response : results.entrySet()) {
            final MultiSearchResponse.Item[] responses = search_response.getValue().getResponses();
            final SearchResponse response = responses[i].getResponse();
            if (response.getHits().getTotalHits() > max_hits) {
              max_hits = response.getHits().getTotalHits();
            }

            if (LOG.isTraceEnabled()) {
              LOG.trace("Got response in " + response.getTook() + "ms");
            }
            long startTime = System.currentTimeMillis();

            if (query.isSummary()) {
              Aggregation event_type = response.getAggregations().get
                ("event_type");

              Map<String, List<Bucket>> event_time_buckets = new HashMap<>();
              List<EventsStorageResult> final_result = new ArrayList<>();
              for (final Terms.Bucket event_bucket : ((ParsedStringTerms) event_type)
                .getBuckets()) {
                List<Bucket> time_buckets = new ArrayList<>();
                Aggregation buckets = event_bucket.getAggregations().get
                  ("events_over_time");
                for (final Histogram.Bucket time_bucket : ((ParsedDateHistogram)
                  buckets)
                  .getBuckets()) {
                  Bucket bucket = new Bucket(time_bucket.getKey().toString(), time_bucket
                    .getDocCount());
                  time_buckets.add(bucket);
                }
                event_time_buckets.put(event_bucket.getKey().toString(),
                  time_buckets);
                result = new EventResult
                  (EventsStorageResult.EventResult.DATA, query, event_query);

              }
              result.addBuckets(event_time_buckets);
            }

            if (LOG.isTraceEnabled()) {
              LOG.trace("Time took to parse out results == " + (System
                .currentTimeMillis() - startTime) + " ms");
            }

            if (result == null) {
              result = new EventResult
                (EventsStorageResult.EventResult
                  .NO_DATA,
                  query, event_query);
            }
            result.setTotalHits(max_hits);
            final_results.put(event_query.namespace(), result);
          }

          if (child != null) {
            child.setSuccessTags()
              .setTag("result", final_results.toString())
              .finish();
          }
        }
        return final_results;
      }

    }

    return client.runQuery(search_source_builder,
      null, child)
      .addCallback(new ResultCB());
      //.addErrback(new ErrorCB());
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public String version() {
    return "3.0.0";
  }
}
