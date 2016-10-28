// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.hbase.async.HBaseException;
import org.hbase.async.RpcTimedOutException;
import org.hbase.async.Bytes.ByteMap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.core.DataPoints;
import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.Query;
import net.opentsdb.core.QueryException;
import net.opentsdb.core.RateOptions;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import net.opentsdb.core.Tags;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSUIDQuery;
import net.opentsdb.query.expression.ExpressionTree;
import net.opentsdb.query.expression.Expressions;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

/**
 * Handles queries for timeseries datapoints. Each request is parsed into a
 * TSQuery object, the values given validated, and if all tests pass, the
 * query is converted into TsdbQueries and each one is executed to fetch the
 * data. The resulting DataPoints[] are then passed to serializers for 
 * formatting.
 * <p>
 * Some private methods are included for parsing query string data into a 
 * TSQuery object.
 * @since 2.0
 */
final class QueryRpc implements HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(QueryRpc.class);
  
  /** Various counters and metrics for reporting query stats */
  static final AtomicLong query_invalid = new AtomicLong();
  static final AtomicLong query_exceptions = new AtomicLong();
  static final AtomicLong query_success = new AtomicLong();
  
  /**
   * Implements the /api/query endpoint to fetch data from OpenTSDB.
   * @param tsdb The TSDB to use for fetching data
   * @param query The HTTP query for parsing and responding
   */
  @Override
  public void execute(final TSDB tsdb, final HttpQuery query) 
    throws IOException {
    
    // only accept GET/POST/DELETE
    if (query.method() != HttpMethod.GET && query.method() != HttpMethod.POST &&
        query.method() != HttpMethod.DELETE) {
      throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED, 
          "Method not allowed", "The HTTP method [" + query.method().getName() +
          "] is not permitted for this endpoint");
    }
    if (query.method() == HttpMethod.DELETE && 
        !tsdb.getConfig().getBoolean("tsd.http.query.allow_delete")) {
      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
               "Bad request",
               "Deleting data is not enabled (tsd.http.query.allow_delete=false)");
    }
    
    final String[] uri = query.explodeAPIPath();
    final String endpoint = uri.length > 1 ? uri[1] : ""; 
    
    if (endpoint.toLowerCase().equals("last")) {
      handleLastDataPointQuery(tsdb, query);
    } else if (endpoint.toLowerCase().equals("gexp")){
      handleQuery(tsdb, query, true);
    } else if (endpoint.toLowerCase().equals("exp")) {
      handleExpressionQuery(tsdb, query);
      return;
    } else {
      handleQuery(tsdb, query, false);
    }
  }

  /**
   * Processing for a data point query
   * @param tsdb The TSDB to which we belong
   * @param query The HTTP query to parse/respond
   * @param allow_expressions Whether or not expressions should be parsed
   * (based on the endpoint)
   */
  private void handleQuery(final TSDB tsdb, final HttpQuery query, 
      final boolean allow_expressions) {
    final long start = DateTime.currentTimeMillis();
    final TSQuery data_query;
    final List<ExpressionTree> expressions;
    if (query.method() == HttpMethod.POST) {
      switch (query.apiVersion()) {
      case 0:
      case 1:
        data_query = query.serializer().parseQueryV1();
        break;
      default:
        query_invalid.incrementAndGet();
        throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
            "Requested API version not implemented", "Version " + 
            query.apiVersion() + " is not implemented");
      }
      expressions = null;
    } else {
      expressions = new ArrayList<ExpressionTree>();
      data_query = parseQuery(tsdb, query, expressions);
    }
    
    if (query.getAPIMethod() == HttpMethod.DELETE &&
        tsdb.getConfig().getBoolean("tsd.http.query.allow_delete")) {
      data_query.setDelete(true);
    }
    
    // validate and then compile the queries
    try {
      LOG.debug(data_query.toString());
      data_query.validateAndSetQuery();
    } catch (Exception e) {
      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
          e.getMessage(), data_query.toString(), e);
    }
    
    // if the user tried this query multiple times from the same IP and src port
    // they'll be rejected on subsequent calls
    final QueryStats query_stats = 
        new QueryStats(query.getRemoteAddress(), data_query, 
            query.getPrintableHeaders());
    data_query.setQueryStats(query_stats);
    query.setStats(query_stats);
    
    final int nqueries = data_query.getQueries().size();
    final ArrayList<DataPoints[]> results = new ArrayList<DataPoints[]>(nqueries);
    final List<Annotation> globals = new ArrayList<Annotation>();
    
    /** This has to be attached to callbacks or we may never respond to clients */
    class ErrorCB implements Callback<Object, Exception> {
      public Object call(final Exception e) throws Exception {
        Throwable ex = e;
        try {
          LOG.error("Query exception: ", e);
          if (ex instanceof DeferredGroupException) {
            ex = e.getCause();
            while (ex != null && ex instanceof DeferredGroupException) {
              ex = ex.getCause();
            }
            if (ex == null) {
              LOG.error("The deferred group exception didn't have a cause???");
            }
          } 

          if (ex instanceof RpcTimedOutException) {
            query_stats.markSerialized(HttpResponseStatus.REQUEST_TIMEOUT, ex);
            query.badRequest(new BadRequestException(
                HttpResponseStatus.REQUEST_TIMEOUT, ex.getMessage()));
            query_exceptions.incrementAndGet();
          } else if (ex instanceof HBaseException) {
            query_stats.markSerialized(HttpResponseStatus.FAILED_DEPENDENCY, ex);
            query.badRequest(new BadRequestException(
                HttpResponseStatus.FAILED_DEPENDENCY, ex.getMessage()));
            query_exceptions.incrementAndGet();
          } else if (ex instanceof QueryException) {
            query_stats.markSerialized(((QueryException)ex).getStatus(), ex);
            query.badRequest(new BadRequestException(
                ((QueryException)ex).getStatus(), ex.getMessage()));
            query_exceptions.incrementAndGet();
          } else if (ex instanceof BadRequestException) {
            query_stats.markSerialized(((BadRequestException)ex).getStatus(), ex);
            query.badRequest((BadRequestException)ex);
            query_invalid.incrementAndGet();
          } else if (ex instanceof NoSuchUniqueName) {
            query_stats.markSerialized(HttpResponseStatus.BAD_REQUEST, ex);
            query.badRequest(new BadRequestException(ex));
            query_invalid.incrementAndGet();
          } else {
            query_stats.markSerialized(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex);
            query.badRequest(new BadRequestException(ex));
            query_exceptions.incrementAndGet();
          }
          
        } catch (RuntimeException ex2) {
          LOG.error("Exception thrown during exception handling", ex2);
          query_stats.markSerialized(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex2);
          query.sendReply(HttpResponseStatus.INTERNAL_SERVER_ERROR, 
              ex2.getMessage().getBytes());
          query_exceptions.incrementAndGet();
        }
        return null;
      }
    }
    
    /**
     * After all of the queries have run, we get the results in the order given
     * and add dump the results in an array
     */
    class QueriesCB implements Callback<Object, ArrayList<DataPoints[]>> {
      public Object call(final ArrayList<DataPoints[]> query_results) 
        throws Exception {
        if (allow_expressions) {
          // process each of the expressions into a new list, then merge it
          // with the original. This avoids possible recursion loops.
          final List<DataPoints[]> expression_results = 
              new ArrayList<DataPoints[]>(expressions.size());
          // let exceptions bubble up
          for (final ExpressionTree expression : expressions) {
            expression_results.add(expression.evaluate(query_results));
          }
          results.addAll(expression_results);
        } else {
          results.addAll(query_results);
        }
        
        /** Simply returns the buffer once serialization is complete and logs it */
        class SendIt implements Callback<Object, ChannelBuffer> {
          public Object call(final ChannelBuffer buffer) throws Exception {
            query.sendReply(buffer);
            query_success.incrementAndGet();
            return null;
          }
        }

        switch (query.apiVersion()) {
        case 0:
        case 1:
            query.serializer().formatQueryAsyncV1(data_query, results, 
               globals).addCallback(new SendIt()).addErrback(new ErrorCB());
          break;
        default: 
          query_invalid.incrementAndGet();
          throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
              "Requested API version not implemented", "Version " + 
              query.apiVersion() + " is not implemented");
        }
        return null;
      }
    }
    
    /**
     * Callback executed after we have resolved the metric, tag names and tag
     * values to their respective UIDs. This callback then runs the actual 
     * queries and fetches their results.
     */
    class BuildCB implements Callback<Deferred<Object>, Query[]> {
      @Override
      public Deferred<Object> call(final Query[] queries) {
        final ArrayList<Deferred<DataPoints[]>> deferreds = 
            new ArrayList<Deferred<DataPoints[]>>(queries.length);
        for (final Query query : queries) {
          deferreds.add(query.runAsync());
        }
        return Deferred.groupInOrder(deferreds).addCallback(new QueriesCB());
      }
    }
    
    /** Handles storing the global annotations after fetching them */
    class GlobalCB implements Callback<Object, List<Annotation>> {
      public Object call(final List<Annotation> annotations) throws Exception {
        globals.addAll(annotations);
        return data_query.buildQueriesAsync(tsdb).addCallback(new BuildCB());
      }
    }
 
    // if we the caller wants to search for global annotations, fire that off 
    // first then scan for the notes, then pass everything off to the formatter
    // when complete
    if (!data_query.getNoAnnotations() && data_query.getGlobalAnnotations()) {
      Annotation.getGlobalAnnotations(tsdb, 
        data_query.startTime() / 1000, data_query.endTime() / 1000)
          .addCallback(new GlobalCB()).addErrback(new ErrorCB());
    } else {
      data_query.buildQueriesAsync(tsdb).addCallback(new BuildCB())
        .addErrback(new ErrorCB());
    }
  }
  
  /**
   * Handles an expression query
   * @param tsdb The TSDB to which we belong
   * @param query The HTTP query to parse/respond
   * @since 2.3
   */
  private void handleExpressionQuery(final TSDB tsdb, final HttpQuery query) {
    final net.opentsdb.query.pojo.Query v2_query = 
        JSON.parseToObject(query.getContent(), net.opentsdb.query.pojo.Query.class);
    v2_query.validate();
    final QueryExecutor executor = new QueryExecutor(tsdb, v2_query);
    executor.execute(query);
  }
  
  /**
   * Processes a last data point query
   * @param tsdb The TSDB to which we belong
   * @param query The HTTP query to parse/respond
   */
  private void handleLastDataPointQuery(final TSDB tsdb, final HttpQuery query) {
    
    final LastPointQuery data_query;
    if (query.method() == HttpMethod.POST) {
      switch (query.apiVersion()) {
      case 0:
      case 1:
        data_query = query.serializer().parseLastPointQueryV1();
        break;
      default: 
        throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
            "Requested API version not implemented", "Version " + 
            query.apiVersion() + " is not implemented");
      }
    } else {
      data_query = this.parseLastPointQuery(tsdb, query);
    }
    
    if (data_query.sub_queries == null || data_query.sub_queries.isEmpty()) {
      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
          "Missing sub queries");
    }
    
    // a list of deferreds to wait on
    final ArrayList<Deferred<Object>> calls = new ArrayList<Deferred<Object>>();
    // final results for serialization
    final List<IncomingDataPoint> results = new ArrayList<IncomingDataPoint>();
    
    /**
     * Used to catch exceptions 
     */
    final class ErrBack implements Callback<Object, Exception> {
      public Object call(final Exception e) throws Exception {
        Throwable ex = e;
        while (ex.getClass().equals(DeferredGroupException.class)) {
          if (ex.getCause() == null) {
            LOG.warn("Unable to get to the root cause of the DGE");
            break;
          }
          ex = ex.getCause();
        }
        if (ex instanceof RuntimeException) {
          throw new BadRequestException(ex);
        } else {
          throw e;
        }
      }
      @Override
      public String toString() {
        return "Error back";
      }
    }
    
    final class FetchCB implements Callback<Deferred<Object>, ArrayList<IncomingDataPoint>> {
      @Override
      public Deferred<Object> call(final ArrayList<IncomingDataPoint> dps) throws Exception {
        synchronized(results) {
          for (final IncomingDataPoint dp : dps) {
            if (dp != null) {
              results.add(dp);
            }
          }
        }
        return Deferred.fromResult(null);
      }
      @Override
      public String toString() {
        return "Fetched data points CB";
      }
    }
    
    /**
     * Called after scanning the tsdb-meta table for TSUIDs that match the given
     * metric and/or tags. If matches were found, it fires off a number of
     * getLastPoint requests, adding the deferreds to the calls list
     */
    final class TSUIDQueryCB implements Callback<Deferred<Object>, ByteMap<Long>> {
      public Deferred<Object> call(final ByteMap<Long> tsuids) throws Exception {
        if (tsuids == null || tsuids.isEmpty()) {
          return null;
        }
        final ArrayList<Deferred<IncomingDataPoint>> deferreds =
            new ArrayList<Deferred<IncomingDataPoint>>(tsuids.size());
        for (Map.Entry<byte[], Long> entry : tsuids.entrySet()) {
          deferreds.add(TSUIDQuery.getLastPoint(tsdb, entry.getKey(), 
              data_query.getResolveNames(), data_query.getBackScan(), 
              entry.getValue()));
        }
        return Deferred.group(deferreds).addCallbackDeferring(new FetchCB());
      }
      @Override
      public String toString() {
        return "TSMeta scan CB";
      }
    }

    /**
     * Used to wait on the list of data point deferreds. Once they're all done
     * this will return the results to the call via the serializer
     */
    final class FinalCB implements Callback<Object, ArrayList<Object>> {
      public Object call(final ArrayList<Object> done) throws Exception {
        query.sendReply(query.serializer().formatLastPointQueryV1(results));
        return null;
      }
      @Override
      public String toString() {
        return "Final CB";
      }
    }
    
    try {   
      // start executing the queries
      for (final LastPointSubQuery sub_query : data_query.getQueries()) {
        final ArrayList<Deferred<IncomingDataPoint>> deferreds =
            new ArrayList<Deferred<IncomingDataPoint>>();
        // TSUID queries take precedence so if there are any TSUIDs listed, 
        // process the TSUIDs and ignore the metric/tags
        if (sub_query.getTSUIDs() != null && !sub_query.getTSUIDs().isEmpty()) {
          for (final String tsuid : sub_query.getTSUIDs()) {
            final TSUIDQuery tsuid_query = new TSUIDQuery(tsdb, 
                UniqueId.stringToUid(tsuid));
            deferreds.add(tsuid_query.getLastPoint(data_query.getResolveNames(), 
                data_query.getBackScan()));
          }
        } else {
          @SuppressWarnings("unchecked")
          final TSUIDQuery tsuid_query = 
              new TSUIDQuery(tsdb, sub_query.getMetric(), 
                  sub_query.getTags() != null ? 
                      sub_query.getTags() : Collections.EMPTY_MAP);
          if (data_query.getBackScan() > 0) {
            deferreds.add(tsuid_query.getLastPoint(data_query.getResolveNames(), 
                data_query.getBackScan()));
          } else {
            calls.add(tsuid_query.getLastWriteTimes()
                .addCallbackDeferring(new TSUIDQueryCB()));
          }
        }
        
        if (deferreds.size() > 0) {
          calls.add(Deferred.group(deferreds).addCallbackDeferring(new FetchCB()));
        }
      }
      
      Deferred.group(calls)
        .addCallback(new FinalCB())
        .addErrback(new ErrBack())
        .joinUninterruptibly();
      
    } catch (Exception e) {
      Throwable ex = e;
      while (ex.getClass().equals(DeferredGroupException.class)) {
        if (ex.getCause() == null) {
          LOG.warn("Unable to get to the root cause of the DGE");
          break;
        }
        ex = ex.getCause();
      }
      if (ex instanceof RuntimeException) {
        throw new BadRequestException(ex);
      } else {
        throw new RuntimeException("Shouldn't be here", e);
      }
    }
  }
  
  /**
   * Parses a query string legacy style query from the URI
   * @param tsdb The TSDB we belong to
   * @param query The HTTP Query for parsing
   * @return A TSQuery if parsing was successful
   * @throws BadRequestException if parsing was unsuccessful
   * @since 2.3
   */
  public static TSQuery parseQuery(final TSDB tsdb, final HttpQuery query) {
    return parseQuery(tsdb, query, null);
  }
  
  /**
   * Parses a query string legacy style query from the URI
   * @param tsdb The TSDB we belong to
   * @param query The HTTP Query for parsing
   * @param expressions A list of parsed expression trees filled from the URI.
   * If this is null, it means any expressions in the URI will be skipped.
   * @return A TSQuery if parsing was successful
   * @throws BadRequestException if parsing was unsuccessful
   * @since 2.3
   */
  public static TSQuery parseQuery(final TSDB tsdb, final HttpQuery query,
      final List<ExpressionTree> expressions) {
    final TSQuery data_query = new TSQuery();
    
    data_query.setStart(query.getRequiredQueryStringParam("start"));
    data_query.setEnd(query.getQueryStringParam("end"));
    
    if (query.hasQueryStringParam("padding")) {
      data_query.setPadding(true);
    }
    
    if (query.hasQueryStringParam("no_annotations")) {
      data_query.setNoAnnotations(true);
    }
    
    if (query.hasQueryStringParam("global_annotations")) {
      data_query.setGlobalAnnotations(true);
    }
    
    if (query.hasQueryStringParam("show_tsuids")) {
      data_query.setShowTSUIDs(true);
    }
    
    if (query.hasQueryStringParam("ms")) {
      data_query.setMsResolution(true);
    }
    
    if (query.hasQueryStringParam("show_query")) {
      data_query.setShowQuery(true);
    }  
    
    if (query.hasQueryStringParam("show_stats")) {
      data_query.setShowStats(true);
    }    
    
    if (query.hasQueryStringParam("show_summary")) {
        data_query.setShowSummary(true);
    }
    
    // handle tsuid queries first
    if (query.hasQueryStringParam("tsuid")) {
      final List<String> tsuids = query.getQueryStringParams("tsuid");     
      for (String q : tsuids) {
        parseTsuidTypeSubQuery(q, data_query);
      }
    }
    
    if (query.hasQueryStringParam("m")) {
      final List<String> legacy_queries = query.getQueryStringParams("m");      
      for (String q : legacy_queries) {
        parseMTypeSubQuery(q, data_query);
      }
    }
    
    // TODO - testing out the graphite style expressions here with the "exp" 
    // param that could stand for experimental or expression ;)
    if (expressions != null) {
      if (query.hasQueryStringParam("exp")) {
        final List<String> uri_expressions = query.getQueryStringParams("exp");
        final List<String> metric_queries = new ArrayList<String>(
            uri_expressions.size());
        // parse the expressions into their trees. If one or more expressions 
        // are improper then it will toss an exception up
        expressions.addAll(Expressions.parseExpressions(
            uri_expressions, data_query, metric_queries));
        // iterate over each of the parsed metric queries and store it in the
        // TSQuery list so that we fetch the data for them.
        for (final String mq: metric_queries) {
          parseMTypeSubQuery(mq, data_query);
        }
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received a request with an expression but at the "
            + "wrong endpoint: " + query);
      }
    }
    
    if (data_query.getQueries() == null || data_query.getQueries().size() < 1) {
      throw new BadRequestException("Missing sub queries");
    }
    return data_query;
  }
  
  /**
   * Parses a query string "m=..." type query and adds it to the TSQuery.
   * This will generate a TSSubQuery and add it to the TSQuery if successful
   * @param query_string The value of the m query string parameter, i.e. what
   * comes after the equals sign
   * @param data_query The query we're building
   * @throws BadRequestException if we are unable to parse the query or it is
   * missing components
   */
  private static void parseMTypeSubQuery(final String query_string, 
      TSQuery data_query) {
    if (query_string == null || query_string.isEmpty()) {
      throw new BadRequestException("The query string was empty");
    }
    
    // m is of the following forms:
    // agg:[interval-agg:][rate:]metric[{tag=value,...}]
    // where the parts in square brackets `[' .. `]' are optional.
    final String[] parts = Tags.splitString(query_string, ':');
    int i = parts.length;
    if (i < 2 || i > 5) {
      throw new BadRequestException("Invalid parameter m=" + query_string + " ("
          + (i < 2 ? "not enough" : "too many") + " :-separated parts)");
    }
    final TSSubQuery sub_query = new TSSubQuery();
    
    // the aggregator is first
    sub_query.setAggregator(parts[0]);
    
    i--; // Move to the last part (the metric name).
    List<TagVFilter> filters = new ArrayList<TagVFilter>();
    sub_query.setMetric(Tags.parseWithMetricAndFilters(parts[i], filters));
    sub_query.setFilters(filters);
    
    // parse out the rate and downsampler 
    for (int x = 1; x < parts.length - 1; x++) {
      if (parts[x].toLowerCase().startsWith("rate")) {
        sub_query.setRate(true);
        if (parts[x].indexOf("{") >= 0) {
          sub_query.setRateOptions(QueryRpc.parseRateOptions(true, parts[x]));
        }
      } else if (Character.isDigit(parts[x].charAt(0))) {
        sub_query.setDownsample(parts[x]);
      } else if (parts[x].equalsIgnoreCase("pre-agg")) {
        sub_query.setPreAggregate(true);
      } else if (parts[x].toLowerCase().startsWith("rollup_")) {
        sub_query.setRollupUsage(parts[x]);
      } else if (parts[x].toLowerCase().startsWith("explicit_tags")) {
        sub_query.setExplicitTags(true);
      }
    }
    
    if (data_query.getQueries() == null) {
      final ArrayList<TSSubQuery> subs = new ArrayList<TSSubQuery>(1);
      data_query.setQueries(subs);
    }
    data_query.getQueries().add(sub_query);
  }
  
  /**
   * Parses a "tsuid=..." type query and adds it to the TSQuery.
   * This will generate a TSSubQuery and add it to the TSQuery if successful
   * @param query_string The value of the m query string parameter, i.e. what
   * comes after the equals sign
   * @param data_query The query we're building
   * @throws BadRequestException if we are unable to parse the query or it is
   * missing components
   */
  private static void parseTsuidTypeSubQuery(final String query_string, 
      TSQuery data_query) {
    if (query_string == null || query_string.isEmpty()) {
      throw new BadRequestException("The tsuid query string was empty");
    }
    
    // tsuid queries are of the following forms:
    // agg:[interval-agg:][rate:]tsuid[,s]
    // where the parts in square brackets `[' .. `]' are optional.
    final String[] parts = Tags.splitString(query_string, ':');
    int i = parts.length;
    if (i < 2 || i > 5) {
      throw new BadRequestException("Invalid parameter m=" + query_string + " ("
          + (i < 2 ? "not enough" : "too many") + " :-separated parts)");
    }
    
    final TSSubQuery sub_query = new TSSubQuery();
    
    // the aggregator is first
    sub_query.setAggregator(parts[0]);
    
    i--; // Move to the last part (the metric name).
    final List<String> tsuid_array = Arrays.asList(parts[i].split(","));
    sub_query.setTsuids(tsuid_array);
    
    // parse out the rate and downsampler 
    for (int x = 1; x < parts.length - 1; x++) {
      if (parts[x].toLowerCase().startsWith("rate")) {
        sub_query.setRate(true);
        if (parts[x].indexOf("{") >= 0) {
          sub_query.setRateOptions(QueryRpc.parseRateOptions(true, parts[x]));
        }
      } else if (Character.isDigit(parts[x].charAt(0))) {
        sub_query.setDownsample(parts[x]);
      }
    }
    
    if (data_query.getQueries() == null) {
      final ArrayList<TSSubQuery> subs = new ArrayList<TSSubQuery>(1);
      data_query.setQueries(subs);
    }
    data_query.getQueries().add(sub_query);
  }
  
  /**
   * Parses the "rate" section of the query string and returns an instance
   * of the RateOptions class that contains the values found.
   * <p/>
   * The format of the rate specification is rate[{counter[,#[,#]]}].
   * @param rate If true, then the query is set as a rate query and the rate
   * specification will be parsed. If false, a default RateOptions instance
   * will be returned and largely ignored by the rest of the processing
   * @param spec The part of the query string that pertains to the rate
   * @return An initialized RateOptions instance based on the specification
   * @throws BadRequestException if the parameter is malformed
   * @since 2.0
   */
   static final public RateOptions parseRateOptions(final boolean rate,
       final String spec) {
     if (!rate || spec.length() == 4) {
       return new RateOptions(false, Long.MAX_VALUE,
           RateOptions.DEFAULT_RESET_VALUE);
     }

     if (spec.length() < 6) {
       throw new BadRequestException("Invalid rate options specification: "
           + spec);
     }

     String[] parts = Tags
         .splitString(spec.substring(5, spec.length() - 1), ',');
     if (parts.length < 1 || parts.length > 3) {
       throw new BadRequestException(
           "Incorrect number of values in rate options specification, must be " +
           "counter[,counter max value,reset value], recieved: "
               + parts.length + " parts");
     }

     final boolean counter = parts[0].endsWith("counter");
     try {
       final long max = (parts.length >= 2 && parts[1].length() > 0 ? Long
           .parseLong(parts[1]) : Long.MAX_VALUE);
       try {
         final long reset = (parts.length >= 3 && parts[2].length() > 0 ? Long
             .parseLong(parts[2]) : RateOptions.DEFAULT_RESET_VALUE);
         final boolean drop_counter = parts[0].equals("dropcounter");
         return new RateOptions(counter, max, reset, drop_counter);
       } catch (NumberFormatException e) {
         throw new BadRequestException(
             "Reset value of counter was not a number, received '" + parts[2]
                 + "'");
       }
     } catch (NumberFormatException e) {
       throw new BadRequestException(
           "Max value of counter was not a number, received '" + parts[1] + "'");
     }
   }

  /**
   * Parses a last point query from the URI string
   * @param tsdb The TSDB to which we belong
   * @param http_query The HTTP query to work with
   * @return A LastPointQuery object to execute against
   * @throws BadRequestException if parsing failed
   */
  private LastPointQuery parseLastPointQuery(final TSDB tsdb, 
      final HttpQuery http_query) {
    final LastPointQuery query = new LastPointQuery();
    
    if (http_query.hasQueryStringParam("resolve")) {
      query.setResolveNames(true);
    }
    
    if (http_query.hasQueryStringParam("back_scan")) {
      try {
        query.setBackScan(Integer.parseInt(http_query.getQueryStringParam("back_scan")));
      } catch (NumberFormatException e) {
        throw new BadRequestException("Unable to parse back_scan parameter");
      }
    }
    
    final List<String> ts_queries = http_query.getQueryStringParams("timeseries");
    final List<String> tsuid_queries = http_query.getQueryStringParams("tsuids");
    final int num_queries = 
      (ts_queries != null ? ts_queries.size() : 0) +
      (tsuid_queries != null ? tsuid_queries.size() : 0);
    final List<LastPointSubQuery> sub_queries = 
      new ArrayList<LastPointSubQuery>(num_queries);
    
    if (ts_queries != null) {
      for (String ts_query : ts_queries) {
        sub_queries.add(LastPointSubQuery.parseTimeSeriesQuery(ts_query));
      }
    }
    
    if (tsuid_queries != null) {
      for (String tsuid_query : tsuid_queries) {
        sub_queries.add(LastPointSubQuery.parseTSUIDQuery(tsuid_query));
      }
    }
    
    query.setQueries(sub_queries);
    return query;
  }
  
  /** @param collector Populates the collector with statistics */
  public static void collectStats(final StatsCollector collector) {
    collector.record("http.query.invalid_requests", query_invalid);
    collector.record("http.query.exceptions", query_exceptions);
    collector.record("http.query.success", query_success);
  }
  
  public static class LastPointQuery {
    
    private boolean resolve_names;
    private int back_scan;
    private List<LastPointSubQuery> sub_queries;
    
    /**
     * Default Constructor necessary for de/serialization
     */
    public LastPointQuery() {
      
    }
    
    /** @return Whether or not to resolve the UIDs to names */
    public boolean getResolveNames() {
      return resolve_names;
    }
    
    /** @return Number of hours to scan back in time looking for data */
    public int getBackScan() {
      return back_scan;
    }
    
    /** @return A list of sub queries */
    public List<LastPointSubQuery> getQueries() {
      return sub_queries;
    }
    
    /** @param resolve_names Whether or not to resolve the UIDs to names */
    public void setResolveNames(final boolean resolve_names) {
      this.resolve_names = resolve_names;
    }
    
    /** @param back_scan Number of hours to scan back in time looking for data */
    public void setBackScan(final int back_scan) {
      this.back_scan = back_scan;
    }
    
    /** @param queries A list of sub queries to execute */
    public void setQueries(final List<LastPointSubQuery> queries) {
      this.sub_queries = queries;
    }
  }
  
  public static class LastPointSubQuery {
    
    private String metric;
    private HashMap<String, String> tags;
    private List<String> tsuids;
    
    /**
     * Default constructor necessary for de/serialization
     */
    public LastPointSubQuery() {
      
    }
    
    public static LastPointSubQuery parseTimeSeriesQuery(final String query) {
      final LastPointSubQuery sub_query = new LastPointSubQuery();
      sub_query.tags = new HashMap<String, String>();
      sub_query.metric = Tags.parseWithMetric(query, sub_query.tags);
      return sub_query;
    }
    
    public static LastPointSubQuery parseTSUIDQuery(final String query) {
      final LastPointSubQuery sub_query = new LastPointSubQuery();
      final String[] tsuids = query.split(",");
      sub_query.tsuids = new ArrayList<String>(tsuids.length);
      for (String tsuid : tsuids) {
        sub_query.tsuids.add(tsuid);
      }
      return sub_query;
    }
    
    /** @return The name of the metric to search for */
    public String getMetric() {
      return metric;
    }
    
    /** @return A map of tag names and values */
    public Map<String, String> getTags() {
      return tags;
    }
    
    /** @return A list of TSUIDs to get the last point for */
    public List<String> getTSUIDs() {
      return tsuids;
    }
    
    /** @param metric The metric to search for */
    public void setMetric(final String metric) {
      this.metric = metric;
    }
    
    /** @param tags A map of tag name/value pairs */
    public void setTags(final Map<String, String> tags) {
      this.tags = (HashMap<String, String>) tags;
    }
    
    /** @param tsuids A list of TSUIDs to get data for */
    public void setTSUIDs(final List<String> tsuids) {
      this.tsuids = tsuids;
    }
  }
}