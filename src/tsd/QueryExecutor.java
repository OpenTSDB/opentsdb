// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph.CycleFoundException;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.QueryException;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import net.opentsdb.core.Tags;
import net.opentsdb.query.expression.ExpressionDataPoint;
import net.opentsdb.query.expression.ExpressionIterator;
import net.opentsdb.query.expression.NumericFillPolicy;
import net.opentsdb.query.expression.TimeSyncedIterator;
import net.opentsdb.query.expression.VariableIterator.SetOperator;
import net.opentsdb.query.pojo.Expression;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.Output;
import net.opentsdb.query.pojo.Query;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

/**
 * TEMP class for handling V2 queries with expression support. So far we ONLY
 * support expressions and this will be pipelined better. For now it's functioning
 * fairly well.
 * 
 * So far this sucker allows for expressions and nested expressions with the
 * ability to determine the output. If no output fields are specified, all 
 * expressions are dumped to the output. If one or more outputs are given then
 * only those outputs will be emitted.
 * 
 * TODO 
 * - handle/add output flags to determine whats emitted
 * - allow for queries only, no expressions
 * - possibly other set operations
 * - time over time queries
 * - skip querying for data that isn't going to be emitted
 */
public class QueryExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(QueryExecutor.class);
  
  /** The TSDB to which we belong (and will use for fetching data) */
  private final TSDB tsdb; 
  
  /** The user's query */
  private final Query query;
  
  /** TEMP A v1 TSQuery that we use for fetching the data from HBase */
  private final TSQuery ts_query;
  
  /** A map of the sub queries to their Metric ids */
  private final Map<String, TSSubQuery> sub_queries;
  
  /** A map of the sub query results to their Metric ids */
  private final Map<String, DataPoints[]> sub_query_results;
  
  /** A map of expression iterators to their IDs */
  private final Map<String, ExpressionIterator> expressions;

  /** A map of Metric fill policies to the metric IDs */
  private final Map<String, NumericFillPolicy> fills;

  /** The HTTP query from the user */
  private HttpQuery http_query;
  
  /**
   * Default Ctor that constructs a TSQuery and TSSubQueries from the new 
   * Query POJO class.
   * @param tsdb The TSDB to which we belong
   * @param query The raw query to parse and use for output
   * @throws IllegalArgumentException if we were unable to parse the Query into
   * a TSQuery.
   */
  public QueryExecutor(final TSDB tsdb, final Query query) {
    this.tsdb = tsdb;
    this.query = query;
    
    // if metrics is null, this is a bad query
    sub_queries = new HashMap<String, TSSubQuery>(query.getMetrics().size());
    sub_query_results = new HashMap<String, DataPoints[]>(
        query.getMetrics().size());
    
    if (query.getExpressions() != null) {
      expressions = new HashMap<String, ExpressionIterator>(
          query.getExpressions().size());
    } else {
      expressions = null;
    }
    
    final Timespan timespan = query.getTime();
    
    // compile the ts_query
    ts_query = new TSQuery();
    ts_query.setStart(timespan.getStart());
    ts_query.setTimezone(timespan.getTimezone());
    
    if (timespan.getEnd() != null && !timespan.getEnd().isEmpty()) {
      ts_query.setEnd(timespan.getEnd());
    }
    
    fills = new HashMap<String, NumericFillPolicy>(query.getMetrics().size());
    for (final Metric mq : query.getMetrics()) {
      if (mq.getFillPolicy() != null) {
        fills.put(mq.getId(), mq.getFillPolicy());
      }
      final TSSubQuery sub = new TSSubQuery();
      sub_queries.put(mq.getId(), sub);
      
      sub.setMetric(mq.getMetric());

      if (timespan.getDownsampler() != null) {
        sub.setDownsample(timespan.getDownsampler().getInterval() + "-" + 
            timespan.getDownsampler().getAggregator());
      }

      // filters
      if (mq.getFilter() != null && !mq.getFilter().isEmpty()) {
        Filter filters = null;
        if (query.getFilters() == null || query.getFilters().isEmpty()) {
          throw new IllegalArgumentException("No filter defined: " + mq.getFilter());
        }
        for (final Filter filter : query.getFilters()) {
          if (filter.getId().equals(mq.getFilter())) {
            filters = filter;
            break;
          }
        }
        sub.setRate(timespan.isRate());
        sub.setFilters(filters.getTags());
        sub.setAggregator(
            mq.getAggregator() != null ? mq.getAggregator() : timespan.getAggregator());
      }
    }
    
    final ArrayList<TSSubQuery> subs = 
        new ArrayList<TSSubQuery>(sub_queries.values()); 
    ts_query.setQueries(subs);
    
    // setup expressions
    for (final Expression expression : query.getExpressions()) {
      // TODO - flags
      
      // TODO - get a default from the configs
      final SetOperator operator = expression.getJoin() != null ? 
          expression.getJoin().getOperator() : SetOperator.UNION;
      final boolean qts = expression.getJoin() == null ? false : expression.getJoin().getUseQueryTags();
      final boolean ats = expression.getJoin() == null ? true : expression.getJoin().getIncludeAggTags();
      final ExpressionIterator iterator = 
          new ExpressionIterator(expression.getId(), expression.getExpr(), 
          operator, qts, ats);
      if (expression.getFillPolicy() != null) {
        iterator.setFillPolicy(expression.getFillPolicy());
      }
      expressions.put(expression.getId(), iterator);
      
    }

    ts_query.validateAndSetQuery();
  }

  /**
   * Execute the RPC and serialize the response
   * @param query The HTTP query to parse and and return results to
   */
  public void execute(final HttpQuery query) {
    http_query = query;
    final QueryStats query_stats = 
        new QueryStats(query.getRemoteAddress(), ts_query, query.getHeaders());
    ts_query.setQueryStats(query_stats);

    final long start = DateTime.currentTimeMillis();

    /**
     * Sends the serialized results to the caller. This should be the very
     * last callback executed.
     */
    class CompleteCB implements Callback<Object, ChannelBuffer> {
      @Override
      public Object call(final ChannelBuffer cb) throws Exception {
        query.sendReply(cb);
        return null;
      }
    }
    
    /**
     * After all of the queries have run and we have data (or not) then we
     * need to compile the iterators.
     * This class could probably be improved:
     * First we iterate over the results AND for each result, iterate over
     * the expressions, giving a time synced iterator to each expression that 
     * needs the result set.
     * THEN we iterate over the expressions again and build a DAG to determine
     * if any of the expressions require the output of an expression. If so
     * then we add the expressions to the proper parent and compile them in
     * order.
     * After all of that we're ready to start serializing and iterating
     * over the results.
     */
    class QueriesCB implements Callback<Object, ArrayList<DataPoints[]>> {
      public Object call(final ArrayList<DataPoints[]> query_results) 
        throws Exception {
        
        for (int i = 0; i < query_results.size(); i++) {
          final TSSubQuery sub = ts_query.getQueries().get(i);
          
          Iterator<Entry<String, TSSubQuery>> it = sub_queries.entrySet().iterator();
          while (it.hasNext()) {
            final Entry<String, TSSubQuery> entry = it.next();
            if (entry.getValue().equals(sub)) {
              sub_query_results.put(entry.getKey(), query_results.get(i));
              for (final ExpressionIterator ei : expressions.values()) {
                if (ei.getVariableNames().contains(entry.getKey())) {
                  final TimeSyncedIterator tsi = new TimeSyncedIterator(
                      entry.getKey(), sub.getFilterTagKs(), 
                      query_results.get(i));
                  final NumericFillPolicy fill = fills.get(entry.getKey());
                  if (fill != null) {
                    tsi.setFillPolicy(fill);
                  }
                  ei.addResults(entry.getKey(), tsi);
                  LOG.debug("Added results for " + entry.getKey() + 
                      " to " + ei.getId());
                }
              }
            }
          }
        }
        
        // handle nested expressions
        DirectedAcyclicGraph<String, DefaultEdge> graph = null;
        for (final Entry<String, ExpressionIterator> eii : expressions.entrySet()) {
          for (final String var : eii.getValue().getVariableNames()) {
            final ExpressionIterator ei = expressions.get(var);
            if (ei != null) {
              // TODO - really ought to calculate this earlier
              if (eii.getKey().equals(var)) {
                throw new IllegalArgumentException(
                    "Self referencing expression found: " + eii.getKey());
              }
              LOG.debug("Nested expression detected. " + eii.getKey() + 
                  " depends on " + var);

              if (graph == null) {
                graph = new DirectedAcyclicGraph<String, DefaultEdge>(DefaultEdge.class);
              }
              if (!graph.containsVertex(eii.getKey())) {
                graph.addVertex(eii.getKey());
              }
              if (!graph.containsVertex(var)) {
                graph.addVertex(var);
              }
              try {
                graph.addDagEdge(eii.getKey(), var);
              } catch (CycleFoundException cfe) {
                throw new IllegalArgumentException("Circular reference found: " + 
                    eii.getKey(), cfe);
              }
            }
          }
        }

        // compile all of the expressions
        final long intersect_start = DateTime.currentTimeMillis();
        if (graph != null) {
          final ExpressionIterator[] compile_stack = 
              new ExpressionIterator[expressions.size()];
          final TopologicalOrderIterator<String, DefaultEdge> it = 
              new TopologicalOrderIterator<String, DefaultEdge>(graph);
          int i = 0;
          while (it.hasNext()) {
            compile_stack[i++] = expressions.get(it.next());
          }
          for (int x = compile_stack.length - 1; x >= 0; x--) {
            // look for and add expressions
            for (final String var : compile_stack[x].getVariableNames()) {
              ExpressionIterator source = expressions.get(var);
              if (source != null) {
                compile_stack[x].addResults(var, source.getCopy());
                LOG.debug("Adding expression " + source.getId() + " to " + 
                    compile_stack[x].getId());
              }
            }
            
            compile_stack[x].compile();
            LOG.debug("Successfully compiled " + compile_stack[x]);
          }
        } else {
          for (final ExpressionIterator ei : expressions.values()) {
            ei.compile();
            LOG.debug("Successfully compiled " + ei);
          }
        }
        LOG.debug("Finished compilations in " + 
            (DateTime.currentTimeMillis() - intersect_start) + " ms");
        
        return serialize().addCallback(new CompleteCB()).addErrback(new ErrorCB());
      }
    }
    
    /**
     * Callback executed after we have resolved the metric, tag names and tag
     * values to their respective UIDs. This callback then runs the actual 
     * queries and fetches their results.
     */
    class BuildCB implements Callback<Deferred<Object>, net.opentsdb.core.Query[]> {
      @Override
      public Deferred<Object> call(final net.opentsdb.core.Query[] queries) {
        final ArrayList<Deferred<DataPoints[]>> deferreds = 
            new ArrayList<Deferred<DataPoints[]>>(queries.length);
        
        for (final net.opentsdb.core.Query query : queries) {
          deferreds.add(query.runAsync());
        }
        return Deferred.groupInOrder(deferreds).addCallback(new QueriesCB())
            .addErrback(new ErrorCB());
      }
    }
    
    // TODO - only run the ones that will be involved in an output. Folks WILL
    // ask for stuff they don't need.... *sigh*
    ts_query.buildQueriesAsync(tsdb).addCallback(new BuildCB())
      .addErrback(new ErrorCB());
  }
  
  /**
   * Writes the results to a ChannelBuffer to return to the caller. This will
   * iterate over all of the outputs and drop in meta data where appropriate.
   * @throws Exception if something went pear shaped
   */
  private Deferred<ChannelBuffer> serialize() throws Exception {
    final long start = System.currentTimeMillis();
    // buffers and an array list to stored the deferreds
    final ChannelBuffer response = ChannelBuffers.dynamicBuffer();
    final OutputStream output_stream = new ChannelBufferOutputStream(response);

    final JsonGenerator json = JSON.getFactory().createGenerator(output_stream);
    json.writeStartObject();
    json.writeFieldName("outputs");
    json.writeStartArray();

    // We want the serializer to execute serially so we need to create a callback
    // chain so that when one DPsResolver is finished, it triggers the next to
    // start serializing.
    final Deferred<Object> cb_chain = new Deferred<Object>();

    // default to the expressions if there, or fall back to the metrics
    final List<Output> outputs;
    if (query.getOutputs() == null || query.getOutputs().isEmpty()) {
      if (query.getExpressions() != null && !query.getExpressions().isEmpty()) {
        outputs = new ArrayList<Output>(query.getExpressions().size());
        for (final Expression exp : query.getExpressions()) {
          outputs.add(Output.Builder().setId(exp.getId()).build());
        }
      } else if (query.getMetrics() != null && !query.getMetrics().isEmpty()) {
        outputs = new ArrayList<Output>(query.getMetrics().size());
        for (final Metric metric : query.getMetrics()) {
          outputs.add(Output.Builder().setId(metric.getId()).build());
        }
      } else {
        throw new IllegalArgumentException(
            "How did we get here?? No metrics or expressions??");
      }
    } else {
      outputs = query.getOutputs();
    }

    for (final Output output : outputs) {
      if (expressions != null) {
        final ExpressionIterator it = expressions.get(output.getId());
        if (it != null) {
          cb_chain.addCallback(new SerializeExpressionIterator(tsdb, json, 
              output, it, ts_query));
          continue;
        }
      }

      if (query.getMetrics() != null && !query.getMetrics().isEmpty()) {
        final TSSubQuery sub = sub_queries.get(output.getId());
        if (sub != null) {
          final TimeSyncedIterator it = new TimeSyncedIterator(output.getId(), 
              sub.getFilterTagKs(), sub_query_results.get(output.getId()));
          cb_chain.addCallback(new SerializeSubIterator(tsdb, json, output, it));
          continue;
        }
      } else {
        LOG.warn("Couldn't find a variable matching: " + output.getId() + 
            " in query " + query);
      }
    }
  
    /** Final callback to close out the JSON array and return our results */
    class FinalCB implements Callback<ChannelBuffer, Object> {
      public ChannelBuffer call(final Object obj)
          throws Exception {
        json.writeEndArray();
        
//        ts_query.getQueryStats().setTimeSerialization(
//            DateTime.currentTimeMillis() - start);
        ts_query.getQueryStats().markSerializationSuccessful();

        // dump overall stats as an extra object in the array
//        if (true) {
//          final QueryStats stats = ts_query.getQueryStats();
//          json.writeFieldName("statsSummary");
//          json.writeStartObject();
//          //json.writeStringField("hostname", TSDB.getHostname());
//          //json.writeNumberField("runningQueries", stats.getNumRunningQueries());
//          json.writeNumberField("datapoints", stats.getAggregatedSize());
//          json.writeNumberField("rawDatapoints", stats.getSize());
//          //json.writeNumberField("rowsFetched", stats.getRowsFetched());
//          json.writeNumberField("aggregationTime", stats.getTimeAggregation());
//          json.writeNumberField("serializationTime", stats.getTimeSerialization());
//          json.writeNumberField("storageTime", stats.getTimeStorage());
//          json.writeNumberField("timeTotal", 
//              ((double)stats.getTimeTotal() / (double)1000000));
//          json.writeEndObject();
//        }
        
        // dump the original query
        if (true) {
          json.writeFieldName("query");
          json.writeObject(QueryExecutor.this.query);
        }
        // IMPORTANT Make sure the close the JSON array and the generator
        json.writeEndObject();
        json.close();
        return response;
      }
    }

    // trigger the callback chain here
    cb_chain.callback(null);
    return cb_chain.addCallback(new FinalCB());
  }
  
  /** This has to be attached to callbacks or we may never respond to clients */
  class ErrorCB implements Callback<Object, Exception> {
    public Object call(final Exception e) throws Exception {
      try {
        LOG.error("Query exception: ", e);
        if (e instanceof DeferredGroupException) {
          Throwable ex = e.getCause();
          while (ex != null && ex instanceof DeferredGroupException) {
            ex = ex.getCause();
          }
          if (ex != null) {
            LOG.error("Unexpected exception: ", ex);
            // TODO - find a better way to determine the real error
//            QueryExecutor.this.ts_query.getQueryStats()
//              .markComplete(HttpResponseStatus.BAD_REQUEST, ex);
            QueryExecutor.this.http_query.badRequest(new BadRequestException(ex));
          } else {
            LOG.error("The deferred group exception didn't have a cause???");
//            QueryExecutor.this.ts_query.getQueryStats()
//              .markComplete(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex);
            QueryExecutor.this.http_query.badRequest(new BadRequestException(e));
          }
        } else if (e.getClass() == QueryException.class) {
//          QueryExecutor.this.ts_query.getQueryStats()
//            .markComplete(HttpResponseStatus.REQUEST_TIMEOUT, e);
          QueryExecutor.this.http_query.badRequest(new BadRequestException((QueryException)e));
        } else {
//          QueryExecutor.this.ts_query.getQueryStats()
//            .markComplete(HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
          QueryExecutor.this.http_query.badRequest(new BadRequestException(e));
        }
        return null;
      } catch (RuntimeException ex) {
        LOG.error("Exception thrown during exception handling", ex);
//        QueryExecutor.this.ts_query.getQueryStats()
//          .markComplete(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex);
        QueryExecutor.this.http_query.sendReply(HttpResponseStatus.INTERNAL_SERVER_ERROR, 
            ex.getMessage().getBytes());
        return null;
      }
    }
  }
  
  /**
   * Handles serializing the output of an expression iterator
   */
  private class SerializeExpressionIterator 
    implements Callback<Deferred<Object>, Object> {
    final TSDB tsdb;
    final JsonGenerator json;
    final Output output;
    final ExpressionIterator iterator;
    final ExpressionDataPoint[] dps;
    final TSQuery query;

    // WARNING: Make sure to write an endObject() before triggering this guy
    final Deferred<Object> completed;
    
    /**
     * The default ctor to setup the serializer
     * @param tsdb The TSDB to use for name resolution
     * @param json The JSON generator to write to
     * @param output The Output spec associated with this expression
     * @param iterator The iterator to run through
     * @param query The original TSQuery
     */
    public SerializeExpressionIterator(final TSDB tsdb, final JsonGenerator json, 
        final Output output, final ExpressionIterator iterator, final TSQuery query) {
      this.tsdb = tsdb;
      this.json = json;
      this.output = output;
      this.iterator = iterator;
      this.query = query;
      dps = iterator.values();
      completed = new Deferred<Object>();
    }

    /** Super simple closer that tells the upstream chain we're done with this */
    class MetaCB implements Callback<Object, Object> {
      @Override
      public Object call(final Object ignored) throws Exception {
        completed.callback(null);
        return completed;
      }
    }
    
    @Override
    public Deferred<Object> call(final Object ignored) throws Exception {
      //result set opening
      json.writeStartObject();
      
      json.writeStringField("id", output.getId());
      if (output.getAlias() != null) {
        json.writeStringField("alias", output.getAlias());
      }
      json.writeFieldName("dps");
      json.writeStartArray();
      
      long first_ts = Long.MIN_VALUE;
      long last_ts = 0;
      long count = 0;
      long ts = iterator.nextTimestamp();
      long qs = query.startTime();
      long qe = query.endTime();
      while (iterator.hasNext()) {
        iterator.next(ts);
        
        long timestamp = dps[0].timestamp();
        if (timestamp >= qs && timestamp <= qe) {
          json.writeStartArray();
          if (dps.length > 0) {
            json.writeNumber(timestamp);
            if (first_ts == Long.MIN_VALUE) {
              first_ts = timestamp;
            } else {
              last_ts = timestamp;
            }
            ++count;
          }
          for (int i = 0; i < dps.length; i++) {
            json.writeNumber(dps[i].toDouble());
          }
          
          json.writeEndArray();
        }
        ts = iterator.nextTimestamp();
      }
      json.writeEndArray();
      
      // data points meta
      json.writeFieldName("dpsMeta");
      json.writeStartObject();
      json.writeNumberField("firstTimestamp", first_ts < 0 ? 0 : first_ts);
      json.writeNumberField("lastTimestamp", last_ts);
      json.writeNumberField("setCount", count);
      json.writeNumberField("series", dps.length);
      json.writeEndObject();
      
      // resolve meta LAST since we may not even need it
      if (dps.length > 0) {
        final MetaSerializer meta_serializer = 
            new MetaSerializer(tsdb, json, iterator.values());
        meta_serializer.call(null).addCallback(new MetaCB())
          .addErrback(QueryExecutor.this.new ErrorCB());
      } else {
        // done, not dumping any more info
        json.writeEndObject();
        //json.writeEndArray();
        completed.callback(null);
      }
      
      return completed;
    }
    
  }
  
  /**
   * Serializes a raw, non expression result set.
   */
  private class SerializeSubIterator implements 
    Callback<Deferred<Object>, Object> {
    final TSDB tsdb;
    final JsonGenerator json;
    final Output output;
    final TimeSyncedIterator iterator;
    
    // WARNING: Make sure to write an endObject() before triggering this guy
    final Deferred<Object> completed;
    
    public SerializeSubIterator(final TSDB tsdb, final JsonGenerator json, 
        final Output output, final TimeSyncedIterator iterator) {
      this.tsdb = tsdb;
      this.json = json;
      this.output = output;
      this.iterator = iterator;
      completed = new Deferred<Object>();
    }
    
    class MetaCB implements Callback<Object, Object> {
      @Override
      public Object call(final Object ignored) throws Exception {
        completed.callback(null);
        return completed;
      }
    }
    
    @Override
    public Deferred<Object> call(final Object ignored) throws Exception {
      //result set opening
      json.writeStartObject();
      
      json.writeStringField("id", output.getId());
      if (output.getAlias() != null) {
        json.writeStringField("alias", output.getAlias());
      }
      json.writeFieldName("dps");
      json.writeStartArray();
      
      final long first_ts = iterator.nextTimestamp();
      long ts = first_ts;
      long last_ts = 0;
      long count = 0;
      final DataPoint[] dps = iterator.values();
      while (iterator.hasNext()) {
        iterator.next(ts);
        json.writeStartArray();
        
        if (dps.length > 0) {
          json.writeNumber(dps[0].timestamp());
          last_ts = dps[0].timestamp();
          ++count;
        }
        for (int i = 0; i < dps.length; i++) {
          json.writeNumber(dps[i].toDouble());
        }
        
        json.writeEndArray();
        ts = iterator.nextTimestamp();
      }
      json.writeEndArray();
      
      // data points meta
      json.writeFieldName("dpsMeta");
      json.writeStartObject();
      json.writeNumberField("firstTimestamp", first_ts);
      json.writeNumberField("lastTimestamp", last_ts);
      json.writeNumberField("setCount", count);
      json.writeNumberField("series", dps.length);
      json.writeEndObject();
      
      // resolve meta LAST since we may not even need it
      if (dps.length > 0) {
        final DataPoints[] odps = iterator.getDataPoints();
        final ExpressionDataPoint[] edps = new ExpressionDataPoint[dps.length];
        for (int i = 0; i < dps.length; i++) {
          edps[i] = new ExpressionDataPoint(odps[i]);
        }
        final MetaSerializer meta_serializer = 
            new MetaSerializer(tsdb, json, edps);
        meta_serializer.call(null).addCallback(new MetaCB());
      } else {
        // done, not dumping any more info
        json.writeEndObject();
        completed.callback(null);
      }
      
      return completed;
    }
    
  }
  
  /**
   * Handles resolving metrics, tags, aggregated tags and other meta data 
   * associated with a result set.
   */
  private class MetaSerializer implements Callback<Deferred<Object>, Object> {
    final TSDB tsdb;
    final JsonGenerator json;
    final ExpressionDataPoint[] dps;
    final List<String> metrics;
    final Map<String, String>[] tags;
    final List<String>[] agg_tags;

    final Deferred<Object> completed;
    
    @SuppressWarnings("unchecked")
    public MetaSerializer(final TSDB tsdb, final JsonGenerator json, 
        final ExpressionDataPoint[] dps) {
      this.tsdb = tsdb;
      this.json = json;
      this.dps = dps;
      completed = new Deferred<Object>();
      metrics = new ArrayList<String>();
      tags = new Map[dps.length];
      agg_tags = new List[dps.length];
    }
    
    class MetricsCB implements Callback<Object, ArrayList<String>> {
      @Override
      public Object call(final ArrayList<String> names) throws Exception {
        metrics.addAll(names);
        Collections.sort(metrics);
        return null;
      }
    }
    
    class AggTagsCB implements Callback<Object, ArrayList<String>> {
      final int index;
      public AggTagsCB(final int index) {
        this.index = index;
      }
      @Override
      public Object call(final ArrayList<String> tags) throws Exception {
        agg_tags[index] = tags;
        return null;
      }
    }
    
    class TagsCB implements Callback<Object, Map<String, String>> {
      final int index;
      public TagsCB(final int index) {
        this.index = index;
      }
      @Override
      public Object call(final Map<String, String> tags) throws Exception {
        MetaSerializer.this.tags[index] = tags;
        return null;
      }
    }
    
    class MetaCB implements Callback<Object, ArrayList<Object>> {
      @Override
      public Object call(final ArrayList<Object> ignored) throws Exception {
        json.writeFieldName("meta");
        json.writeStartArray();
        
        // first field is the timestamp
        json.writeStartObject();
        json.writeNumberField("index", 0);
        json.writeFieldName("metrics");
        json.writeStartArray();
        json.writeString("timestamp");
        json.writeEndArray();
        json.writeEndObject();
        
        for (int i = 0; i < dps.length; i++) {
          json.writeStartObject();
          
          json.writeNumberField("index", i + 1);
          json.writeFieldName("metrics");
          json.writeObject(metrics);
          
          json.writeFieldName("commonTags");
          if (tags[i] == null) {
            json.writeObject(Collections.emptyMap());
          } else {
            json.writeObject(tags[i]);
          }
          
          json.writeFieldName("aggregatedTags");
          if (agg_tags[i] == null) {
            json.writeObject(Collections.emptyList());
          } else {
            json.writeObject(agg_tags[i]);
          }
          
          // TODO restore when we can calculate size efficiently
          //json.writeNumberField("dps", dps[i].size());
          //json.writeNumberField("rawDps", dps[i].rawSize());
          
          json.writeEndObject();
        }
        
        json.writeEndArray();

        // all done with this series of results
        json.writeEndObject();
        completed.callback(null);
        return null;
      }
    }
    
    @Override
    public Deferred<Object> call(final Object ignored) throws Exception {
      final List<Deferred<Object>> deferreds = 
          new ArrayList<Deferred<Object>>();
      
      final List<Deferred<String>> metric_deferreds = 
          new ArrayList<Deferred<String>>(dps[0].metricUIDs().size());
      
      for (final byte[] uid : dps[0].metricUIDs()) {
        metric_deferreds.add(tsdb.getUidName(UniqueIdType.METRIC, uid));
      }
      
      deferreds.add(Deferred.group(metric_deferreds)
          .addCallback(new MetricsCB()));
      
      for (int i = 0; i < dps.length; i++) {
        if (dps[i].aggregatedTags().size() > 0) {
          final List<Deferred<String>> agg_deferreds =
              new ArrayList<Deferred<String>>(dps[i].aggregatedTags().size());
          for (final byte[] uid : dps[i].aggregatedTags()) {
            agg_deferreds.add(tsdb.getUidName(UniqueIdType.TAGK, uid));
          }
          deferreds.add(Deferred.group(agg_deferreds)
              .addCallback(new AggTagsCB(i)));
        }
        
        deferreds.add(Tags.getTagsAsync(tsdb, dps[i].tags())
            .addCallback(new TagsCB(i)));
      }
      
      Deferred.groupInOrder(deferreds).addCallback(new MetaCB())
        .addErrback(QueryExecutor.this.new ErrorCB());
      return completed;
    }
    
  }

}
