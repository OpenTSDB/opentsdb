// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.data.prometheus;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.FailedPredicateException;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import jersey.repackaged.com.google.common.collect.Lists;
import jersey.repackaged.com.google.common.collect.Maps;
import jersey.repackaged.com.google.common.collect.Sets;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.prometheus.grammar.PromQLLexer;
import net.opentsdb.prometheus.grammar.PromQLParser.AddOpContext;
import net.opentsdb.prometheus.grammar.PromQLParser.AggregationContext;
import net.opentsdb.prometheus.grammar.PromQLParser.AndUnlessOpContext;
import net.opentsdb.prometheus.grammar.PromQLParser.ByContext;
import net.opentsdb.prometheus.grammar.PromQLParser.CompareOpContext;
import net.opentsdb.prometheus.grammar.PromQLParser.ExpressionContext;
import net.opentsdb.prometheus.grammar.PromQLParser.FunctionContext;
import net.opentsdb.prometheus.grammar.PromQLParser.GroupLeftContext;
import net.opentsdb.prometheus.grammar.PromQLParser.GroupRightContext;
import net.opentsdb.prometheus.grammar.PromQLParser.GroupingContext;
import net.opentsdb.prometheus.grammar.PromQLParser.IgnoringContext;
import net.opentsdb.prometheus.grammar.PromQLParser.InstantSelectorContext;
import net.opentsdb.prometheus.grammar.PromQLParser.KeywordContext;
import net.opentsdb.prometheus.grammar.PromQLParser.LabelMatcherContext;
import net.opentsdb.prometheus.grammar.PromQLParser.LabelMatcherListContext;
import net.opentsdb.prometheus.grammar.PromQLParser.LabelMatcherOperatorContext;
import net.opentsdb.prometheus.grammar.PromQLParser.LabelNameContext;
import net.opentsdb.prometheus.grammar.PromQLParser.LabelNameListContext;
import net.opentsdb.prometheus.grammar.PromQLParser.LiteralContext;
import net.opentsdb.prometheus.grammar.PromQLParser.MatrixSelectorContext;
import net.opentsdb.prometheus.grammar.PromQLParser.MultOpContext;
import net.opentsdb.prometheus.grammar.PromQLParser.OffsetContext;
import net.opentsdb.prometheus.grammar.PromQLParser.OnContext;
import net.opentsdb.prometheus.grammar.PromQLParser.OrOpContext;
import net.opentsdb.prometheus.grammar.PromQLParser.ParameterContext;
import net.opentsdb.prometheus.grammar.PromQLParser.ParameterListContext;
import net.opentsdb.prometheus.grammar.PromQLParser.ParensContext;
import net.opentsdb.prometheus.grammar.PromQLParser.PowOpContext;
import net.opentsdb.prometheus.grammar.PromQLParser.UnaryOpContext;
import net.opentsdb.prometheus.grammar.PromQLParser.VectorContext;
import net.opentsdb.prometheus.grammar.PromQLParser.VectorOperationContext;
import net.opentsdb.prometheus.grammar.PromQLParser.WithoutContext;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.filter.ChainFilter;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.filter.NotFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.TagKeyLiteralOrFilter;
import net.opentsdb.query.filter.TagValueLiteralOrFilter;
import net.opentsdb.query.filter.TagValueRegexFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.expressions.ExpressionConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.query.processor.rate.RateConfig;
import net.opentsdb.query.processor.topn.TopNConfig;
import net.opentsdb.utils.DateTime;
import net.opentsdb.prometheus.grammar.PromQLParserVisitor;

/**
 * The start of a PromQL query parsing visitor. Since TSDB doesn't do great with
 * last values we're defaulting to a 1 hour query range and making everything a
 * range. We _can_ support instants and will do so.
 * 
 * Notes:
 * <b>RATE:</b>
 * This one is odd. So from the docs, "rate(http_requests_total[5m])" will give 
 * the per-second _average_ rate over 5 minutes. Sounds fine. But if we leave 
 * out the range we should throw an exception cause a rate on an instant doesn't
 * make sense?
 * But then we have "rate(http_requests_total[5m])[30m:1m]" which supposedly is
 * a sub-query that gives "the 5-minute rate" for the past 30m with a 1m resolution.
 * So it looks like this may do a sliding window of 5m rate averages for 30m
 * every minute. Blech.
 * <p>
 * <b>Downsample:</b>
 * I think the downsample is always an average. Also, it is a bit confusing when
 * it's applied in the docs so I'm supporting `[5m:1m]` as a downsample always.
 * <p>
 * <b>Misc:</b>
 * TODO - support instants. We need a config to say how far back the TSD should 
 * look and then we need to set the "last" flag on the data source.
 * TODO - RE2 regex syntax to Java. Dunno how different it'd be.
 * TODO - what's the range `[5m:]` mean??
 * TODO - ranges at the end of all functions. How do we handle overrides?
 * TODO - join ops
 * TODO - power
 * TODO - tons of functions
 * TODO - what happens with different ranges in prom? If it's valid then we'd 
 *        need a set of queries instead of a single query. *sigh*
 * TODO - ranges on a ranged query. WWPD?
 * 
 * @since 3.0
 */
public class PromQLParser extends DefaultErrorStrategy implements PromQLParserVisitor {
  private static final Logger LOG = LoggerFactory.getLogger(PromQLParser.class);
  
  private static final Pattern IS_EPOCH = Pattern.compile("^\\d+$");
  private static final Pattern IS_FLOAT_STEP = Pattern.compile("^[0-9\\.]+$");
  
  /** The query we're working on. */
  protected final String query;
  protected final String start;
  protected final String end;
  
  /** Can be a duration or a floating point value representing seconds. */
  protected final String step;
  
  /** Naming counters. */
  protected int metric_counter = 0;
  protected int expression_counter = 0;
  
  /** Tracks counts in a query per node so if we have multiples we don't collide
   * on the IDs. */
  protected final Map<Class, Integer> id_counters;
  
  /** The last range applied to a metric. */
  protected final Map<String, String> metric_ranges;
  
  /** Sub-queries keyed on the metric node ID. */
  protected final Map<String, List<QueryNodeConfig>> sub_queries;
  
  /** The last node parsed from the visitor. */
  protected QueryNodeConfig last_node;
  
  /** The ID of the last metric parsed. */
  protected String last_metric_id;
  
  /** Default interpolator config. Doesn't look like Prometheus does much
   * interpolation. TODO - figure it out. */
  protected NumericInterpolatorConfig default_interperlator = 
      (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
      .setDataType(NumericType.TYPE.toString())
      .build();
  
  /** Default join config for now. */
  protected JoinConfig join = JoinConfig.newBuilder()
      .setJoinType(JoinType.NATURAL_OUTER)
      .setId("default")
      .build();
  
  /**
   * Default ctor.
   * @param query A non-null and non-empty query to parse.
   * @param start The non-null and non-empty start timestamp to parse.
   * @param end An optional end timestamp. If null or empt we use the current
   * timestamp.
   * @param step An optional downsample. 
   */
  public PromQLParser(final String query,
                      final String start,
                      final String end,
                      final String step) {
    this.query = query;
    id_counters = Maps.newHashMap();
    metric_ranges = Maps.newHashMap();
    sub_queries = Maps.newHashMap();
    
    // validate and convert timestamps.
    if (Strings.isNullOrEmpty(start)) {
      throw new IllegalArgumentException("Missing the start timestamp.");
    }
    
    if (IS_EPOCH.matcher(start).find()) {
      this.start = start;
    } else {
      Instant temp = Instant.parse(start);
      // TODO - ms
      this.start = Long.toString(temp.getEpochSecond());
    }
    
    if (!Strings.isNullOrEmpty(end)) {
      if (IS_EPOCH.matcher(end).find()) {
        this.end = end;
      } else {
        Instant temp = Instant.parse(end);
        this.end = Long.toString(temp.getEpochSecond());
      }
    } else {
      this.end = Long.toString(DateTime.currentTimeMillis() / 1000);
    }
    
    if (!Strings.isNullOrEmpty(step) && IS_FLOAT_STEP.matcher(step).find()) {
      final double float_step = Double.parseDouble(step);
      // TODO - ignoring float values for now but we could assume that'd be in ms
      this.step = ((int) float_step) + "s";
    } else {
      this.step = step;
    }
  }
  
  /**
   * Parses the given query and turns it into a SemanticQuery builder.
   * @return A non-null semantic query builder if successful. Throws an exception
   * if not.
   */
  public SemanticQuery.Builder parse() {
    final PromQLLexer lexer = new PromQLLexer(new ANTLRInputStream(query));
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    net.opentsdb.prometheus.grammar.PromQLParser parser = 
        new net.opentsdb.prometheus.grammar.PromQLParser(tokens);
    parser.removeErrorListeners(); // suppress logging to stderr.
    parser.setErrorHandler(this);
    
    // parse.
    parser.expression().accept(this);
    
    // TODO - add time shifts if ranges are different and appear in an expression
    // otherwise if timeshifts differ we need to find a way to query just what
    // we need. hmm.
    final SemanticQuery.Builder builder = SemanticQuery.newBuilder()
        .setStart(start)
        .setEnd(end)
        .setMode(QueryMode.SINGLE);
    for (final Entry<String, List<QueryNodeConfig>> sub_query : sub_queries.entrySet()) {
      List<QueryNodeConfig> list = sub_query.getValue();
      for (int i = 0; i < list.size(); i++) {
        builder.addExecutionGraphNode(list.get(i));
      }
    }
    return builder;
  }

  @Override
  public Object visit(ParseTree tree) {
    return tree.accept(this);
  }

  @Override
  public Object visitChildren(RuleNode node) {
    // no idea what this is.
    throw new UnsupportedOperationException("Unimplemented visitor: " + node);
  }

  @Override
  public Object visitTerminal(TerminalNode node) {
    return node.getText();
  }

  @Override
  public Object visitErrorNode(ErrorNode node) {
    throw new UnsupportedOperationException("Unimplemented visitor: " + node);
  }

  @Override
  public Object visitExpression(ExpressionContext ctx) {
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      Object child = ctx.getChild(i).accept(this);
      if (!child.equals("<EOF>")) {
        last = child;
      }
    }
    return last;
  }

  @Override
  public Object visitVectorOperation(VectorOperationContext ctx) {
    // Likely an expression but could be some other ops.
    if (ctx.getChildCount() == 1) {
      // simple
      return ctx.getChild(0).accept(this);
    }
    
    // an expression most definitly I think.
    Object left_operand = ctx.getChild(0).accept(this);
    String left_metric = last_metric_id;
    
    Object operand = ctx.getChild(1).accept(this);
    
    Object right_operand = ctx.getChild(2).accept(this);
    int exp_cnt = expression_counter++;
    String exp_id = "Expression_" + exp_cnt;
    ExpressionConfig.Builder builder = ExpressionConfig.newBuilder()
        .setAs(exp_id)
        .setJoinConfig(join)
        .addInterpolatorConfig(default_interperlator)
        .setId(exp_id);
    List<QueryNodeConfig> srcs = sub_queries.get(left_metric);
    builder.addSource(srcs.get(srcs.size() - 1).getId());
    
    StringBuilder string_builder = new StringBuilder();
    if (left_operand instanceof QueryNodeConfig || 
        (left_operand instanceof String && ((String) left_operand).equals(")"))) {
      string_builder.append(left_metric)
                    .append(" ");
    } else if (left_operand instanceof String) {
      // could be literal
      string_builder.append((String) left_operand)
                    .append(" ");
    } else {
      throw new IllegalStateException("Unknown operand: " + left_operand);
    }
    string_builder.append(operand)
                  .append(" ");
    if (right_operand instanceof QueryNodeConfig) {
      string_builder.append(last_metric_id);
      builder.addSource(((QueryNodeConfig) right_operand).getId());
    } else if (right_operand instanceof String) {
      // could be literal
      string_builder.append((String) right_operand);
    }
    builder.setExpression(string_builder.toString());
    last_metric_id = exp_id;
    ExpressionConfig config = builder.build();
    last_node = config;
    sub_queries.put(last_metric_id, Lists.newArrayList(config));
    return config;
  }

  @Override
  public Object visitUnaryOp(UnaryOpContext ctx) {
    throw new UnsupportedOperationException("Unimplemented visitor: " + ctx);
  }

  @Override
  public Object visitPowOp(PowOpContext ctx) {
    throw new UnsupportedOperationException("Unimplemented visitor: " + ctx);
  }

  @Override
  public Object visitMultOp(MultOpContext ctx) {
    // can be * or / depending on text I believe.
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      last = ctx.getChild(i).accept(this);
    }
    return last;
  }

  @Override
  public Object visitAddOp(AddOpContext ctx) {
    // Add two values.
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      last = ctx.getChild(i).accept(this);
    }
    return last;
  }

  @Override
  public Object visitCompareOp(CompareOpContext ctx) {
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      last = ctx.getChild(i).accept(this);
    }
    return last;
  }

  @Override
  public Object visitAndUnlessOp(AndUnlessOpContext ctx) {
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      last = ctx.getChild(i).accept(this);
    }
    return last;
  }

  @Override
  public Object visitOrOp(OrOpContext ctx) {
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      last = ctx.getChild(i).accept(this);
    }
    return last;
  }

  @Override
  public Object visitVector(VectorContext ctx) {
    // just a pass through.
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      last = ctx.getChild(i).accept(this);
    }
    return last;
  }

  @Override
  public Object visitParens(ParensContext ctx) {
    // skip through.
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      last = ctx.getChild(i).accept(this);
    }
    return last;
  }

  @Override
  public Object visitInstantSelector(InstantSelectorContext ctx) {
    // The metric we're fetching. May be nested in a vector and/or matrix.
    int mc = metric_counter++;
    final String metric_id = "m" + mc;
    final String metric = (String) ctx.getChild(0).accept(this);
    final TimeSeriesDataSourceConfig.Builder builder = 
        DefaultTimeSeriesDataSourceConfig.newBuilder()
          .setMetric(MetricLiteralFilter.newBuilder()
              .setMetric(metric)
              .build())
          .setId(metric_id);
    
    DownsampleConfig ds = null;
    if (!Strings.isNullOrEmpty(step)) {
      ds = DownsampleConfig.newBuilder()
          .setAggregator("avg")
          .setInterval(step)
          .addInterpolatorConfig(default_interperlator)
          .setId(getNodeId(DownsampleConfig.class, last_metric_id))
          .addSource(metric_id)
          .build();
    }
    
    if (ctx.getChildCount() == 1) {
      final TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig) 
          builder.build();
      last_metric_id = config.getId();
      final List<QueryNodeConfig> sub_queries = Lists.newArrayList(config);
      this.sub_queries.put(config.getId(), sub_queries);
      if (ds != null) {
        sub_queries.add(ds);
        last_node = ds;
        return ds;
      }
      last_node = config;
      return config;
    }
    
    Object last = null;
    // Next we can have a matrix selector (range via []) or filters (in curlies: {} )
    for (int i = 1; i < ctx.getChildCount(); i++) {
      String delimiter = (String) ctx.getChild(i).accept(this);
      if (delimiter.equals("{")) {
        i++;
        Object filter = ctx.getChild(i).accept(this);
        builder.setQueryFilter((QueryFilter) filter);
        i++;
        delimiter = (String) ctx.getChild(i).accept(this);
        if (!delimiter.equals("}")) {
          throw new IllegalStateException("Unexpected tag filter delimiter: " 
              + delimiter);
        }
        // filter
      }
    }
    
    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig) builder.build();
    last_metric_id = config.getId();
    final List<QueryNodeConfig> sub_queries = Lists.newArrayList(config);
    this.sub_queries.put(config.getId(), Lists.newArrayList(config));
    if (ds != null) {
      sub_queries.add(ds);
      last_node = ds;
      return ds;
    }
    return config;
  }

  @Override
  public Object visitLabelMatcher(LabelMatcherContext ctx) {
    // A tag filter. 
    // TODO - see if this is a VALUE matcher too! If so... meh
    // otherwise it's a filter.
    // TODO - de-escaping
    String tag_key = (String) ctx.getChild(0).accept(this);
    if (ctx.getChildCount() == 1) {
      // just a tag key filter.
      return TagKeyLiteralOrFilter.newBuilder()
          .setFilter(tag_key)
          .build();
    }
    // get the op next
    String op = (String) ctx.getChild(1).accept(this);
    
    // get the value filter
    // TODO - de-escape
    String value_filter = (String) ctx.getChild(2).accept(this);
    // strip the quotes at the top and end.
    value_filter = value_filter.trim();
    value_filter = value_filter.substring(1, value_filter.length() - 1);
    
    QueryFilter filter = null;
    if (op.equals("=")) {
      filter = TagValueLiteralOrFilter.newBuilder()
          .setKey(tag_key)
          .setFilter(value_filter)
          .build();
    } else if (op.equals("!=")) {
      filter = NotFilter.newBuilder()
          .setFilter(TagValueLiteralOrFilter.newBuilder()
              .setKey(tag_key)
              .setFilter(value_filter)
              .build())
          .build();
    } else if (op.equals("=~")) {
      filter = TagValueRegexFilter.newBuilder()
          .setKey(tag_key)
          .setFilter(anchorRegex(value_filter))
          .build();
    } else if (op.equals("!~")) {
      filter = NotFilter.newBuilder()
          .setFilter(TagValueRegexFilter.newBuilder()
              .setKey(tag_key)
              .setFilter(anchorRegex(value_filter))
              .build())
          .build();
    }
    return filter;
  }

  @Override
  public Object visitLabelMatcherOperator(LabelMatcherOperatorContext ctx) {
    // an op like `=` or `~` or `!~`
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      last = ctx.getChild(i).accept(this);
    }
    return last;
  }

  @Override
  public Object visitLabelMatcherList(LabelMatcherListContext ctx) {
    // chain filter!
    if (ctx.getChildCount() == 1) {
      return ctx.getChild(0).accept(this);
    }
    
    ChainFilter.Builder builder = ChainFilter.newBuilder();
    for (int i = 0; i < ctx.getChildCount(); i += 2) {
      QueryFilter filter = (QueryFilter) ctx.getChild(i).accept(this);
      builder.addFilter(filter);
    }
    return builder.build();
  }

  @Override
  public Object visitMatrixSelector(MatrixSelectorContext ctx) {
    // range selector. 
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      final Object temp = ctx.getChild(i).accept(this);
      if (temp instanceof QueryNodeConfig) {
        last = temp;
      }
      if (temp instanceof String) {
        // assume a range!
        String range = (String) temp;
        metric_ranges.put(last_metric_id, range);
        // check for a downsample
        if (range.contains(":")) {
          // add a DS node!
          String[] splits = range.split(":");
          if (splits.length == 1 || splits[1].equals("]")) {
            // TODO - no-op, they did "1m:" for some reason. Why?
          } else {
            List<QueryNodeConfig> nodes = sub_queries.get(last_metric_id);
            if (nodes.size() >= 2 && nodes.get(1) instanceof DownsampleConfig) {
              final DownsampleConfig.Builder builder = 
                  (DownsampleConfig.Builder) nodes.get(1).toBuilder();
              builder.setInterval(splits[1].substring(0, splits[1].length() - 1));
              if (last_node == nodes.get(1)) {
                last_node = builder.build();
                nodes.set(1, last_node);
              } else {
                nodes.set(1, builder.build());
              }
            } else {
              DownsampleConfig ds = DownsampleConfig.newBuilder()
                  .setAggregator("avg")
                  .setInterval(splits[1].substring(0, splits[1].length() - 1))
                  .addInterpolatorConfig(default_interperlator)
                  .setId(getNodeId(DownsampleConfig.class, last_metric_id))
                  .addSource(last_metric_id)
                  .build();
              nodes.add(ds);
              last_node = ds;
            }
          }
        }
      }
    }
    
    return last;
  }

  @Override
  public Object visitOffset(OffsetContext ctx) {
    // 0 is the metric
    // 1 is the keyword offset.
    // 2 is the duration.
    ctx.getChild(0).accept(this);
    String offset = (String) ctx.getChild(2).accept(this);
    List<QueryNodeConfig> sub_query = sub_queries.get(last_metric_id);
    TimeSeriesDataSourceConfig.Builder builder = 
        (TimeSeriesDataSourceConfig.Builder) sub_query.get(0).toBuilder();
    builder.setTimeShiftInterval(offset);
    sub_query.set(0, builder.build());
    
    return offset;
  }

  @Override
  public Object visitFunction(FunctionContext ctx) {
    // function names, e.g. `rate`
    // NOTE: topk/bottomk are aggregations, not functions. *sigh*
    // function ID is the first child
    final String function_id = (String) ctx.getChild(0).accept(this);
    if (function_id.equalsIgnoreCase("rate")) {
      return handleRate(ctx);
    } else if (function_id.endsWith("_over_time")) {
      return handleOverTime(function_id, ctx);
    } else {
      throw new UnsupportedOperationException("Unimplemented function: " + function_id);
    }
  }

  @Override
  public Object visitParameter(ParameterContext ctx) {
    // function param.
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      last = ctx.getChild(i).accept(this);
    }
    return last;
  }

  @Override
  public Object visitParameterList(ParameterListContext ctx) {
    // list of function params.
    List<Object> parameters = Lists.newArrayList();
    // 0 and the last are the parens.
    for (int i = 1; i < ctx.getChildCount(); i += 2) {
      parameters.add(ctx.getChild(i).accept(this));
    }
    return parameters;
  }

  @Override
  public Object visitAggregation(AggregationContext ctx) {
    // Typically a group by but also the other agg operators from:
    // https://prometheus.io/docs/prometheus/latest/querying/operators/#aggregation-operators
    // looks like 0 is the aggregator
    // and 1 or 2 can be the BY context or the Parameter context.

    String aggregator = ((String) ctx.getChild(0).accept(this)).toLowerCase();
    if (aggregator.equals("sum") ||
        aggregator.equals("min") ||
        aggregator.equals("max") ||
        aggregator.equals("avg") ||
        aggregator.equals("count")) {
      Set<String> keys = null;
      if (ctx.getChildCount() == 2) {
        // group-all
        ctx.getChild(1).accept(this);
      } else {
        if (ctx.getChild(1) instanceof ByContext) {
          keys = (Set<String>) ctx.getChild(1).accept(this);
          ctx.getChild(2).accept(this);  
        } else {
          ctx.getChild(1).accept(this);
          keys = (Set<String>) ctx.getChild(2).accept(this);
        }
        
      }
      // check for other gb's in this sub graph. Could happen.
      List<QueryNodeConfig> sub_graph = sub_queries.get(last_metric_id);
      int gb_count = 0;
      for (int i = 0; i < sub_graph.size(); i++) {
        if (sub_graph.get(i) instanceof GroupByConfig) {
          gb_count++;
        }
      }
      GroupByConfig gb = GroupByConfig.newBuilder()
          .setAggregator(aggregator)
          .setTagKeys(keys)
          .addInterpolatorConfig(default_interperlator)
          .addSource(last_node.getId())
          .setId("group_by_" + gb_count + "_" + last_metric_id)
          .build();
      last_node = gb;
      if (sub_graph == null || sub_graph.isEmpty()) {
        throw new IllegalStateException("Missing subgraph for " + last_metric_id);
      }
      sub_graph.add(gb);
      return gb;
    } else if (aggregator.equals("topk") ||
               aggregator.equals("bottomk")) {
      List<Object> parameters = (List<Object>) ctx.getChild(1).accept(this);
      if (!(parameters.get(0) instanceof String)) {
        throw new IllegalArgumentException("First parameter must be an integer.");
      }
      int count = Integer.parseInt((String) parameters.get(0));
      
      List<QueryNodeConfig> sub_query = sub_queries.get(last_metric_id);
      TopNConfig.Builder builder = TopNConfig.newBuilder()
          .setAggregator("avg") // TODO - not in the docs... look at the code
          .setCount(count)
          .setTop(true)
          .addSource(sub_query.get(sub_query.size() - 1).getId())
          .setId(getNodeId(TopNConfig.class, last_metric_id));
      if (aggregator.equalsIgnoreCase("bottomk")) {
        builder.setTop(false);
      }
      last_node = builder.build();
      sub_query.add(last_node);
      return last_node;
    } else {
      throw new IllegalStateException("Unimplemented aggregation operation " + aggregator);
    }
  }

  @Override
  public Object visitBy(ByContext ctx) {
    // yup, the group by context:
    // sum(rate(http_request_duration_seconds_bucket{le="0.3"}[5m])) by (job)
    // 0 == 'by' keyword
    // 1 == LabelNameListContext
    return ctx.getChild(1).accept(this);
  }

  @Override
  public Object visitWithout(WithoutContext ctx) {
    throw new UnsupportedOperationException("Unimplemented visitor: " + ctx);
  }

  @Override
  public Object visitGrouping(GroupingContext ctx) {
    throw new UnsupportedOperationException("Unimplemented visitor: " + ctx);
  }

  @Override
  public Object visitOn(OnContext ctx) {
    throw new UnsupportedOperationException("Unimplemented visitor: " + ctx);
  }

  @Override
  public Object visitIgnoring(IgnoringContext ctx) {
    // THIS is a join operator wherein we're choosing the tags to match on.
    // I need a way to handle this as the example is:
    // method_code:http_errors:rate5m{code="500"} / ignoring(code) method:http_requests:rate5m
    // wherein for the second instant we are ignoring the "code" tag so we can join across.
    // We can grab the tag keys from the first metrics filters but how does this
    // square with group bys?
    // https://prometheus.io/docs/prometheus/latest/querying/operators/#vector-matching
    throw new UnsupportedOperationException("Unimplemented visitor: " + ctx);
  }

  @Override
  public Object visitGroupLeft(GroupLeftContext ctx) {
    // https://prometheus.io/docs/prometheus/latest/querying/operators/#vector-matching
    // also a join operator
    throw new UnsupportedOperationException("Unimplemented visitor: " + ctx);
  }

  @Override
  public Object visitGroupRight(GroupRightContext ctx) {
    // https://prometheus.io/docs/prometheus/latest/querying/operators/#vector-matching
    // also a join operator
    throw new UnsupportedOperationException("Unimplemented visitor: " + ctx);
  }

  @Override
  public Object visitLabelName(LabelNameContext ctx) {
    // The tag key.
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      last = ctx.getChild(i).accept(this);
    }
    return last;
  }

  @Override
  public Object visitLabelNameList(LabelNameListContext ctx) {
    // a list of tag keys used for a group by operation.
    Set<String> tag_keys = Sets.newHashSet();
    for (int i = 1; i < ctx.getChildCount() - 1; i++) {
      final String param = (String) ctx.getChild(i).accept(this);
      if (param.equals(",")) {
        continue;
      }
      tag_keys.add(param);
    }
    return tag_keys;
  }

  @Override
  public Object visitKeyword(KeywordContext ctx) {
    // not sure yet....
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      last = ctx.getChild(i).accept(this);
    }
    return last;
  }

  @Override
  public Object visitLiteral(LiteralContext ctx) {
    // here's where we get something like `2` as a literal.
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      last = ctx.getChild(i).accept(this);
    }
    return last;
  }

  @Override
  public void recover(final Parser recognizer, final RecognitionException e) {
    final StringBuilder buf = new StringBuilder()
        .append("Error at line (")
        .append(e.getOffendingToken().getLine())
        .append(":")
        .append(e.getOffendingToken().getCharPositionInLine())
        .append(") ");
    if (e instanceof NoViableAltException) {
      final TokenStream tokens = recognizer.getInputStream();
      final NoViableAltException ex = (NoViableAltException) e;
      buf.append("Expression may not be complete. '");
      if (tokens != null) {
        if (ex.getStartToken().getType() == Token.EOF) {
          buf.append("<EOF>");
        } else {
          buf.append(tokens.getText(ex.getStartToken(), ex.getOffendingToken()));
        }
      } else {
        buf.append(query);
      }
      buf.append("'");
    } else if (e instanceof InputMismatchException) {
      final InputMismatchException ex = (InputMismatchException) e;
      buf.append("Missmatched input. Got '")
         .append(getTokenErrorDisplay(ex.getOffendingToken()))
         .append("' but expected '")
         .append(ex.getExpectedTokens().toString(recognizer.getVocabulary()))
         .append("'");
    } else if (e instanceof FailedPredicateException) {
      final FailedPredicateException ex = (FailedPredicateException) e;
      final String rule_name = recognizer
          .getRuleNames()[recognizer.getContext().getRuleIndex()];
      buf.append(" Predicate error with rule '")
         .append(rule_name)
         .append("' ")
         .append(ex.getMessage());
    } else {
      buf.append("Unexpected error at token '")
         .append(getTokenErrorDisplay(e.getOffendingToken()))
         .append("'");
    }

    for (ParserRuleContext context = recognizer.getContext();
        context != null; context = context.getParent()) {
      context.exception = e;
    }
    throw new ParseCancellationException(buf.toString(), e);
  }

  /** Make sure we don't attempt to recover inline; if the parser
   *  successfully recovers, it won't throw an exception.
   */
  @Override
  public Token recoverInline(final Parser recognizer)
      throws RecognitionException {
    final InputMismatchException e = new InputMismatchException(recognizer);
    recover(recognizer, e);
    return null;
  }

  /** Make sure we don't attempt to recover from problems in subrules. */
  @Override
  public void sync(final Parser recognizer) { }

  /**
   * Parses a duration, e.g. [5m] or [5m:1m].
   * @param duration A non-null duration to parse.
   * @return The duration in milliseconds.
   */
  static long parseDuration(final String duration) {
    // can be concatenated.
    long parsed = 0;
    String temp = "";
    for (int i = 1; i < 
        (duration.charAt(duration.length() - 1) == ']' ? duration.length() - 1 : duration.length()); i++) {
      final char c = duration.charAt(i);
      if (c == ':') {
        // resolution so break
        break;
      }
      if (Character.isDigit(c)) {
        temp += duration.charAt(i);
      } else {
        long digits = Long.parseLong(temp);
        switch (c) {
        case 'm':
          if (i + 1 < duration.length() && duration.charAt(i + 1) == 's') {
            // ms
            parsed += digits;
          } else {
            // minutes
            parsed += (digits * 60L * 1000L);
          }
          break;
        case 's':
          parsed += (digits * 1000L);
          break;
        case 'h':
          parsed += (digits * 60L * 60L * 1000L);
          break;
        case 'd':
          parsed += digits * 86400L * 1000L;
          break;
        case 'w':
          parsed += digits * 7L * 86400L * 1000L;
          break;
        case 'y':
          parsed += digits * 365L * 86400L * 1000L;
          break;
        default:
          throw new IllegalArgumentException("Unhandled duration suffix [" + c 
              + "] from " + duration);
        }
      }
    }
    return parsed;
  }

  /**
   * Pulls out just the duration from a range in case it includes a downsample.
   * @param range The non-null range to parse.
   * @return The parsed duration.
   */
  static String durationFromRange(final String range) {
    int index = range.indexOf(":");
    if (index > -1) {
      return range.substring(1, index);
    }
    return range.substring(1, range.length() - 1);
  }
  
  /**
   * Pulls out the resolution (downsample) for the query, i.e. the bit after the
   * colon.
   * @param range The non-null range to parse.
   * @return The resolution if found.
   */
  static String resolutionFromRange(final String range) {
    int index = range.indexOf(":");
    if (index < 0) {
      throw new IllegalStateException("Missing resolution from range: " + range);
    }
    return range.substring(index + 1, range.length() - 1);
  }

  /**
   * Helper to check the regex for anchors and append them if needed.
   * @param pattern The non-null pattern to validate.
   * @return The updated pattern.
   */
  static String anchorRegex(final String pattern) {
    if (pattern.startsWith("^")) {
      if (pattern.endsWith("$")) {
        return pattern;
      }
      return pattern + "$";
    } else if (pattern.endsWith("$")) {
      return "^" + pattern;
    } else {
      return "^" + pattern + "$";
    }
  }
  
  /**
   * Handles setting up a rate and adjusts the range and downsample if required.
   * @param ctx The non-null context to parse.
   * @return the last node parsed.
   */
  Object handleRate(final FunctionContext ctx) {
    // 1 == (
    // 2 == instant
    String range = null;
    ctx.getChild(2).accept(this);
    // 3 == )
    if (ctx.getChildCount() >= 5) {
      // 4 == a range and resolution
      range = (String) ctx.getChild(4).accept(this);
    }
    
    String resolution = null;
    String interval = "1s";
    if (!Strings.isNullOrEmpty(range)) {
      // fun is... The resolution is the previous "ragne" (WTF?) and the new
      // query range is THIS range with a downsample....
      String existing_range = metric_ranges.get(last_metric_id);
      if (Strings.isNullOrEmpty(existing_range)) {
        throw new IllegalStateException("Missing the existing range for " + last_metric_id);
      }
      interval = durationFromRange(existing_range);
      resolution = resolutionFromRange(range);
      metric_ranges.put(last_metric_id, range);
    }
    
    QueryNodeConfig return_value;
    final String id = getNodeId(RateConfig.class, last_metric_id);
    RateConfig.Builder rate_builder = RateConfig.newBuilder()
        .setInterval("1s") // always 1s apparently.
        .setCounter(true)  // and always a counter
        .setDropResets(true)
        .setId(id);
    List<QueryNodeConfig> sub_query = sub_queries.get(last_metric_id);
    if (sub_query.size() == 2 && 
        sub_query.get(1) instanceof DownsampleConfig) {
      rate_builder.addSource(sub_query.get(0).getId());
      
      // rebuild downsample as we have a new resolution
      DownsampleConfig rebuilt = ((DownsampleConfig) sub_query.get(1)).toBuilder()
          .setInterval(resolution)
          .addSource(id)
          .build();
      sub_query.set(1, rebuilt);
      
      // insert between the metric and downsampler and note that we may change it
      return_value = rate_builder.build();
      sub_query.add(1, return_value);
    } else {
      // just appending
      rate_builder.addSource(sub_query.get(sub_query.size() - 1).getId());
      return_value = rate_builder.build();
      sub_query.add(return_value);
      last_node = return_value;
      
      if (range != null) {
        DownsampleConfig ds = DownsampleConfig.newBuilder()
            .setAggregator("avg")
            .setInterval(resolution)
            .addInterpolatorConfig(default_interperlator)
            .setId(getNodeId(DownsampleConfig.class, last_metric_id))
            .addSource(return_value.getId())
            .build();
        
        return_value = ds;
        sub_query.add(ds);
        last_node = return_value;
      }
    }
    
    return return_value;
  }
  
  /**
   * Parses a downsample-all node from the *_over_time function.
   * @param name The non-null name of the function (to pull out the aggregator)
   * @param ctx The non-null context.
   * @return The last node parsed.
   */
  Object handleOverTime(final String name, final FunctionContext ctx) {
    String[] components = name.split("_");
    // 0 == function
    // 1 == open parens
    // 2 == range vector
    // 3 == close parens
    ctx.getChild(2).accept(this);
    
    if (components[0].equals("stddev")) {
      components[0] = "dev";
    } else if (!components[0].equals("avg") && 
               !components[0].equals("min") &&
               !components[0].equals("max") &&
               !components[0].equals("sum") &&
               !components[0].equals("count")) {
      throw new IllegalArgumentException("No implementation yet over_time for " 
               + components[0]);
    }
    
    List<QueryNodeConfig> sub_query = sub_queries.get(last_metric_id);
    DownsampleConfig.Builder builder = DownsampleConfig.newBuilder()
        .setAggregator(components[0])
        .setRunAll(true)
        .setInterval("0all")
        .addSource(sub_query.get(sub_query.size() - 1).getId())
        .addInterpolatorConfig(default_interperlator)
        .setId(getNodeId(DownsampleConfig.class, last_metric_id));
    last_node = builder.build();
    sub_query.add(last_node);
    return last_node;
    
  }
  
  /**
   * Helper to increment the node type counter and generate a useful node ID. 
   * @param clazz The non-null class we're building a node for.
   * @param metric_id The non-null metric ID.
   * @return The non-null node ID to use.
   */
  String getNodeId(final Class clazz, final String metric_id) {
    Integer counter = id_counters.get(clazz);
    if (counter == null) {
      id_counters.put(clazz, 1);
      return clazz.getSimpleName() + "_" + metric_id;
    }
    
    String id = clazz.getSimpleName() + "_" + counter + "_" + metric_id;
    id_counters.put(clazz, counter + 1);
    return id;
  }

}
