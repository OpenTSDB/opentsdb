// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.query.expression;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.core.DataPoints;
import net.opentsdb.core.IllegalDataException;
import net.opentsdb.core.TSQuery;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A node in a tree of nested expressions. The tree may link to other nodes as
 * sub expressions. Evaluating the tree evaluates all sub expressions.
 * <p>
 * Before calling {@link evaluate} you MUST call a one or a combination of
 * {@link addSubExpression}, {@link addSubMetricQuery} and optionally 
 * {@link addFunctionParameter}
 * <p>
 * TODO(cl) - Cleanup needed. Tracking the indices can likely be done better 
 * and it would be good to have a ctor that sets the sub or metric query. 
 * @since 2.3
 */
public class ExpressionTree {
  /** Used for the toString() helpers */
  private static final Joiner DOUBLE_COMMA_JOINER = Joiner.on(",").skipNulls();
  
  /** An enumerator of the different query types */
  enum Parameter {
    SUB_EXPRESSION,
    METRIC_QUERY
  }
  
  /** The root expression for the tree */
  private final Expression expression;
  /** The original time series query */
  private final TSQuery data_query;
  /** An optional list of sub expressions */
  private List<ExpressionTree> sub_expressions;
  /** A list of parameters for the root expression */
  private List<String> func_params;
  /** A mapping of result indices to sub metric queries */
  private Map<Integer, String> sub_metric_queries;
  /** A mapping of query types to their result index */
  private Map<Integer, Parameter> parameter_index = Maps.newHashMap();

  /**
   * Creates a tree with a root and no children
   * @param expression_name The name of the expression to lookup in the factory
   * @param data_query The original query
   * @throws UnsupportedOperationException if the expression is not implemented
   */
  public ExpressionTree(final String expression_name, final TSQuery data_query) {
    this(ExpressionFactory.getByName(expression_name), data_query);
  }
  
  /**
   * Creates a tree with a root and no children
   * @param expression The expression to use
   * @param data_query The original query
   */
  public ExpressionTree(final Expression expression, final TSQuery data_query) {
    this.expression = expression;
    this.data_query = data_query;
  }

  public void addSubExpression(final ExpressionTree child, final int param_index) {
    if (child == null) {
      throw new IllegalArgumentException("Cannot add a null child tree");
    }
    if (child == this) {
      throw new IllegalDataException("Recursive sub expression detected: " 
          + this);
    }
    if (param_index < 0) {
      throw new IllegalArgumentException("Parameter index must be 0 or greater");
    }
    if (sub_expressions == null) {
      sub_expressions = Lists.newArrayList();
    }
    sub_expressions.add(child);
    parameter_index.put(param_index, Parameter.SUB_EXPRESSION);
  }

  /**
   * Sets the metric query key and index, setting the Parameter type to 
   * METRIC_QUERY
   * @param metric_query The metric query id
   * @param sub_query_index The index of the metric query
   * @param param_index The index of the parameter (??) 
   */
  public void addSubMetricQuery(final String metric_query, 
                                final int sub_query_index,
                                final int param_index) {
    if (metric_query == null || metric_query.isEmpty()) {
      throw new IllegalArgumentException("Metric query cannot be null or empty");
    }
    if (sub_query_index < 0) {
      throw new IllegalArgumentException("Sub query index must be 0 or greater");
    }
    if (param_index < 0) {
      throw new IllegalArgumentException("Parameter index must be 0 or greater");
    }
    if (sub_metric_queries == null) {
      sub_metric_queries = Maps.newHashMap();
    }
    sub_metric_queries.put(sub_query_index, metric_query);
    parameter_index.put(param_index, Parameter.METRIC_QUERY);
  }
  
  /**
   * Adds parameters for the root expression only.
   * @param param The parameter to add, cannot be null or empty
   * @throws IllegalArgumentException if the parameter is null or empty
   */
  public void addFunctionParameter(final String param) {
    if (param == null || param.isEmpty()) {
      throw new IllegalArgumentException("Parameter cannot be null or empty");
    }
    if (func_params == null) {
      func_params = Lists.newArrayList();
    }
    func_params.add(param);
  }

  /**
   * Processes the expression tree, including sub expressions, and returns the 
   * results.
   * TODO(cl) - More tests around indices, etc. This can likely be cleaned up.
   * @param query_results The result set to pass to the expressions
   * @return The result set or an exception will bubble up if something wasn't
   * configured properly.
   */
  public DataPoints[] evaluate(final List<DataPoints[]> query_results) {
    // TODO - size the array
    final List<DataPoints[]> materialized = Lists.newArrayList();
    List<Integer> metric_query_keys = null;
    if (sub_metric_queries != null && sub_metric_queries.size() > 0) {
      metric_query_keys = Lists.newArrayList(sub_metric_queries.keySet());
      Collections.sort(metric_query_keys);
    }

    int metric_pointer = 0;
    int sub_expression_pointer = 0;
    for (int i = 0; i < parameter_index.size(); i++) {
      final Parameter param = parameter_index.get(i);

      if (param == Parameter.METRIC_QUERY) {
        if (metric_query_keys == null) {
          throw new RuntimeException("Attempt to read metric " +
                  "results when none exist");
        }

        final int ix = metric_query_keys.get(metric_pointer++);
        materialized.add(query_results.get(ix));
      } else if (param == Parameter.SUB_EXPRESSION) {
        final ExpressionTree st = sub_expressions.get(sub_expression_pointer++);
        materialized.add(st.evaluate(query_results));
      } else {
        throw new IllegalDataException("Unknown parameter type: " + param 
            + " in tree: " + this);
      }
    }
    
    return expression.evaluate(data_query, materialized, func_params);
  }
  
  @Override
  public String toString() {
    return writeStringField();
  }

  /**
   * Helper to create the original expression (or at least a nested expression
   * without the parameters included)
   * @return A string representing the full expression.
   */
  public String writeStringField() {
    final List<String> strs = Lists.newArrayList();
    if (sub_expressions != null) {
      for (ExpressionTree sub : sub_expressions) {
        strs.add(sub.toString());
      }
    }

    if (sub_metric_queries != null) {
      final String sub_metrics = clean(sub_metric_queries.values());
      if (sub_metrics != null && sub_metrics.length() > 0) {
        strs.add(sub_metrics);
      }
    }

    final String inner_expression = DOUBLE_COMMA_JOINER.join(strs);
    return expression.writeStringField(func_params, inner_expression);
  }

  /**
   * Helper to clean out some characters
   * @param values The collection of strings to cleanup
   * @return An empty string if values was empty or a cleaned up string
   */
  private String clean(final Collection<String> values) {
    if (values == null || values.size() == 0) {
      return "";
    }

    final List<String> strs = Lists.newArrayList();
    for (String v : values) {
      final String tmp = v.replaceAll("\\{.*\\}", "");
      final int ix = tmp.lastIndexOf(':');
      if (ix < 0) {
        strs.add(tmp);
      } else {
        strs.add(tmp.substring(ix+1));
      }
    }

    return DOUBLE_COMMA_JOINER.join(strs);
  }

  @VisibleForTesting
  List<ExpressionTree> subExpressions() {
    return sub_expressions;
  }
  
  @VisibleForTesting
  List<String> funcParams() {
    return func_params;
  }
  
  @VisibleForTesting
  Map<Integer, String> subMetricQueries() {
    return sub_metric_queries;
  }
  
  @VisibleForTesting
  Map<Integer, Parameter> parameterIndex() {
    return parameter_index;
  }
}