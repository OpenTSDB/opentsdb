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

import java.util.List;

import net.opentsdb.core.TSQuery;

/**
 * Static class with helpers to parse and deal with expressions
 * @since 2.3
 */
public class Expressions {

  /** No instantiation for you! */
  private Expressions() { }
  
  /**
   * Parses an expression into a tree
   * @param expression The expression to parse (as a string)
   * @param metric_queries A list to store the parsed metrics in
   * @param data_query The time series query
   * @return The parsed tree ready for evaluation
   * @throws IllegalArgumentException if the expression was null, empty or 
   * invalid.
   * @throws UnsupportedOperationException if the requested function couldn't
   * be found.
   */
  public static ExpressionTree parse(final String expression,
                                     final List<String> metric_queries,
                                     final TSQuery data_query) {
    if (expression == null || expression.isEmpty()) {
      throw new IllegalArgumentException("Expression may not be null or empty");
    }
    if (expression.indexOf('(') == -1 || expression.indexOf(')') == -1) {
      throw new IllegalArgumentException("Invalid Expression: " + expression);
    }

    final ExpressionReader reader = new ExpressionReader(expression.toCharArray());
    // consume any whitespace ahead of the expression
    reader.skipWhitespaces();

    final String function_name = reader.readFuncName();
    final Expression root_expression = ExpressionFactory.getByName(function_name);

    final ExpressionTree root = new ExpressionTree(root_expression, data_query);
    reader.skipWhitespaces();
    
    if (reader.peek() == '(') {
      reader.next();
      parse(reader, metric_queries, root, data_query);
    }

    return root;
  }

  /**
   * Helper to parse out the function(s) and parameters
   * @param reader The reader used for iterating over the expression
   * @param metric_queries A list to store the parsed metrics in
   * @param root The root tree
   * @param data_query The time series query
   */
  private static void parse(final ExpressionReader reader, 
                            final List<String> metric_queries,
                            final ExpressionTree root,
                            final TSQuery data_query) {

    int parameter_index = 0;
    reader.skipWhitespaces();
    if (reader.peek() != ')') {
      final String param = reader.readNextParameter();
      parseParam(param, metric_queries, root, data_query, parameter_index++);
    }

    while (!reader.isEOF()) {
      reader.skipWhitespaces();
      if (reader.peek() == ')') {
        return;
      } else if (reader.isNextSeq(",,")) {
        reader.skip(2); //swallow the ",," delimiter
        reader.skipWhitespaces();
        final String param = reader.readNextParameter();
        parseParam(param, metric_queries, root, data_query, parameter_index++);
      } else {
        throw new IllegalArgumentException("Invalid delimiter in parameter " +
                "list at pos=" + reader.getMark() + ", expr="
                + reader.toString());
      }
    }
  }

  /**
   * Helper that parses out the parameter from the expression
   * @param param The parameter to parse
   * @param metric_queries A list to store the parsed metrics in
   * @param root The root tree
   * @param data_query The time series query
   * @param index Index of the parameter
   */
  private static void parseParam(final String param, 
                                 final List<String> metric_queries,
                                 final ExpressionTree root, 
                                 final TSQuery data_query, 
                                 final int index) {
    if (param == null || param.length() == 0) {
      throw new IllegalArgumentException("Parameter cannot be null or empty");
    }

    if (param.indexOf('(') > 0 && param.indexOf(')') > 0) {
      // sub expression
      final ExpressionTree sub_tree = parse(param, metric_queries, data_query);
      root.addSubExpression(sub_tree, index);
    } else if (param.indexOf(':') >= 0) {
      // metric query
      metric_queries.add(param);
      root.addSubMetricQuery(param, metric_queries.size() - 1, index);
    } else {
      // expression parameter
      root.addFunctionParameter(param);
    }
  }

}

