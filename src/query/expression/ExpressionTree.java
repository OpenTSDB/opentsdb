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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.TSQuery;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExpressionTree {

  private final Expression expr;
  private final TSQuery data_query;

  private List<ExpressionTree> subExpressions;
  private List<String> funcParams;
  private Map<Integer, String> subMetricQueries;
  private Map<Integer, Parameter> parameterSourceIndex = Maps.newHashMap();

  private static final Joiner DOUBLE_COMMA_JOINER = Joiner.on(",").skipNulls();

  enum Parameter {
    SUB_EXPRESSION,
    METRIC_QUERY
  }

  public ExpressionTree(String exprName, TSQuery data_query) {
    this(ExpressionFactory.getByName(exprName), data_query);
  }

  public ExpressionTree(Expression expr, TSQuery data_query) {
    this.expr = expr;
    this.data_query = data_query;
  }

  public void addSubExpression(ExpressionTree child, int paramIndex) {
    if (subExpressions == null) {
      subExpressions = Lists.newArrayList();
    }
    subExpressions.add(child);
    parameterSourceIndex.put(paramIndex, Parameter.SUB_EXPRESSION);
  }

  public void addSubMetricQuery(String metricQuery, int magic,
                                int paramIndex) {
    if (subMetricQueries == null) {
      subMetricQueries = Maps.newHashMap();
    }
    subMetricQueries.put(magic, metricQuery);
    parameterSourceIndex.put(paramIndex, Parameter.METRIC_QUERY);
  }

  public void addFunctionParameter(String param) {
    if (funcParams == null) {
      funcParams = Lists.newArrayList();
    }
    funcParams.add(param);
  }

  public DataPoints[] evaluate(List<DataPoints[]> queryResults) {
    List<DataPoints[]> materialized = Lists.newArrayList();
    List<Integer> metricQueryKeys = null;
    if (subMetricQueries != null && subMetricQueries.size() > 0) {
      metricQueryKeys = Lists.newArrayList(subMetricQueries.keySet());
      Collections.sort(metricQueryKeys);
    }

    int metricPointer = 0;
    int subExprPointer = 0;
    for (int i=0; i<parameterSourceIndex.size(); i++) {
      Parameter p = parameterSourceIndex.get(i);

      if (p == Parameter.METRIC_QUERY) {
        if (metricQueryKeys == null) {
          throw new RuntimeException("Attempt to read metric " +
                  "results when none exist");
        }

        int ix = metricQueryKeys.get(metricPointer++);
        materialized.add(queryResults.get(ix));
      }
      else if (p == Parameter.SUB_EXPRESSION) {
        ExpressionTree st = subExpressions.get(subExprPointer++);
        materialized.add(st.evaluate(queryResults));
      } else {
        throw new RuntimeException("Unknown value: " + p);
      }
    }

    return expr.evaluate(data_query, materialized, funcParams);
  }

  public String toString() {
    return writeStringField();
  }

  public String writeStringField() {
    List<String> strs = Lists.newArrayList();
    if (subExpressions != null) {
      for (ExpressionTree sub : subExpressions) {
        strs.add(sub.toString());
      }
    }

    if (subMetricQueries != null) {
      String subMetrics = clean(subMetricQueries.values());
      if (subMetrics != null && subMetrics.length() > 0) {
        strs.add(subMetrics);
      }
    }

    String innerExpression = DOUBLE_COMMA_JOINER.join(strs);
    return expr.writeStringField(funcParams, innerExpression);
  }

  private String clean(Collection<String> values) {
    if (values == null || values.size() == 0) {
      return "";
    }

    List<String> strs = Lists.newArrayList();
    for (String v : values) {
      String tmp = v.replaceAll("\\{.*\\}", "");
      int ix = tmp.lastIndexOf(':');
      if (ix < 0) {
        strs.add(tmp);
      } else {
        strs.add(tmp.substring(ix+1));
      }
    }

    return DOUBLE_COMMA_JOINER.join(strs);
  }

}