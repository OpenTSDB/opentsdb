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

import net.opentsdb.core.DataPoints;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.query.expression.VariableIterator.SetOperator;

/**
 * Performs a UNION set join on up to 26 metric query results and returns the 
 * quotient.
 */
public class DivideSeries implements Expression {
  /** The TSDB used for UID to name lookups */
  final TSDB tsdb;
  
  /**
   * Default ctor.
   * @param tsdb The TSDB used for UID to name lookups
   */
  public DivideSeries(final TSDB tsdb) {
    this.tsdb = tsdb;
  }
  
  @Override
  public DataPoints[] evaluate(final TSQuery data_query, 
      final List<DataPoints[]> query_results, final List<String> params) {
    if (data_query == null) {
      throw new IllegalArgumentException("Missing time series query");
    }
    if (query_results == null || query_results.isEmpty()) {
      return new DataPoints[]{};
    }
    
    if (query_results.size() < 2 || query_results.size() > 26) {
      throw new IllegalArgumentException("Must have 2 to 26 series, got " + 
          query_results.size() + " instead");
    }
        
    final StringBuilder buf = new StringBuilder();
    char v = 'a';
    for (int i = 0; i < query_results.size(); i++) {
      buf.append(v++);
      if (i < query_results.size() - 1) {
        buf.append(" / ");
      }
    }
    
    final ExpressionIterator expression = new ExpressionIterator("divideSeries", 
        buf.toString(), SetOperator.UNION, false, false);
    v = 'a';
    
    for (final DataPoints[] dps : query_results) {
      final TimeSyncedIterator it = new TimeSyncedIterator(
          Character.toString(v++), null, dps);
      expression.addResults(it.getId(), it);
    }
    expression.compile();
    
    final DataPoints[] results = new DataPoints[expression.values().length];
    for (int i = 0; i < expression.values().length; i++) {
      results[i] = new EDPtoDPS(tsdb, i, expression);
    }
    return results;
  }

  @Override
  public String writeStringField(final List<String> query_params, 
      final String inner_expression) {
    return "divideSeries(" + inner_expression + ")";
  }

}
