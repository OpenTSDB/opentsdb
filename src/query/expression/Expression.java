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
import net.opentsdb.core.TSQuery;

/**
 * The interface for various expressions/functions used when querying OpenTSDB.
 * @since 2.3
 */
public interface Expression {

  /**
   * Computes a set of results given the results of a {@link TSQuery} that may
   * include multiple metrics and/or group by result sets.
   * @param data_query The original query from the user
   * @param results The results of the query
   * @param params Parameters parsed from the expression endpoint related to
   * the implementing function
   * @return An array of data points resulting from the implementation
   */
  public DataPoints[] evaluate(TSQuery data_query, 
      List<DataPoints[]> results, List<String> params);

  /**
   * TODO - document me!
   * @param params
   * @param inner_expression
   * @return
   */
  public String writeStringField(List<String> params, String inner_expression);
  
}
