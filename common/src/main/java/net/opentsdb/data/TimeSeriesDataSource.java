//This file is part of OpenTSDB.
//Copyright (C) 2017  The OpenTSDB Authors.
//
//This program is free software: you can redistribute it and/or modify it
//under the terms of the GNU Lesser General Public License as published by
//the Free Software Foundation, either version 2.1 of the License, or (at your
//option) any later version.  This program is distributed in the hope that it
//will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
//of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
//General Public License for more details.  You should have received a copy
//of the GNU Lesser General Public License along with this program.  If not,
//see <http://www.gnu.org/licenses/>.
package net.opentsdb.data;

import net.opentsdb.query.QueryNode;

/**
 * The base interface for a data source such as a local database or remote TSD.
 * 
 * @since 3.0
 */
public interface TimeSeriesDataSource extends QueryNode {

  /**
   * Called by the upstream context or nodes to fetch the next set of data.
   */
  public void fetchNext();
  
}
