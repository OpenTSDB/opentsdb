// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.execution.cache;

import net.opentsdb.query.pojo.TimeSeriesQuery;

public abstract class TimeSeriesCacheKeyGenerator {

  /**
   * Generates a cache key based on the given query and whether or not to
   * include the time.
   * @param query A non-null query to generate a hash from.
   * @param with_timestamps Whether or not to include times when generating
   * the key.
   * @return A cache key.
   */
  public abstract byte[] generate(final TimeSeriesQuery query, 
                                  final boolean with_timestamps);
  
}
