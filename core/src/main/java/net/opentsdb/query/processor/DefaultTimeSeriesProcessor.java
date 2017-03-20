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
package net.opentsdb.query.processor;

import net.opentsdb.query.context.QueryContext;

/**
 * Base implementation of a processor that allows for manually adding 
 * iterators. Just implements the clone method.
 * 
 * @since 3.0
 */
public class DefaultTimeSeriesProcessor extends TimeSeriesProcessor {

  @Override
  public TimeSeriesProcessor getClone(final QueryContext context) {
    final DefaultTimeSeriesProcessor clone = new DefaultTimeSeriesProcessor();
    clone.iterators = iterators.getClone(context);
    clone.setContext(context);
    return clone;
  }

}
