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

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.query.context.QueryContext;

/**
 * A simple iterator that stores and iterates over a set 
 * {@link TimeSeriesIterator}s synchronized on time.
 *  *  
 * @since 3.0
 */
public class IteratorGroup extends TimeSeriesProcessor implements 
  Callback<Deferred<Object>, Exception> {

  @Override
  public TimeSeriesProcessor getClone(final QueryContext context) {
    final IteratorGroup new_iterator = new IteratorGroup();
    new_iterator.iterators = iterators.getClone(context);
    return new_iterator;
  }

  @Override
  public Deferred<Object> call(final Exception ex) throws Exception {
    return Deferred.fromError(ex);
  }

}
