// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
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
package net.opentsdb.data.iterators;

import com.stumbleupon.async.Deferred;

/**
 * Represents a time series source that returns a new {@link TimeSeriesReadOnlyView}
 * for iteration each time {@link #newView(IteratorOptions)} is called.
 * 
 * All stages in a query pipeline should implement this interface so that they
 * can be connected.
 *  
 * @since 3.0
 */
public interface TimeSeriesSource {
  
  /**
   * Generates a new {@link TimeSeriesReadOnlyView} from the source.
   * 
   * If the implementing source must fetch data from a remote location it should 
   * do so asynchronously and callback the deferred once the data is returned.
   * 
   * Note that multiple sinks may call this method to fetch a set of views.
   * The views returned must be independent per call but they can all operate
   * on the same set of source data.
   * 
   * WARNING: Implementations of this method must be thread safe.
   * 
   * @param options A non-null options object for configuring iterators returned
   * by the set.
   * @return A deferred that will be called back once the new set is ready.
   */
  public Deferred<TimeSeriesReadOnlyView> newView(final IteratorOptions options);
}
