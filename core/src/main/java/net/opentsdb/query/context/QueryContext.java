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
package net.opentsdb.query.context;

import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.query.processor.TimeSeriesProcessor;

/**
 * TODO - complete and doc
 * 
 * @since 3.0
 */
public abstract class QueryContext {

  public TimeStamp syncTimestamp() {
    return null;
  }
  
  public void updateContext(final IteratorStatus status, 
      final TimeStamp timestamp) {
    
  }
  
  public void register(final TimeSeriesIterator<?> it) {
    
  }
  
  public void unregister(final TimeSeriesIterator<?> it) {
    
  }
  
  public void register(final TimeSeriesProcessor processor) {
    
  }
  
  public void unregister(final TimeSeriesProcessor processor) {
    
  }
}
