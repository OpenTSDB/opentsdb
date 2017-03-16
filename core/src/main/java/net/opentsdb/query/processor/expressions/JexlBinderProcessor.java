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
package net.opentsdb.query.processor.expressions;

import com.stumbleupon.async.Deferred;

import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.processor.TimeSeriesProcessor;
import net.opentsdb.query.processor.TimeSeriesProcessorConfig;

/**
 * An expression processor that allows for flexible custom mutations of 
 * time series.
 * TODO - more docs and work on this.
 * 
 * @since 3.0
 */
public class JexlBinderProcessor extends TimeSeriesProcessor {
  private final TimeSeriesProcessor source;
  
  /**
   * Default Ctor used by the registry.
   * @param source A non-null source to operate on.
   * @param config An optional config.
   */
  public JexlBinderProcessor(final TimeSeriesProcessor source,
      final TimeSeriesProcessorConfig<JexlBinderProcessor> config) {
    super(config);
    this.source = source;
  }
  
  @Override
  public TimeSeriesProcessor getClone(final QueryContext context) {
    return new JexlBinderProcessor(source.getClone(context), 
        config != null ? (TimeSeriesProcessorConfig<JexlBinderProcessor>) config : null);
  }

  @Override
  public Deferred<Object> initialize() {
    if (source == null) {
      return Deferred.fromError(new IllegalStateException("Source cannot be null."));
    }
    
    // TODO - complete me!
    return Deferred.fromResult(null);
  }
  
}
