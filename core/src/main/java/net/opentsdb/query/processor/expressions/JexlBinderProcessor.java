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

import net.opentsdb.query.processor.AbstractPassThroughProcessor;
import net.opentsdb.query.processor.TimeSeriesProcessor;
import net.opentsdb.query.processor.TimeSeriesProcessorConfig;

/**
 * An expression processor that allows for flexible custom mutations of 
 * time series.
 * TODO - more docs and work on this.
 * 
 * @since 3.0
 */
public class JexlBinderProcessor extends 
    AbstractPassThroughProcessor<JexlBinderProcessor> {

  /**
   * Default Ctor used by the registry.
   * @param source A non-null source to operate on.
   * @param config An optional config.
   */
  public JexlBinderProcessor(TimeSeriesProcessor source,
      TimeSeriesProcessorConfig<JexlBinderProcessor> config) {
    super(source, config);
  }
  
  /**
   * Copy constructor that sets the parent reference
   * @param source A non-null source to operate on.
   * @param config An optional config.
   * @param parent A non-null parent.
   */
  private JexlBinderProcessor(final TimeSeriesProcessor source,
      final TimeSeriesProcessorConfig<JexlBinderProcessor> config, 
      final TimeSeriesProcessor parent) {
    super(source, config, parent);
  }

  @Override
  public TimeSeriesProcessor getCopy() {
    return new JexlBinderProcessor(source.getCopy(), config, this);
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
