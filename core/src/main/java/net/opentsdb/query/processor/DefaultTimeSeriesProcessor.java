// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.query.processor;

import net.opentsdb.query.context.QueryContext;

/**
 * Base implementation of a processor that allows for manually adding 
 * iterators. Just implements the clone method.
 * 
 * @since 3.0
 */
public class DefaultTimeSeriesProcessor extends TimeSeriesProcessor {

  public DefaultTimeSeriesProcessor(final QueryContext context) {
    super(context);
  }
  
  @Override
  public TimeSeriesProcessor getClone(final QueryContext context) {
    final DefaultTimeSeriesProcessor clone = new DefaultTimeSeriesProcessor(context);
    clone.iterators = iterators.getCopy(context);
    return clone;
  }

}
