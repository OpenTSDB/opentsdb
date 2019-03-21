// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.timeshift;

import java.time.temporal.TemporalAmount;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.utils.Pair;

/**
 * A node for wrapping QueryResults and shifting the timestamps to align with
 * the query for period over period comparisons.
 * 
 * @since 3.0
 */
public class TimeShift extends AbstractQueryNode {
  private static final Logger LOG = LoggerFactory.getLogger(TimeShift.class);
  
  private final TimeShiftConfig config;
  
  public TimeShift(final QueryNodeFactory factory, 
                   final QueryPipelineContext context,
                   final QueryNodeConfig config) {
    super(factory, context);
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    this.config = (TimeShiftConfig) config;
  }

  @Override
  public QueryNodeConfig config() {
    return config;
  }
  
  @Override
  public void close() {
    // TODO Auto-generated method stub
  }
  
  @Override
  public void onNext(final QueryResult next) {
    final Pair<Boolean, TemporalAmount> amount = 
        config.getConfig().timeShifts().get(next.dataSource());
    if (amount == null) {
      // whoops. We got something we don't deal with.
      LOG.warn("Received a result at node " + config.getId() 
        + " that we didn't expect: " + next.source().config().getId() + ":" 
          + next.dataSource());
      return;
    }
    final TimeShiftResult results = new TimeShiftResult(
        this, 
        next, 
        amount.getKey(), 
        amount.getValue());
    sendUpstream(results);
  }
  
}
