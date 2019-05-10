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
package net.opentsdb.storage.schemas.tsdb1x;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.pools.CloseablePooledObject;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;

/**
 * A runnable used to schedule a query task dealing with PTS data.
 * 
 * @since 3.0
 */
public class PooledPartialTimeSeriesRunnable implements Runnable, CloseablePooledObject {
  private static final Logger LOG = LoggerFactory.getLogger(
      PooledPartialTimeSeriesRunnable.class);
  
  /** The pooled obj. ref */
  protected PooledObject pooled_object;
  
  /** The current data set to send up. */
  protected PartialTimeSeries pts;
  
  /** The upstream node to write to. */
  protected QueryNode node;
  
  /**
   * Resets the pooled state.
   * @param context The non-null query context
   * @param pts The non-null time series data.
   * @param node The non-null query node.
   */
  public void reset(final PartialTimeSeries pts, 
                    final QueryNode node) {
    this.pts = pts;
    this.node = node;
  }

  @Override
  public void run() {
    try {
      if (pts != null && pts instanceof Tsdb1xPartialTimeSeries) {
        // TODO - order and keeper settings.
        ((Tsdb1xPartialTimeSeries) pts).dedupe(false, false);
      }
      node.onNext(pts);
    } catch (Throwable t) {
      LOG.error("Unable to send PTS " + pts + " to node " + node, t);
      node.onError(t);
    } finally {
      try {
        close();
      } catch (Exception e) {
        LOG.error("Should never happen!", e);
      }
    }
  }

  /** @return The current context. */
  public QueryPipelineContext context() {
    return node.pipelineContext();
  }
  
  @Override
  public Object object() {
    return this;
  }

  @Override
  public void release() {
    if (pooled_object != null) {
      pooled_object.release();
    }
  }

  @Override
  public void close() throws Exception {
    pts = null;
    node = null;
    release();
  }

  @Override
  public void setPooledObject(PooledObject pooled_object) {
    this.pooled_object = pooled_object;
  }
  
  /** @return The configured PTS. */
  public PartialTimeSeries pts() {
    return pts;
  }
  
  /** @return The query node. */
  public QueryNode node() {
    return node;
  }
}