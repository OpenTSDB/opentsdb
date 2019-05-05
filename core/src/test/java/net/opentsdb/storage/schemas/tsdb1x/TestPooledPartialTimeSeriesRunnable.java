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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.query.QueryNode;
import net.opentsdb.utils.UnitTestException;

public class TestPooledPartialTimeSeriesRunnable {

  private PooledObject pooled_obj;
  private PartialTimeSeries pts;
  private QueryNode node;
  
  @Before
  public void before() throws Exception {
    pooled_obj = mock(PooledObject.class);
    pts = mock(PartialTimeSeries.class);
    node = mock(QueryNode.class);
  }
  
  @Test
  public void reset() throws Exception {
    PooledPartialTimeSeriesRunnable runnable = new PooledPartialTimeSeriesRunnable();
    assertNull(runnable.pooled_object);
    assertNull(runnable.pts);
    assertNull(runnable.node);
    
    runnable.reset(pts, node);
    assertSame(pts, runnable.pts);
    assertSame(node, runnable.node);
  }
  
  @Test
  public void run() throws Exception {
    PooledPartialTimeSeriesRunnable runnable = new PooledPartialTimeSeriesRunnable();
    runnable.setPooledObject(pooled_obj);
    runnable.reset(pts, node);
    runnable.run();
    
    assertNull(runnable.pts);
    assertNull(runnable.node);
    verify(node, times(1)).onNext(pts);
    verify(node, never()).onError(any(Throwable.class));
    verify(pooled_obj, times(1)).release();
  }
  
  @Test
  public void runTsdb1xPartialTimeSeries() throws Exception {
    PooledPartialTimeSeriesRunnable runnable = new PooledPartialTimeSeriesRunnable();
    runnable.setPooledObject(pooled_obj);
    pts = mock(Tsdb1xPartialTimeSeries.class);
    runnable.reset(pts, node);
    runnable.run();
    
    assertNull(runnable.pts);
    assertNull(runnable.node);
    verify(((Tsdb1xPartialTimeSeries) pts), times(1)).dedupe(false, false);
    verify(node, times(1)).onNext(pts);
    verify(node, never()).onError(any(Throwable.class));
    verify(pooled_obj, times(1)).release();
  }
  
  @Test
  public void runError() throws Exception {
    doThrow(new UnitTestException()).when(node).onNext(any(PartialTimeSeries.class));
    PooledPartialTimeSeriesRunnable runnable = new PooledPartialTimeSeriesRunnable();
    runnable.setPooledObject(pooled_obj);
    runnable.reset(pts, node);
    runnable.run();
    
    assertNull(runnable.pts);
    assertNull(runnable.node);
    verify(node, times(1)).onNext(pts);
    verify(node, times(1)).onError(any(UnitTestException.class));
    verify(pooled_obj, times(1)).release();
  }
  
}
