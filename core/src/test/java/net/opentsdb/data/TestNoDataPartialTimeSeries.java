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
package net.opentsdb.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Test;

import net.opentsdb.pools.PooledObject;

public class TestNoDataPartialTimeSeries {

  @Test
  public void test() throws Exception {
    NoDataPartialTimeSeries ndpts = new NoDataPartialTimeSeries();
    assertEquals(0, ndpts.idHash());
    assertNull(ndpts.set());
    assertNull(ndpts.value());
    assertSame(ndpts, ndpts.object());
    assertNull(ndpts.pooled_object);
    
    PooledObject pooled = mock(PooledObject.class);
    PartialTimeSeriesSet set1 = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set2 = mock(PartialTimeSeriesSet.class);
    
    // no-op
    ndpts.close();
    
    ndpts.setPooledObject(pooled);
    verify(pooled, never()).release();
    
    // close
    ndpts.close();
    verify(pooled, times(1)).release();
    
    ndpts.reset(set1);
    assertSame(set1, ndpts.set());
    ndpts.close();
    verify(pooled, times(2)).release();
    assertNull(ndpts.set());
    
    ndpts.reset(set2);
    assertSame(set2, ndpts.set());
    ndpts.close();
    verify(pooled, times(3)).release();
    assertNull(ndpts.set());
  }
  
}
