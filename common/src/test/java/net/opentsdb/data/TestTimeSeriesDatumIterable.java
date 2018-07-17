// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.data.TimeSeriesDatum;

public class TestTimeSeriesDatumIterable {

  @Test
  public void fromCollection() throws Exception {
    List<TimeSeriesDatum> data = Lists.newArrayList();
    data.add(mock(TimeSeriesDatum.class));
    data.add(mock(TimeSeriesDatum.class));
    
    TimeSeriesDatumIterable iterable = 
        TimeSeriesDatumIterable.fromCollection(data);
    Iterator<TimeSeriesDatum> iterator = iterable.iterator();
    assertTrue(iterator.hasNext());
    assertSame(data.get(0), iterator.next());
    
    assertTrue(iterator.hasNext());
    assertSame(data.get(1), iterator.next());
    
    assertFalse(iterator.hasNext());
    
    // empty
    data.clear();
    iterable = TimeSeriesDatumIterable.fromCollection(data);
    iterator = iterable.iterator();
    assertFalse(iterator.hasNext());
    
    try {
      TimeSeriesDatumIterable.fromCollection(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
}
