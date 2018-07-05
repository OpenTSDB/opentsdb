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
package net.opentsdb.query.joins;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.BeforeClass;

import com.google.common.collect.Lists;

import gnu.trove.map.hash.TLongObjectHashMap;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.query.joins.JoinConfig.JoinType;

public class BaseJoinTest {

  // one to one match
  protected final static TimeSeries L_1 = mock(TimeSeries.class);
  protected final static TimeSeries R_1 = mock(TimeSeries.class);
  
  // only left side
  protected final static TimeSeries L_2 = mock(TimeSeries.class);
  
  // only right side
  protected final static TimeSeries R_3 = mock(TimeSeries.class);
  
  // one left, 2 right
  protected final static TimeSeries L_4 = mock(TimeSeries.class);
  protected final static TimeSeries R_4A = mock(TimeSeries.class);
  protected final static TimeSeries R_4B = mock(TimeSeries.class);
  
  // 2 left, one right
  protected final static TimeSeries L_5A = mock(TimeSeries.class);
  protected final static TimeSeries L_5B = mock(TimeSeries.class);
  protected final static TimeSeries R_5 = mock(TimeSeries.class);
  
  // 2 left, 2 right
  protected final static TimeSeries L_6A = mock(TimeSeries.class);
  protected final static TimeSeries L_6B = mock(TimeSeries.class);
  protected final static TimeSeries R_6A = mock(TimeSeries.class);
  protected final static TimeSeries R_6B = mock(TimeSeries.class);
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    when(L_1.toString()).thenReturn("L_1");
    when(R_1.toString()).thenReturn("R_1");
    
    when(L_2.toString()).thenReturn("L_2");
    
    when(R_3.toString()).thenReturn("R_3");
    
    when(L_4.toString()).thenReturn("L_4");
    when(R_4A.toString()).thenReturn("R_4A");
    when(R_4B.toString()).thenReturn("R_4B");
    
    when(L_5A.toString()).thenReturn("L_5A");
    when(L_5B.toString()).thenReturn("L_5B");
    when(R_5.toString()).thenReturn("R_5");
    
    when(L_6A.toString()).thenReturn("L_6A");
    when(L_6B.toString()).thenReturn("L_6B");
    when(R_6A.toString()).thenReturn("R_6A");
    when(R_6B.toString()).thenReturn("R_6B");
  }
  
  protected static BaseHashedJoinSet leftAndRightSet(final JoinType type) {
    final UTBaseHashedJoinSet set = new UTBaseHashedJoinSet(type);
    
    set.left_map = new TLongObjectHashMap<List<TimeSeries>>();
    set.right_map = new TLongObjectHashMap<List<TimeSeries>>();
    
    // one to one
    set.left_map.put(1, Lists.newArrayList(L_1));
    set.right_map.put(1, Lists.newArrayList(R_1));
    
    // left only
    set.left_map.put(2, Lists.newArrayList(L_2));
    
    // right only
    set.right_map.put(3, Lists.newArrayList(R_3));
    
    // one left, 2 right
    set.left_map.put(4, Lists.newArrayList(L_4));
    set.right_map.put(4, Lists.newArrayList(R_4A, R_4B));
    
    // 2 left, one right
    set.left_map.put(5, Lists.newArrayList(L_5A, L_5B));
    set.right_map.put(5, Lists.newArrayList(R_5));
    
    // 2 left, 2 right
    set.left_map.put(6, Lists.newArrayList(L_6A, L_6B));
    set.right_map.put(6, Lists.newArrayList(R_6A, R_6B));
    
    return set;
  }
  
  protected static BaseHashedJoinSet leftOnlySet(final JoinType type) {
    final BaseHashedJoinSet set = leftAndRightSet(type);
    set.right_map = null;
    return set;
  }
  
  protected static BaseHashedJoinSet rightOnlySet(final JoinType type) {
    final BaseHashedJoinSet set = leftAndRightSet(type);
    set.left_map = null;
    return set;
  }
  
  protected static BaseHashedJoinSet emptyMaps(final JoinType type) {
    final UTBaseHashedJoinSet set = new UTBaseHashedJoinSet(type);
    
    set.left_map = new TLongObjectHashMap<List<TimeSeries>>();
    set.right_map = new TLongObjectHashMap<List<TimeSeries>>();
    
    return set;
  }
  
  protected static BaseHashedJoinSet leftAndRightNullLists(final JoinType type) {
    final UTBaseHashedJoinSet set = new UTBaseHashedJoinSet(type);
    
    set.left_map = new TLongObjectHashMap<List<TimeSeries>>();
    set.right_map = new TLongObjectHashMap<List<TimeSeries>>();
    
    // one to one
    set.left_map.put(1, Lists.newArrayList(L_1));
    set.right_map.put(1, Lists.newArrayList(R_1));
    
    // left only
    set.left_map.put(2, Lists.newArrayList(L_2));
    
    // right only
    set.right_map.put(3, Lists.newArrayList(R_3));
    
    // one left, 2 right
    set.left_map.put(4, null);
    set.right_map.put(4, Lists.newArrayList(R_4A, R_4B));
    
    // 2 left, one right
    set.left_map.put(5, null);
    set.right_map.put(5, Lists.newArrayList(R_5));
    
    // 2 left, 2 right
    set.left_map.put(6, Lists.newArrayList(L_6A, L_6B));
    set.right_map.put(6, Lists.newArrayList(R_6A, R_6B));
    
    return set;
  }
  
  protected static BaseHashedJoinSet leftAndRightEmptyLists(final JoinType type) {
    final UTBaseHashedJoinSet set = new UTBaseHashedJoinSet(type);
    
    set.left_map = new TLongObjectHashMap<List<TimeSeries>>();
    set.right_map = new TLongObjectHashMap<List<TimeSeries>>();
    
    // one to one
    set.left_map.put(1, Lists.newArrayList(L_1));
    set.right_map.put(1, Lists.newArrayList(R_1));
    
    // left only
    set.left_map.put(2, Lists.newArrayList(L_2));
    
    // right only
    set.right_map.put(3, Lists.newArrayList(R_3));
    
    // one left, 2 right
    set.left_map.put(4, Lists.newArrayList());
    set.right_map.put(4, Lists.newArrayList(R_4A, R_4B));
    
    // 2 left, one right
    set.left_map.put(5, Lists.newArrayList());
    set.right_map.put(5, Lists.newArrayList(R_5));
    
    // 2 left, 2 right
    set.left_map.put(6, Lists.newArrayList(L_6A, L_6B));
    set.right_map.put(6, Lists.newArrayList(R_6A, R_6B));
    
    return set;
  }
  
  static class UTBaseHashedJoinSet extends BaseHashedJoinSet {

    public UTBaseHashedJoinSet(final JoinType type) {
      super(type);
    }
    
  }
  
}
