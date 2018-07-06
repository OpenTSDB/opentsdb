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
package net.opentsdb.utils;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Maps;

public class TestComparators {

  @Test
  public void mapComparator() throws Exception {
    Map<Long, String> m1 = Maps.newHashMap();
    m1.put(42L, "Tyrion");
    m1.put(-1L, "Cersei");
    Map<Long, String> m2 = Maps.newHashMap();
    m2.put(-1L, "Cersei");
    m2.put(42L, "Tyrion");
    
    assertEquals(0, new Comparators.MapComparator<Long, String>()
        .compare(m1, m2));
    
    m2.remove(42L);
    assertEquals(-1, new Comparators.MapComparator<Long, String>()
        .compare(m1, m2));
    
    m2.clear();
    assertEquals(-1, new Comparators.MapComparator<Long, String>()
        .compare(m1, m2));
    
    assertEquals(1, new Comparators.MapComparator<Long, String>()
        .compare(m1, null));
    
    assertEquals(-1, new Comparators.MapComparator<Long, String>()
        .compare(null, m1));
    
    assertEquals(0, new Comparators.MapComparator<Long, String>()
        .compare(null, null));
    
    assertEquals(0, new Comparators.MapComparator<Long, String>()
        .compare(m1, m1));
  }
}
