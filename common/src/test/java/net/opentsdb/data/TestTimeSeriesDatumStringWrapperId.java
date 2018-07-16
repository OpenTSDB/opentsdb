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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;

public class TestTimeSeriesDatumStringWrapperId {

  @Test
  public void wrap() throws Exception {
    final TimeSeriesDatumStringId id = new TimeSeriesDatumStringId() {

      @Override
      public TypeToken<? extends TimeSeriesId> type() {
        return Const.TS_STRING_ID;
      }

      @Override
      public int hashCode() {
        return Long.hashCode(buildHashCode());
      }
      
      @Override
      public long buildHashCode() {
        return 42;
      }

      @Override
      public int compareTo(final TimeSeriesDatumId o) {
        return 0;
      }

      @Override
      public String namespace() {
        return "mynamespace";
      }

      @Override
      public String metric() {
        return "sys.cpu.busy";
      }

      @Override
      public Map<String, String> tags() {
        return ImmutableMap.<String, String>builder()
            .put("host", "web01")
            .put("owner", "tyrion")
            .build();
      }
      
    };
    
    TimeSeriesStringId wrapped = TimeSeriesDatumStringWrapperId.wrap(id);
    assertNotSame(id, wrapped);
    assertEquals("mynamespace", wrapped.namespace());
    assertEquals("sys.cpu.busy", wrapped.metric());
    assertEquals(2, wrapped.tags().size());
    assertEquals("web01", wrapped.tags().get("host"));
    assertEquals("tyrion", wrapped.tags().get("owner"));
    assertTrue(wrapped.aggregatedTags().isEmpty());
    assertTrue(wrapped.disjointTags().isEmpty());
    
    assertTrue(wrapped.equals(wrapped));
    assertEquals(0, wrapped.compareTo(wrapped));
    assertEquals(42, wrapped.hashCode());
    
    try {
      TimeSeriesDatumStringWrapperId.wrap(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
