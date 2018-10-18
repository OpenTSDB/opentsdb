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
package net.opentsdb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import org.junit.Test;

public class TestBaseTSDBPlugin {

  @Test
  public void methods() throws Exception {
    UTPlugin plugin = new UTPlugin();
    assertNull(plugin.tsdb);
    assertNull(plugin.id());
    assertEquals("UT", plugin.type());
    assertEquals("3.0.0", plugin.version());
    
    TSDB tsdb = mock(TSDB.class);
    assertNull(plugin.initialize(tsdb, "boo").join());
    assertSame(tsdb, plugin.tsdb);
    assertEquals("boo", plugin.id());
    
    assertNull(plugin.shutdown().join());
  }
  
  class UTPlugin extends BaseTSDBPlugin {

    @Override
    public String type() {
      return "UT";
    }

    @Override
    public String version() {
      return "3.0.0";
    }
    
  }
}
