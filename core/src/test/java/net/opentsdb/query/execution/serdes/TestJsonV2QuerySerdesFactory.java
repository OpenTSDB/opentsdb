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
package net.opentsdb.query.execution.serdes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.InputStream;
import java.io.OutputStream;

import org.junit.Test;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.query.serdes.TimeSeriesSerdes;

public class TestJsonV2QuerySerdesFactory {

  @Test
  public void plugin() throws Exception {
    JsonV2QuerySerdesFactory factory = new JsonV2QuerySerdesFactory();
    assertNull(factory.initialize(mock(TSDB.class)).join());
    assertNull(factory.shutdown().join());
    assertEquals("JsonV2QuerySerdes", factory.id());
  }
  
  @Test
  public void newInstance() throws Exception {
    JsonV2QuerySerdesFactory factory = new JsonV2QuerySerdesFactory();
    TimeSeriesSerdes serdes = factory.newInstance(
        mock(QueryContext.class), 
        mock(JsonV2QuerySerdesOptions.class), 
        mock(OutputStream.class));
    assertNotNull(serdes);
    assertTrue(serdes instanceof JsonV2QuerySerdes);
    
    try {
      factory.newInstance(
          mock(QueryContext.class), 
          mock(SerdesOptions.class), 
          mock(InputStream.class));
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }
  
}
