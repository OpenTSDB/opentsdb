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
package net.opentsdb.query.processor.slidingwindow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

public class TestSlidingWindowConfig {

  @Test
  public void build() throws Exception {
    SlidingWindowConfig config = 
        (SlidingWindowConfig) SlidingWindowConfig.newBuilder()
        .setAggregator("sum")
        .setWindowSize("1m")
        .setInfectiousNan(true)
        .build();
    assertEquals("sum", config.getAggregator());
    assertEquals("1m", config.getWindowSize());
    assertTrue(config.getInfectiousNan());
    
    config = (SlidingWindowConfig) SlidingWindowConfig.newBuilder()
        .setAggregator("sum")
        .setWindowSize("1h")
        .build();
    assertEquals("sum", config.getAggregator());
    assertEquals("1h", config.getWindowSize());
    assertFalse(config.getInfectiousNan());
    
    try {
      SlidingWindowConfig.newBuilder()
          .setAggregator("sum")
          .setWindowSize("no-such-window")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      SlidingWindowConfig.newBuilder()
          //.setAggregator("sum")
          .setWindowSize("1h")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      SlidingWindowConfig.newBuilder()
          .setAggregator("")
          .setWindowSize("1h")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      SlidingWindowConfig.newBuilder()
          .setAggregator("sum")
          //.setWindowSize("1h")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      SlidingWindowConfig.newBuilder()
          .setAggregator("sum")
          .setWindowSize("")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
