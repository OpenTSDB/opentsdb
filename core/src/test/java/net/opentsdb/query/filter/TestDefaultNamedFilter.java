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
package net.opentsdb.query.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import org.junit.Test;

public class TestDefaultNamedFilter {

  @Test
  public void build() throws Exception {
    QueryFilter filter = TagValueLiteralOrFilter.newBuilder()
        .setTagKey("host")
        .setFilter("web01")
        .build();
    
    NamedFilter nf = DefaultNamedFilter.newBuilder()
        .setId("f1")
        .setFilter(filter)
        .build();
    assertEquals("f1", nf.id());
    assertSame(filter, nf.filter());
    
    try {
      DefaultNamedFilter.newBuilder()
          //.setId("f1")
          .setFilter(filter)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DefaultNamedFilter.newBuilder()
          .setId("")
          .setFilter(filter)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DefaultNamedFilter.newBuilder()
          .setId("f1")
          //.setFilter(filter)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
