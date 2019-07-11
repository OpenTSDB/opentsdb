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

import org.junit.Test;

import static org.junit.Assert.*;

public class TestDefaultNamedFilter {

  @Test
  public void build() throws Exception {
    QueryFilter filter = TagValueLiteralOrFilter.newBuilder()
        .setKey("host")
        .setFilter("web01")
        .build();
    
    NamedFilter nf = DefaultNamedFilter.newBuilder()
        .setId("f1")
        .setFilter(filter)
        .build();
    assertEquals("f1", nf.getId());
    assertSame(filter, nf.getFilter());
    
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

  @Test
  public void equality() throws Exception {
    QueryFilter filter = TagValueLiteralOrFilter.newBuilder()
            .setKey("host")
            .setFilter("web01")
            .build();

    NamedFilter nf = DefaultNamedFilter.newBuilder()
            .setId("f1")
            .setFilter(filter)
            .build();

    QueryFilter filter2 = TagValueLiteralOrFilter.newBuilder()
            .setKey("host")
            .setFilter("web01")
            .build();

    NamedFilter nf2 = DefaultNamedFilter.newBuilder()
            .setId("f1")
            .setFilter(filter2)
            .build();

    QueryFilter filter3 = TagValueLiteralOrFilter.newBuilder()
            .setKey("host")
            .setFilter("web01")
            .build();

    NamedFilter nf3 = DefaultNamedFilter.newBuilder()
            .setId("f2")
            .setFilter(filter3)
            .build();


    assertTrue(nf.equals(nf2));
    assertTrue(!nf.equals(nf3));
    assertEquals(nf.hashCode(), nf2.hashCode());
    assertNotEquals(nf.hashCode(), nf3.hashCode());
  }


}
