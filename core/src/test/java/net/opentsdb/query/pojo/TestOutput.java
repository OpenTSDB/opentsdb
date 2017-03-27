// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.query.pojo;

import net.opentsdb.utils.JSON;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;

public class TestOutput {
  @Test
  public void deserializeAllFields() throws Exception {
    String json = "{\"id\":\"m1\",\"alias\":\"CPU OK\"}";
    Output output = JSON.parseToObject(json, Output.class);
    Output expectedOutput = Output.newBuilder().setId("m1").setAlias("CPU OK")
        .build();
    assertEquals(expectedOutput, output);
  }

  @Test
  public void serialize() throws Exception {
    Output output = Output.newBuilder().setId("m1").setAlias("CPU OK")
        .build();
    String actual = JSON.serializeToString(output);
    String expected = "{\"id\":\"m1\",\"alias\":\"CPU OK\"}";
    assertEquals(expected, actual);
  }

  @Test
  public void unknownFieldShouldBeIgnored() throws Exception {
    String json = "{\"id\":\"m1\",\"unknown\":\"yo\"}";
    JSON.parseToObject(json, Filter.class);
    // pass if no unexpected exception
  }

  @Test
  public void build() throws Exception {
    final Output output = new Output.Builder()
        .setId("out1")
        .setAlias("MyMetric")
        .build();
    final Output clone = Output.newBuilder(output).build();
    assertNotSame(output, clone);
    assertEquals("out1", clone.getId());
    assertEquals("MyMetric", clone.getAlias());
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final Output o1 = new Output.Builder()
        .setId("out1")
        .setAlias("MyMetric")
        .build();
    
    Output o2 = new Output.Builder()
        .setId("out1")
        .setAlias("MyMetric")
        .build();
    assertEquals(o1.hashCode(), o2.hashCode());
    assertEquals(o1, o2);
    assertEquals(0, o1.compareTo(o2));
    
    o2 = new Output.Builder()
        .setId("out2")  // <-- diff
        .setAlias("MyMetric")
        .build();
    assertNotEquals(o1.hashCode(), o2.hashCode());
    assertNotEquals(o1, o2);
    assertEquals(-1, o1.compareTo(o2));
    
    o2 = new Output.Builder()
        .setId("out1")
        .setAlias("Nother Metric")  // <-- diff
        .build();
    assertNotEquals(o1.hashCode(), o2.hashCode());
    assertNotEquals(o1, o2);
    assertEquals(-1, o1.compareTo(o2));
    
    o2 = new Output.Builder()
        .setId("out1")
        //.setAlias("MyMetric")  // <-- diff
        .build();
    assertNotEquals(o1.hashCode(), o2.hashCode());
    assertNotEquals(o1, o2);
    assertEquals(1, o1.compareTo(o2));
  }
}
