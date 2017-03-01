// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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

public class TestOutput {
  @Test
  public void deserializeAllFields() throws Exception {
    String json = "{\"id\":\"m1\",\"alias\":\"CPU OK\"}";
    Output output = JSON.parseToObject(json, Output.class);
    Output expectedOutput = Output.Builder().setId("m1").setAlias("CPU OK")
        .build();
    assertEquals(expectedOutput, output);
  }

  @Test
  public void serialize() throws Exception {
    Output output = Output.Builder().setId("m1").setAlias("CPU OK")
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
}
