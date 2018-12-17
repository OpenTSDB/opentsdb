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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import net.opentsdb.query.expression.VariableIterator.SetOperator;
import net.opentsdb.utils.JSON;

import org.junit.Test;

public class TestJoin {

  @Test
  public void deserialize() throws Exception {
    final String json = "{\"operator\":\"union\"}";
    final Join join = Join.Builder().setOperator(SetOperator.UNION).build();
    final Join deserialized = JSON.parseToObject(json, Join.class);
    assertEquals(join, deserialized);
  }
  
  @Test
  public void serialize() throws Exception {
    final Join join = Join.Builder().setOperator(SetOperator.UNION).build();
    final String json = JSON.serializeToString(join);
    assertTrue(json.contains("\"operator\":\"union\""));
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenOperatorIsNull() throws Exception {
    final String json = "{\"operator\":null}";
    final Join join = JSON.parseToObject(json, Join.class);
    join.validate();
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenOperatorIsEmpty() throws Exception {
    final String json = "{\"operator\":\"\"}";
    final Join join = JSON.parseToObject(json, Join.class);
    join.validate();
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenOperatorIsInvalid() throws Exception {
    final String json = "{\"operator\":\"nosuchop\"}";
    final Join join = JSON.parseToObject(json, Join.class);
    join.validate();
  }
  
  @Test
  public void unknownShouldBeIgnored() throws Exception {
    String json = "{\"operator\":\"intersection\",\"unknown\":\"yo\"}";
    JSON.parseToObject(json, Filter.class);
    // pass if no unexpected exception
  }
}
