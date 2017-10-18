// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.data.types.numeric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import net.opentsdb.query.pojo.FillPolicy;

public class TestBaseNumericFillPolicy {

  @Test
  public void ctor() throws Exception {
    final BaseNumericFillPolicy fill = 
        new BaseNumericFillPolicy(FillPolicy.NOT_A_NUMBER);
    assertTrue(Double.isNaN(fill.fill().doubleValue()));
    
    try {
      new BaseNumericFillPolicy(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void fill() throws Exception {
    BaseNumericFillPolicy fill = new BaseNumericFillPolicy(FillPolicy.NOT_A_NUMBER);
    assertFalse(fill.isInteger());
    assertTrue(Double.isNaN(fill.fill().doubleValue()));
    assertTrue(Double.isNaN(fill.fill().toDouble()));
    assertEquals(0, fill.fill().longValue());
    
    fill = new BaseNumericFillPolicy(FillPolicy.ZERO);
    assertTrue(fill.isInteger());
    assertTrue(Double.isNaN(fill.fill().doubleValue()));
    assertEquals(0, fill.fill().toDouble(), 0.001);
    assertEquals(0, fill.fill().longValue());
    
    fill = new BaseNumericFillPolicy(FillPolicy.NONE);
    try {
      fill.isInteger();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
    assertNull(fill.fill());
    
    fill = new BaseNumericFillPolicy(FillPolicy.NULL);
    try {
      fill.isInteger();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
    assertNull(fill.fill());
    
    fill = new BaseNumericFillPolicy(FillPolicy.SCALAR);
    try {
      fill.isInteger();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
    try {
      fill.fill();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }
}
