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
package net.opentsdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.regex.Pattern;

import org.junit.Test;

public class TestStringUtils {

  @Test
  public void getRandomString() throws Exception {
    final Pattern pattern = Pattern.compile("[A-Za-z]{24}");
    for (int i = 0; i < 100; i++) {
      assertTrue(pattern.matcher(StringUtils.getRandomString(24)).matches());
    }
    
    try {
      StringUtils.getRandomString(0);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      StringUtils.getRandomString(-42);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void splitString() throws Exception {
    String[] splits = StringUtils.splitString("Space Separated Chars", ' ');
    assertEquals(3, splits.length);
    assertEquals("Space", splits[0]);
    assertEquals("Separated", splits[1]);
    assertEquals("Chars", splits[2]);
    
    splits = StringUtils.splitString("NoSpace", ' ');
    assertEquals(1, splits.length);
    assertEquals("NoSpace", splits[0]);
    
    splits = StringUtils.splitString("", ' ');
    assertEquals(1, splits.length);
    assertEquals("", splits[0]);
    
    try {
      StringUtils.splitString(null, ' ');
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
  }
}
