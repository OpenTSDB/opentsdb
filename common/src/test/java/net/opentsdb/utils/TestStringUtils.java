// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
