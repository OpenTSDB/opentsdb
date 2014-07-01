// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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
package net.opentsdb.uid;


import net.opentsdb.core.TSDB;
import static org.junit.Assert.assertEquals;
import org.junit.Test;


public final class TestRandomUniqueId {

  @Test(expected=IllegalArgumentException.class)
  public void testInvalidWidth() {
    long uid = RandomUID.getRandomUID(8);
  }

  @Test(expected=NegativeArraySizeException.class)
  public void testNegativeWidth() {
    long uid = RandomUID.getRandomUID(-1);
  }

  @Test
  public void testZeroWidth() {
    assertEquals(0L, RandomUID.getRandomUID(0));
  }

  @Test
  public void testRandomIDQuality1() {
    for (int i = 0;i <100;i++) {
      long uid = RandomUID.getRandomUID();
      assert uid >= 0;
      assert uid <= getUpperLimitForWidth(TSDB.metrics_width());
    }
  }

  @Test
  public void testRandomIDQuality2() {
    for (int width = 1; width <= RandomUID.MAX_WIDTH; width++) {
      long upperLimit = getUpperLimitForWidth(width);
      
      for (int i = 0;i <10;i++) {
        long uid = RandomUID.getRandomUID(width);
        assert uid >= 0;
        assert uid <= upperLimit;
      }
    }
  }
  
  //Find the upper limit for the given width,
  //or return the maximum number a random number can be for a given width
  private static long getUpperLimitForWidth(int width) {
    long limit = 0;
      
    for (int i = 0; i<width;i++){
      limit <<= 8;
      limit |= 0xFF;
    }
        
    return limit;
  }
}
